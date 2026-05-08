.PHONY: help check up down ns secrets diff status spark-image hms \
        spark-code dbt-configmap airflow-dags airflow-trigger-pipeline superset-init \
        ingress hosts images airflow-image hms-image rbac airflow-unpause wait-airflow ensure-buckets verify-trino \
        bootstrap-pipeline seed-data streaming-apps kafka-streaming-app \
        monitoring monitoring-dashboards

KIND_CONTEXT ?= docker-desktop
SAMPLE_DIR ?= lab08/sample

help:
	@echo "Lab08 — Транзакционная аналитика. Управление инфраструктурой."
	@echo ""
	@echo "  make up                       - ОДНА КОМАНДА: полная идемпотентная установка (images + helm + k8s + DAG'и + unpause)"
	@echo "  make check                    - проверить, что kubectl, helm, helmfile доступны"
	@echo "  make ns                       - создать namespaces"
	@echo "  make secrets                  - применить k8s/secrets.yaml (создай из secrets.example.yaml)"
	@echo "  make spark-image              - собрать кастомный Spark image"
	@echo "  make airflow-image            - собрать кастомный Airflow image"
	@echo "  make hms-image                - собрать кастомный Hive Metastore image"
	@echo "  make images                   - собрать все кастомные image (skip если уже есть)"
	@echo "  make hms                      - применить hive-metastore deployment"
	@echo "  make rbac                     - применить spark-rbac, airflow-rbac"
	@echo "  make streaming-apps           - применить long-running streaming SparkApplications (S3 + Kafka)"
	@echo "  make ensure-buckets           - убедиться что lake bucket существует (для существующего MinIO)"
	@echo "  make spark-code               - пересоздать ConfigMap spark-jobs-code из spark-jobs/*.py"
	@echo "  make dbt-configmap            - пересоздать ConfigMap dbt-project из dbt/"
	@echo "  make ingress                  - применить Ingress-ресурсы (airflow/superset/trino/minio)"
	@echo "  make monitoring               - применить ServiceMonitor/PodMonitor + Grafana dashboards"
	@echo "  make monitoring-dashboards    - пересоздать ConfigMap c Grafana дашбордами"
	@echo "  make hosts                    - добавить *.lab08.local в /etc/hosts (требует sudo)"
	@echo "  make airflow-dags             - скопировать airflow/dags/ в под Airflow scheduler"
	@echo "  make airflow-unpause          - снять с паузы DAG transactions_pipeline"
	@echo "  make verify-trino             - проверить что Trino отвечает и видит hudi.bronze.transactions"
	@echo "  make airflow-trigger-pipeline - запустить DAG transactions_pipeline вручную (sensor сам подождёт bronze)"
	@echo "  make superset-init            - создать datasources, charts и dashboard в Superset"
	@echo "  make diff                     - helmfile diff (что изменится)"
	@echo "  make status                   - kubectl get pods во всех namespaces проекта"
	@echo "  make down                     - helmfile destroy + удалить HMS (PVC данные пропадут)"

check:
	@kubectl config current-context | grep -q "$(KIND_CONTEXT)" || (echo "Wrong context: expected $(KIND_CONTEXT)" && exit 1)
	@kubectl version --client=true >/dev/null && echo "kubectl OK"
	@helm version --short >/dev/null && echo "helm OK"
	@helmfile --version >/dev/null && echo "helmfile OK"
	@helm plugin list | grep -q diff && echo "helm-diff OK"
	@kubectl get nodes

ns:
	kubectl apply -f k8s/namespaces.yaml >/dev/null

secrets:
	@test -f k8s/secrets.yaml || (echo "Создай k8s/secrets.yaml из k8s/secrets.example.yaml" && exit 1)
	kubectl apply -f k8s/secrets.yaml >/dev/null

# Собирает spark image только если его нет (идемпотентно).
spark-image:
	@docker image inspect lab08/spark:3.5.8-hudi-1.1.1 >/dev/null 2>&1 \
	  || docker build -t lab08/spark:3.5.8-hudi-1.1.1 docker/spark/

# Собирает airflow image только если его нет (идемпотентно).
airflow-image:
	@docker image inspect lab08/airflow:2.10.4 >/dev/null 2>&1 \
	  || docker build -t lab08/airflow:2.10.4 docker/airflow/

# Собирает hive-metastore image только если его нет (идемпотентно).
hms-image:
	@docker image inspect lab08/hive-metastore:3.0.0-pg2 >/dev/null 2>&1 \
	  || docker build -t lab08/hive-metastore:3.0.0-pg2 docker/hive-metastore/

images: spark-image airflow-image hms-image

# === ОДНА КОМАНДА: полностью идемпотентная установка ===
# 1. namespaces  2. secrets  3. images  4. helmfile sync (MinIO/HMS-pg/Spark-Op/Trino/Superset/Airflow/Ingress)
# 5. HMS deployment  6. lake bucket  7. RBAC  8. configmaps (spark-code, dbt-project)  9. streaming SparkApps
# 10. Ingress  11. DAG'и + unpause  12. verify Trino
#
# Spark Thrift Server убран — dbt теперь запускается как SparkApplication через
# SparkKubernetesOperator (см. lab08/MIGRATION_THRIFT_TO_OPERATOR.md). BI-запросы
# идут через Trino, ad-hoc — `kubectl exec deploy/trino-coordinator -- trino`.
up: ns secrets images
	@echo ">>> [1/3] MinIO (storage)..."
	helmfile --quiet -l name=minio-operator sync
	helmfile --quiet -l name=minio-tenant sync
	$(MAKE) ensure-buckets
	$(MAKE) seed-data
	$(MAKE) airflow-dags
	@echo ">>> [2/3] Остальная инфраструктура (helmfile sync)..."
	helmfile --quiet sync
	@echo "    Ожидаю hive-metastore-postgresql..."
	kubectl -n data-platform wait --for=condition=ready pod -l app.kubernetes.io/instance=hive-metastore-postgres --timeout=300s
	$(MAKE) hms
	$(MAKE) rbac
	$(MAKE) spark-code
	$(MAKE) dbt-configmap
	$(MAKE) streaming-apps
	$(MAKE) ingress
	$(MAKE) monitoring
	@echo "    Ожидаю Airflow scheduler..."
	kubectl -n data-platform wait --for=condition=ready pod -l component=scheduler --timeout=300s
	@echo ">>> [3/3] Bootstrap пайплайна и Superset..."
	$(MAKE) airflow-unpause
	$(MAKE) verify-trino
	$(MAKE) bootstrap-pipeline
	$(MAKE) superset-init
	$(MAKE) verify-trino
	@echo ""
	@echo "================================================================"
	@echo "  Lab08 готов. UI:"
	@echo "    Airflow  : http://airflow.lab08.local   (admin/admin)"
	@echo "    Superset : http://superset.lab08.local"
	@echo "    Trino    : http://trino.lab08.local"
	@echo "    MinIO S3 : http://s3.lab08.local"
	@echo "    Grafana  : http://grafana.lab08.local   (admin/admin)"
	@echo "  (Если *.lab08.local не резолвятся — сделай 'make hosts')"
	@echo "================================================================"

hms:
	kubectl apply -f k8s/hive-metastore.yaml >/dev/null
	kubectl -n data-platform rollout status deployment/hive-metastore --timeout=300s

# RBAC для spark-jobs namespace и cross-ns доступа Airflow.
# Сами Secrets применяются отдельным таргетом `make secrets` (см. k8s/secrets.example.yaml).
rbac:
	kubectl apply -f k8s/spark-rbac.yaml >/dev/null
	kubectl apply -f k8s/airflow-rbac.yaml >/dev/null

# Long-running streaming SparkApplications: bronze.* ingest из S3 (file source).
# SparkApp декларативный (restartPolicy: Always + checkpoints на S3),
# Airflow им не управляет — он живёт отдельно от orchestration слоя.
# Идемпотентно: kubectl apply, повторный запуск не пересоздаёт running app.
#
# NB: Kafka SparkApp применяется ОТДЕЛЬНО через `make kafka-streaming-app`,
# т.к. внешний брокер (kafka.ijklmn.xyz:9092) недоступен в большинстве сред —
# при включении он бы бесконечно рестартовал (см. ADR #24 в lab08/PLAN.md).
streaming-apps:
	@echo ">>> Применяю streaming SparkApplication (S3)..."
	kubectl apply -f k8s/spark-applications/bronze-s3-streaming.yaml >/dev/null
	@echo "    SparkApp applied. bronze.* появится через ~1-3 мин (cold start)."

# Kafka streaming SparkApp — применять только когда брокер реально доступен,
# иначе бесконечный CrashLoopBackOff. См. ADR #24.
kafka-streaming-app:
	@echo ">>> Применяю bronze-kafka-ingest SparkApplication..."
	@echo "    ВНИМАНИЕ: убедись, что Kafka-брокер из Secret lab08-credentials (KAFKA_BOOTSTRAP_SERVERS) доступен изнутри кластера."
	kubectl apply -f k8s/spark-applications/bronze-kafka-ingest.yaml >/dev/null

# На случай если MinIO Tenant уже создан без `lake` bucket — досоздаём через mc внутри pod'а.
# Идемпотентно: mc mb игнорирует уже существующие buckets с --ignore-existing.
ensure-buckets:
	@echo ">>> Создаю/проверяю MinIO buckets..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12; do \
	  if kubectl -n storage get pod -l v1.min.io/tenant=lab08 2>/dev/null | grep -q lab08; then break; fi; \
	  sleep 5; \
	done
	@kubectl -n storage wait --for=condition=ready pod -l v1.min.io/tenant=lab08 --timeout=300s >/dev/null
	@MINIO_POD=$$(kubectl -n storage get pod -l v1.min.io/tenant=lab08 -o jsonpath='{.items[0].metadata.name}'); \
	  for b in lake hudi warehouse checkpoints artifacts; do \
	    kubectl -n storage exec $$MINIO_POD -c minio -- sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin123 >/dev/null 2>&1 && mc mb --ignore-existing local/$$b 2>&1 | grep -v 'already exists'" || true; \
	  done

# Заливает sample/*.jsonl в s3://lake/raw/ согласно структуре, которую читает
# spark-jobs/bronze_s3_streaming.py (long-running file-source streaming):
#   batch/day={d}/slot=00/transactions.jsonl       (за дни из SEED_DAYS; стрим ловит по transactions.jsonl)
#   cancellations/day={d}/cancellations.jsonl
#   exchange_rates/rates.jsonl                      (flat; стрим ловит по rates.jsonl)
#   reference/{users,test_users,promo_codes}.jsonl  (стрим разводит handle_reference по basename)
# Идемпотентно: --overwrite. Дни берутся из аргумента (по умолчанию 2025-10-06,2025-10-07).
# NB: повторный seed с теми же ключами не гарантированно реобрабатывается file source —
# для полного reset удалить s3://hudi/.checkpoints/bronze-s3-stream/* и bronze-таблицы.
SEED_DAYS ?= 2025-10-06 2025-10-07
seed-data:
	@echo ">>> Загружаю sample-данные в s3://lake/raw/..."
	@kubectl -n data-platform delete pod mc-seed --ignore-not-found --wait=true >/dev/null 2>&1 || true
	@kubectl -n data-platform run mc-seed \
	  --image=minio/mc:RELEASE.2024-11-21T17-21-54Z --restart=Never \
	  --env=MC_CONFIG_DIR=/tmp/.mc --env=HOME=/tmp \
	  --command -- sh -c 'sleep 600' >/dev/null
	@kubectl -n data-platform wait --for=condition=Ready pod/mc-seed --timeout=60s >/dev/null
	@for d in $(SEED_DAYS); do \
	  kubectl -n data-platform exec -i mc-seed -- sh -c "cat > /tmp/transactions-$$d.jsonl" < $(SAMPLE_DIR)/transactions_sample.jsonl; \
	  kubectl -n data-platform exec -i mc-seed -- sh -c "cat > /tmp/cancellations-$$d.jsonl" < $(SAMPLE_DIR)/cancellations_sample.jsonl; \
	done
	@kubectl -n data-platform exec -i mc-seed -- sh -c 'cat > /tmp/rates.jsonl' < $(SAMPLE_DIR)/exchange_rates_sample.jsonl
	@kubectl -n data-platform exec -i mc-seed -- sh -c 'cat > /tmp/users.jsonl' < $(SAMPLE_DIR)/users.jsonl
	@kubectl -n data-platform exec -i mc-seed -- sh -c 'cat > /tmp/test_users.jsonl' < $(SAMPLE_DIR)/test_users.jsonl
	@kubectl -n data-platform exec -i mc-seed -- sh -c 'cat > /tmp/promo_codes.jsonl' < $(SAMPLE_DIR)/promo_codes.jsonl
	@kubectl -n data-platform exec mc-seed -- sh -c '\
	  mc alias set m http://minio.storage.svc.cluster.local minioadmin minioadmin123 >/dev/null && \
	  for d in $(SEED_DAYS); do \
	    mc cp --quiet /tmp/transactions-$$d.jsonl m/lake/raw/batch/day=$$d/slot=00/transactions.jsonl && \
	    mc cp --quiet /tmp/cancellations-$$d.jsonl m/lake/raw/cancellations/day=$$d/cancellations.jsonl; \
	  done && \
	  mc cp --quiet /tmp/rates.jsonl       m/lake/raw/exchange_rates/rates.jsonl && \
	  mc cp --quiet /tmp/users.jsonl       m/lake/raw/reference/users.jsonl && \
	  mc cp --quiet /tmp/test_users.jsonl  m/lake/raw/reference/test_users.jsonl && \
	  mc cp --quiet /tmp/promo_codes.jsonl m/lake/raw/reference/promo_codes.jsonl && \
	  echo "    s3://lake/raw: $$(mc ls --recursive m/lake/raw | wc -l) файлов"'
	@kubectl -n data-platform delete pod mc-seed --wait=false >/dev/null 2>&1 || true

diff:
	helmfile diff

status:
	@for ns in data-platform spark-jobs storage minio-operator; do \
		echo "=== $$ns ==="; \
		kubectl get pods -n $$ns 2>/dev/null || true; \
	done

down:
	@echo ">>> 1/6 Удаляю SparkApplication-инстансы (могут блокировать ns terminate)..."
	-kubectl -n spark-jobs delete sparkapplication --all --ignore-not-found --wait=false
	-kubectl -n spark-jobs delete scheduledsparkapplication --all --ignore-not-found --wait=false
	@echo ">>> 2/6 Удаляю ручные манифесты (HMS / streaming / Ingress / RBAC / secrets)..."
	-kubectl delete -f k8s/spark-applications/bronze-s3-streaming.yaml --ignore-not-found
	-kubectl delete -f k8s/spark-applications/bronze-kafka-ingest.yaml --ignore-not-found
	-kubectl delete -f k8s/hive-metastore.yaml --ignore-not-found
	-kubectl delete -f k8s/ingress.yaml --ignore-not-found
	-kubectl delete -f k8s/airflow-rbac.yaml --ignore-not-found
	-kubectl delete -f k8s/spark-rbac.yaml --ignore-not-found
	-test -f k8s/secrets.yaml && kubectl delete -f k8s/secrets.yaml --ignore-not-found || true
	-kubectl -n spark-jobs delete configmap spark-jobs-code dbt-project --ignore-not-found
	@echo ">>> 3/6 helmfile destroy (все helm-релизы)..."
	-helmfile destroy
	@echo ">>> 4/6 Удаляю namespaces проекта (PVC и всё остальное внутри уйдут)..."
	-kubectl delete namespace data-platform spark-jobs storage minio-operator --ignore-not-found --wait=true --timeout=300s
	@echo ">>> 5/6 Удаляю осиротевшие PV (released, без claim)..."
	-kubectl get pv -o json | jq -r '.items[] | select(.status.phase=="Released") | .metadata.name' 2>/dev/null | xargs -r kubectl delete pv --ignore-not-found || true
	@echo ">>> 6/6 Удаляю CRDs Spark Operator и MinIO Operator..."
	-kubectl delete crd sparkapplications.sparkoperator.k8s.io scheduledsparkapplications.sparkoperator.k8s.io --ignore-not-found
	-kubectl delete crd tenants.minio.min.io policybindings.sts.min.io --ignore-not-found
	@echo ""
	@echo "================================================================"
	@echo "  make down завершён. Кластер kind/docker-desktop остался,"
	@echo "  но lab08 полностью удалён (включая PVC, CRDs и namespaces)."
	@echo "  Для полной переустановки: make up"
	@echo "================================================================"

# Пересоздаёт ConfigMap spark-jobs-code из spark-jobs/*.py.
# spark-jobs/ — единственный источник правды для PySpark скриптов.
spark-code:
	kubectl create configmap spark-jobs-code -n spark-jobs \
	  --from-file=spark-jobs/ \
	  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Пересоздаёт ConfigMap dbt-project из dbt/ (flat layout, имена префиксированы слоем).
# Airflow KubernetesPodOperator читает этот configmap; dbt-init.sh раскладывает
# flat-структуру в нормальную dbt-проектную раскладку.
dbt-configmap:
	kubectl create configmap dbt-project -n spark-jobs \
	  --from-file=dbt_project.yml=dbt/dbt_project.yml \
	  --from-file=profiles.yml=dbt/profiles.yml \
	  --from-file=dbt-init.sh=dbt/dbt-init.sh \
	  --from-file=macros_generate_schema_name.sql=dbt/macros/generate_schema_name.sql \
	  --from-file=sources.yml=dbt/models/sources.yml \
	  --from-file=silver_transactions_clean.sql=dbt/models/silver/transactions_clean.sql \
	  --from-file=silver_cancellations_clean.sql=dbt/models/silver/cancellations_clean.sql \
	  --from-file=silver_exchange_rates_daily.sql=dbt/models/silver/exchange_rates_daily.sql \
	  --from-file=silver__silver.yml=dbt/models/silver/_silver.yml \
	  --from-file=gold_transactions_by_hour.sql=dbt/models/gold/transactions_by_hour.sql \
	  --from-file=gold_purchases_by_hour.sql=dbt/models/gold/purchases_by_hour.sql \
	  --from-file=gold_revenue_daily.sql=dbt/models/gold/revenue_daily.sql \
	  --from-file=gold_promo_codes_analysis.sql=dbt/models/gold/promo_codes_analysis.sql \
	  --from-file=gold_cancellations_summary.sql=dbt/models/gold/cancellations_summary.sql \
	  --from-file=gold_user_cohorts.sql=dbt/models/gold/user_cohorts.sql \
	  --from-file=gold__gold.yml=dbt/models/gold/_gold.yml \
	  --from-file=test_recon_silver_transactions_count.sql=dbt/tests/recon_silver_transactions_count.sql \
	  --from-file=test_recon_cancellations_orphan_rate.sql=dbt/tests/recon_cancellations_orphan_rate.sql \
	  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Применяет Ingress ресурсы для всех UI-сервисов (nginx-ingress должен быть запущен).
ingress:
	kubectl apply -f k8s/ingress.yaml >/dev/null
	kubectl apply -f k8s/monitoring/ingress.yaml >/dev/null

# Observability: ServiceMonitor / PodMonitor + Grafana dashboards.
# kube-prometheus-stack уже поднят helmfile-ом; здесь — наша конфигурация скрейпа
# и набор дашбордов lab08. Идемпотентно: kubectl apply.
monitoring: monitoring-dashboards
	kubectl apply -f k8s/monitoring/namespace.yaml >/dev/null
	kubectl apply -f k8s/monitoring/servicemonitor-spark-operator.yaml >/dev/null
	kubectl apply -f k8s/monitoring/podmonitor-spark-applications.yaml >/dev/null
	kubectl apply -f k8s/monitoring/servicemonitor-minio.yaml >/dev/null
	@echo ">>> Monitoring objects applied. Grafana → http://grafana.lab08.local (admin/admin)."

# ConfigMap с дашбордами Grafana — sidecar grafana подхватывает любые ConfigMap
# с label grafana_dashboard=1 в любом namespace и публикует JSON в Grafana.
# Идемпотентно: dry-run | apply.
monitoring-dashboards:
	kubectl apply -f k8s/monitoring/namespace.yaml >/dev/null
	kubectl -n monitoring create configmap grafana-dashboard-lab08-overview \
	  --from-file=lab08-overview.json=k8s/monitoring/dashboards/lab08-overview.json \
	  --dry-run=client -o yaml | \
	  kubectl label --local -f - --dry-run=client -o yaml \
	    grafana_dashboard=1 | \
	  kubectl annotate --local -f - --dry-run=client -o yaml \
	    grafana_folder=lab08 | \
	  kubectl apply -f - >/dev/null

# Добавляет *.lab08.local в /etc/hosts (требует sudo).
# Идемпотентен: не дублирует строки при повторном запуске.
hosts:
	@for host in airflow.lab08.local superset.lab08.local trino.lab08.local s3.lab08.local grafana.lab08.local prometheus.lab08.local; do \
	  grep -q "$$host" /etc/hosts || echo "127.0.0.1 $$host" | sudo tee -a /etc/hosts; \
	done
	@echo "Готово:"
	@echo "  http://airflow.lab08.local    — Airflow UI"
	@echo "  http://superset.lab08.local   — Superset UI"
	@echo "  http://trino.lab08.local      — Trino UI"
	@echo "  http://s3.lab08.local         — MinIO S3 API"
	@echo "  http://grafana.lab08.local    — Grafana (admin/admin)"
	@echo "  http://prometheus.lab08.local — Prometheus"

# Загружает DAG-файлы в MinIO bucket s3://artifacts/dags/.
# В make up вызывается ДО helmfile sync для Airflow → initContainer dag-init подхватит
# свежий снимок при первом старте pod-а.
# При повторном вызове (после изменения файлов) sidecar dag-sync синкнет за ≤15 сек,
# scheduler пересканирует папку за ≤30 сек (DAG_DIR_LIST_INTERVAL).
# Идемпотентно: --overwrite + --remove синхронизирует состояние с локальной копией.
airflow-dags:
	@echo ">>> Загружаю DAG-файлы в s3://artifacts/dags/..."
	@kubectl -n data-platform delete pod mc-dag-uploader --ignore-not-found --wait=true >/dev/null 2>&1 || true
	@kubectl -n data-platform run mc-dag-uploader \
	  --image=minio/mc:RELEASE.2024-11-21T17-21-54Z \
	  --restart=Never \
	  --env=MC_CONFIG_DIR=/tmp/.mc \
	  --env=HOME=/tmp \
	  --command -- sh -c 'sleep 600' >/dev/null
	@kubectl -n data-platform wait --for=condition=Ready pod/mc-dag-uploader --timeout=60s >/dev/null
	@kubectl -n data-platform exec mc-dag-uploader -- mkdir -p /tmp/dags
	@for f in airflow/dags/*.py; do \
	  kubectl -n data-platform exec -i mc-dag-uploader -- sh -c "cat > /tmp/dags/$$(basename $$f)" < $$f; \
	done
	@kubectl -n data-platform exec mc-dag-uploader -- sh -c '\
	  mc alias set minio http://minio.storage.svc.cluster.local minioadmin minioadmin123 >/dev/null && \
	  mc mirror --quiet --overwrite --remove /tmp/dags minio/artifacts/dags && \
	  echo "    s3://artifacts/dags: $$(mc ls --recursive minio/artifacts/dags | wc -l) файлов"'
	@kubectl -n data-platform delete pod mc-dag-uploader --wait=false >/dev/null 2>&1 || true

# Снимает с паузы DAG'и проекта. Идемпотентно: повторный вызов — no-op.
# Ждём пока DAG'и появятся в БД (после parse), потом unpause.
airflow-unpause:
	$(eval SCHEDULER_POD := $(shell kubectl -n data-platform get pod -l component=scheduler -o jsonpath='{.items[0].metadata.name}'))
	@for d in transactions_pipeline; do \
	  for i in 1 2 3 4 5 6; do \
	    if kubectl -n data-platform exec $(SCHEDULER_POD) -- airflow dags list 2>/dev/null | grep -q "^$$d "; then \
	      kubectl -n data-platform exec $(SCHEDULER_POD) -- airflow dags unpause $$d || true; \
	      break; \
	    else \
	      echo "Жду DAG $$d (попытка $$i/6)..."; sleep 10; \
	    fi; \
	  done; \
	done

# Проверяет что Trino отвечает и каталог hudi работает.
verify-trino:
	@echo ">>> Проверка Trino..."
	@kubectl -n data-platform wait --for=condition=ready pod -l app.kubernetes.io/name=trino,app.kubernetes.io/component=coordinator --timeout=180s >/dev/null
	@kubectl -n data-platform exec deploy/trino-coordinator -- trino --execute "SHOW CATALOGS" 2>/dev/null | grep -q hudi \
	  && echo "    catalog hudi: OK" || (echo "    catalog hudi: FAIL" && exit 1)
	@kubectl -n data-platform exec deploy/trino-coordinator -- trino --execute "SHOW SCHEMAS FROM hudi" 2>/dev/null | grep -q bronze \
	  && echo "    schema hudi.bronze: OK" || echo "    schema hudi.bronze: пока пусто (запусти make streaming-apps)"
	@if kubectl -n data-platform exec deploy/trino-coordinator -- trino --execute "SHOW TABLES FROM hudi.bronze" 2>/dev/null | grep -q transactions; then \
	    echo -n "    bronze.transactions rows: "; \
	    kubectl -n data-platform exec deploy/trino-coordinator -- trino --execute "SELECT count(*) FROM hudi.bronze.transactions" 2>/dev/null | tail -1; \
	  else \
	    echo "    bronze.transactions ещё не создана (создастся после первого micro-batch)"; \
	  fi

# Запускает основной DAG вручную (bronze ждётся sensor'ом внутри DAG'а).
airflow-trigger-pipeline:
	$(eval SCHEDULER_POD := $(shell kubectl -n data-platform get pod -l component=scheduler -o jsonpath='{.items[0].metadata.name}'))
	kubectl -n data-platform exec $(SCHEDULER_POD) -- airflow dags trigger transactions_pipeline

# Bootstrap: гарантирует, что lab08 пройден end-to-end к моменту superset-init.
#   1) если gold уже наполнен И последний DAGRun = success (значит dbt_test
#      прошёл) → skip;
#   2) если scheduler уже создал run (queued/running) → НЕ триггерим вручную,
#      иначе получаем 2 параллельных run'а (manual + scheduled), которые
#      max_active_runs=1 сериализует → пайплайн крутится дважды;
#   3) иначе триггерим manual run (cold-start, чтобы не ждать */30 cron-окна);
#   4) ждём state=success у самого свежего run'а — это включает dbt_test.
bootstrap-pipeline:
	@set -e; \
	SCHEDULER_POD=$$(kubectl -n data-platform get pod -l component=scheduler -o jsonpath='{.items[0].metadata.name}'); \
	af() { kubectl -n data-platform exec $$SCHEDULER_POD -c scheduler -- airflow "$$@"; }; \
	count_positive() { \
	  out=$$(kubectl -n data-platform exec deploy/trino-coordinator -- \
	         trino --execute "SELECT count(*) FROM $$1" 2>/dev/null \
	         | tail -1 | tr -d '"' | tr -d ' '); \
	  case "$$out" in [1-9]*) return 0 ;; *) return 1 ;; esac; \
	}; \
	last_run_state() { \
	  af dags list-runs -d transactions_pipeline -o json 2>/dev/null \
	    | python3 -c "import sys,json; rs=json.load(sys.stdin) or []; rs.sort(key=lambda r: r.get('execution_date',''), reverse=True); print(rs[0]['state'] if rs else '')" 2>/dev/null; \
	}; \
	has_active_run() { \
	  af dags list-runs -d transactions_pipeline -o json 2>/dev/null \
	    | python3 -c "import sys,json; rs=json.load(sys.stdin) or []; sys.exit(0 if any(r.get('state') in ('queued','running') for r in rs) else 1)"; \
	}; \
	echo ">>> bootstrap: проверяю состояние pipeline..."; \
	if count_positive hudi.gold.transactions_by_hour && [ "$$(last_run_state)" = "success" ]; then \
	  echo "    gold наполнен и последний DAGRun=success — skip"; exit 0; \
	fi; \
	if has_active_run; then \
	  echo "    активный DAGRun уже есть (создан scheduler'ом) — жду без manual trigger"; \
	else \
	  echo "    активного DAGRun нет — триггерю manual run..."; \
	  af dags trigger transactions_pipeline >/dev/null 2>&1 || true; \
	fi; \
	echo ">>> жду success последнего DAGRun (до 25 мин: cold start Spark + bronze sensor + dbt silver/gold/test)..."; \
	for i in $$(seq 1 150); do \
	  sleep 10; \
	  state=$$(last_run_state); \
	  case "$$state" in \
	    success) echo "    DAGRun success (попытка $$i/150) — dbt_test пройден"; exit 0 ;; \
	    failed)  echo "ERR: DAGRun failed — проверь airflow UI: dag transactions_pipeline"; exit 1 ;; \
	  esac; \
	  if [ $$((i % 6)) = 0 ]; then echo "    жду DAGRun... state=$$state ($$i/150, ~$$((i / 6)) мин)"; fi; \
	done; \
	echo "ERR: DAGRun не завершился за 25 мин — посмотри SparkApplication bronze-s3-streaming, DAG transactions_pipeline"; exit 1

# Инициализирует Superset: создаёт database connection (Trino), datasets, charts и dashboard.
# Выполняется ВНУТРИ кластера: скрипт копируется в superset pod и запускается там
# с in-cluster URL http://localhost:8088 — никакого port-forward не требуется.
# Идемпотентно: повторный запуск пропускает уже существующие сущности.
superset-init:
	$(eval SUPERSET_POD := $(shell kubectl -n data-platform get pod -l app=superset,release=superset -o jsonpath='{.items[0].metadata.name}'))
	@test -n "$(SUPERSET_POD)" || (echo "Superset pod не найден" && exit 1)
	@echo ">>> Жду готовности Superset..."
	@kubectl -n data-platform wait --for=condition=ready pod/$(SUPERSET_POD) --timeout=300s
	@echo ">>> Копирую init_dashboards.py в pod $(SUPERSET_POD)..."
	kubectl -n data-platform cp superset/init_dashboards.py $(SUPERSET_POD):/tmp/init_dashboards.py -c superset
	@echo ">>> Запускаю инициализацию (REST API на localhost:8088 внутри пода)..."
	kubectl -n data-platform exec $(SUPERSET_POD) -c superset -- python /tmp/init_dashboards.py --host http://localhost:8088
