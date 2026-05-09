.PHONY: help check up down ns secrets diff status spark-image hms \
        spark-code dbt-configmap airflow-dags airflow-trigger-pipeline superset-init \
        ingress hosts images airflow-image hms-image rbac airflow-unpause wait-airflow ensure-buckets verify-trino \
        bootstrap-pipeline streaming-apps kafka-streaming-app reference-batch reset-watermarks \
        monitoring monitoring-dashboards reset-cancellations

KIND_CONTEXT ?= docker-desktop

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
	@echo "  make streaming-apps           - применить long-running streaming SparkApplications (S3) + reference batch"
	@echo "  make reference-batch          - применить (или перезапустить) one-shot bronze-reference-batch SparkApplication"
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
	@echo "  make reset-cancellations      - одноразово вычистить дубли в bronze/silver cancellations"
	@echo "                                  (drop tables + clear stream checkpoint; следующий run пересоберёт)"
	@echo "  make reset-watermarks         - сбросить bronze.ingest_watermarks (при corrupted Hudi timeline)"
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
# Reference (users / test_users / promo_codes) — отдельный one-shot
# SparkApplication, см. lab08/ADR-002-reference-batch-split.md.
# Применяется здесь же, т.к. логически часть bronze-bootstrap'а: пайплайн
# silver/gold join'ится со справочниками. Идемпотентно: kubectl apply
# на Completed SparkApplication ничего не пересоздаёт; для перезапуска
# (если справочник реально обновили) — `make reference-batch`.
#
# NB: Kafka SparkApp применяется ОТДЕЛЬНО через `make kafka-streaming-app`,
# т.к. внешний брокер (kafka.ijklmn.xyz:9092) недоступен в большинстве сред —
# при включении он бы бесконечно рестартовал (см. ADR #24 в lab08/PLAN.md).
streaming-apps:
	@echo ">>> Применяю streaming SparkApplication (S3)..."
	kubectl apply -f k8s/spark-applications/bronze-s3-streaming.yaml >/dev/null
	@echo "    SparkApp applied. bronze.* появится через ~1-3 мин (cold start)."
	@$(MAKE) reference-batch

# Reference one-shot: после успеха остаётся в Completed, retry'и только
# по сетевым сбоям до S3 (restartPolicy: OnFailure × 3). Чтобы пересобрать
# bronze.users / test_users / promo_codes из обновлённого snapshot —
# вызови `make reference-batch` повторно: target снесёт старый SparkApp
# и создаст новый.
reference-batch:
	@echo ">>> Применяю bronze-reference-batch (one-shot)..."
	-kubectl -n spark-jobs delete sparkapplication bronze-reference-batch --ignore-not-found --wait=true >/dev/null 2>&1
	kubectl apply -f k8s/spark-applications/bronze-reference-batch.yaml >/dev/null
	@echo "    SparkApp applied. Дождись Completed:"
	@echo "      kubectl -n spark-jobs get sparkapp bronze-reference-batch -w"

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
	  for b in lake checkpoints artifacts; do \
	    kubectl -n storage exec $$MINIO_POD -c minio -- sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin123 >/dev/null 2>&1 && mc mb --ignore-existing local/$$b 2>&1 | grep -v 'already exists'" || true; \
	  done

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
	-kubectl delete -f k8s/spark-applications/bronze-reference-batch.yaml --ignore-not-found
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
	  --from-file=macros_run_date.sql=dbt/macros/run_date.sql \
	  --from-file=sources.yml=dbt/models/sources.yml \
	  --from-file=silver_transactions_clean.sql=dbt/models/silver/transactions_clean.sql \
	  --from-file=silver_cancellations_clean.sql=dbt/models/silver/cancellations_clean.sql \
	  --from-file=silver_exchange_rates_daily.sql=dbt/models/silver/exchange_rates_daily.sql \
	  --from-file=silver_exchange_rates_long.sql=dbt/models/silver/exchange_rates_long.sql \
	  --from-file=silver__silver.yml=dbt/models/silver/_silver.yml \
	  --from-file=gold_transactions_by_hour.sql=dbt/models/gold/transactions_by_hour.sql \
	  --from-file=gold_purchases_by_hour.sql=dbt/models/gold/purchases_by_hour.sql \
	  --from-file=gold_revenue_daily.sql=dbt/models/gold/revenue_daily.sql \
	  --from-file=gold_refunds_daily.sql=dbt/models/gold/refunds_daily.sql \
	  --from-file=gold_promo_codes_analysis.sql=dbt/models/gold/promo_codes_analysis.sql \
	  --from-file=gold_promo_expired_usage_daily.sql=dbt/models/gold/promo_expired_usage_daily.sql \
	  --from-file=gold_cancellations_summary.sql=dbt/models/gold/cancellations_summary.sql \
	  --from-file=gold_user_cohorts.sql=dbt/models/gold/user_cohorts.sql \
	  --from-file=gold_dq_summary_daily.sql=dbt/models/gold/dq_summary_daily.sql \
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
#   1) если gold уже наполнен → skip (любой success-run наполнил витрины);
#   2) если scheduler уже создал run (queued/running) → НЕ триггерим вручную,
#      иначе получаем 2 параллельных run'а (manual + scheduled), которые
#      max_active_runs=1 сериализует → пайплайн крутится дважды;
#   3) иначе триггерим manual run за прошлый день (cold-start).
#   4) ждём ПЕРВЫЙ успешный DAGRun (любой execution_date) — после него
#      gold уже содержит данные за тот day и Superset можно поднимать.
#      ВАЖНО: с catchup=True scheduler создаёт ~30 DAGRun-ов на месячную
#      историю и max_active_runs=1 гонит их последовательно. Ждать success
#      САМОГО свежего execution_date неправильно — это часы. Дашборд
#      работает с любого первого успеха.
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
	any_run_success() { \
	  af dags list-runs -d transactions_pipeline -o json 2>/dev/null \
	    | python3 -c "import sys,json; rs=json.load(sys.stdin) or []; sys.exit(0 if any(r.get('state')=='success' for r in rs) else 1)"; \
	}; \
	any_run_failed() { \
	  af dags list-runs -d transactions_pipeline -o json 2>/dev/null \
	    | python3 -c "import sys,json; rs=json.load(sys.stdin) or []; sys.exit(0 if any(r.get('state')=='failed' for r in rs) else 1)"; \
	}; \
	has_active_run() { \
	  af dags list-runs -d transactions_pipeline -o json 2>/dev/null \
	    | python3 -c "import sys,json; rs=json.load(sys.stdin) or []; sys.exit(0 if any(r.get('state') in ('queued','running') for r in rs) else 1)"; \
	}; \
	echo ">>> bootstrap: проверяю состояние pipeline..."; \
	if count_positive hudi.gold.transactions_by_hour; then \
	  echo "    gold наполнен — skip"; exit 0; \
	fi; \
	if has_active_run; then \
	  echo "    активный DAGRun уже есть (создан scheduler'ом) — жду без manual trigger"; \
	else \
	  echo "    активного DAGRun нет — триггерю manual run..."; \
	  af dags trigger transactions_pipeline >/dev/null 2>&1 || true; \
	fi; \
	echo ">>> жду первый success DAGRun (до 25 мин: cold start Spark + bronze sensor + dbt silver/gold/test)..."; \
	echo "    с catchup=True scheduler гонит ~30 catchup-runs последовательно — ждём ЛЮБОЙ success, не последний"; \
	for i in $$(seq 1 150); do \
	  sleep 10; \
	  if any_run_success; then \
	    echo "    DAGRun success получен (попытка $$i/150) — gold наполнен, можно поднимать Superset"; exit 0; \
	  fi; \
	  if any_run_failed; then \
	    echo "ERR: один из DAGRun-ов упал — проверь airflow UI: dag transactions_pipeline"; exit 1; \
	  fi; \
	  if [ $$((i % 6)) = 0 ]; then echo "    жду первый success... ($$i/150, ~$$((i / 6)) мин)"; fi; \
	done; \
	echo "ERR: ни один DAGRun не завершился успешно за 25 мин — посмотри SparkApplication bronze-s3-streaming, DAG transactions_pipeline"; exit 1

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

# Одноразовая очистка cross-partition дублей в bronze.cancellations и
# silver.cancellations_clean. После того как `bronze_s3_streaming.py` перешёл
# на GLOBAL_BLOOM index, новые upsert-ы дедуплицируются корректно, но УЖЕ
# накопленные дубли (~13051 строк) не пропадут сами — Hudi global-index
# решает дубль только когда ключ ещё раз приходит со стороны источника.
# Самый чистый путь — снести таблицы и checkpoint cancellations-стрима;
# при следующем тике стрим заново пройдёт по S3 и наполнит таблицу с нуля,
# уже без дублей. После этого нужно прогнать silver с --full-refresh.
#
# ВАЖНО: таргет НЕ трогает transactions/rates/reference — у них своя логика
# и их данные не повреждены.
reset-cancellations:
	@echo ">>> 1/3 Удаляю Hive table bronze.cancellations и silver.cancellations_clean..."
	@kubectl -n data-platform exec deploy/trino-coordinator -- \
	  trino --execute "DROP TABLE IF EXISTS hudi.bronze.cancellations" >/dev/null 2>&1 || true
	@kubectl -n data-platform exec deploy/trino-coordinator -- \
	  trino --execute "DROP TABLE IF EXISTS hudi.silver.cancellations_clean" >/dev/null 2>&1 || true
	@echo ">>> 2/3 Чищу S3-данные таблиц и checkpoint cancellations-стрима..."
	@kubectl -n storage delete pod mc-reset-cancel --ignore-not-found --wait=true >/dev/null 2>&1 || true
	@kubectl -n storage run mc-reset-cancel \
	  --image=minio/mc:RELEASE.2024-11-21T17-21-54Z --restart=Never \
	  --env=MC_CONFIG_DIR=/tmp/.mc --env=HOME=/tmp \
	  --command -- sh -c 'sleep 120' >/dev/null
	@kubectl -n storage wait --for=condition=Ready pod/mc-reset-cancel --timeout=60s >/dev/null
	@kubectl -n storage exec mc-reset-cancel -- sh -c '\
	  mc alias set m http://minio.storage.svc.cluster.local minioadmin minioadmin123 >/dev/null && \
	  mc rm --recursive --force m/lake/bronze/cancellations/ >/dev/null 2>&1 || true; \
	  mc rm --recursive --force m/lake/silver/cancellations_clean/ >/dev/null 2>&1 || true; \
	  mc rm --recursive --force m/hudi/.checkpoints/bronze-s3-stream-yc/cancellations/ >/dev/null 2>&1 || true; \
	  echo "    очищено: bronze/cancellations, silver/cancellations_clean, ckpt cancellations"'
	@kubectl -n storage delete pod mc-reset-cancel --wait=false >/dev/null 2>&1 || true
	@echo ">>> 3/3 Рестартую bronze-s3-streaming чтобы перечитать всё с нуля..."
	@kubectl -n spark-jobs delete sparkapplication bronze-s3-streaming --ignore-not-found --wait=true >/dev/null 2>&1 || true
	@kubectl apply -f k8s/spark-applications/bronze-s3-streaming.yaml >/dev/null
	@echo "    Готово. Дальше:"
	@echo "      1) Дождись, пока bronze.cancellations наполнится (~3-5 мин);"
	@echo "      2) Прогони silver с full-refresh: dbt run --select cancellations_clean --full-refresh"
	@echo "         (или дождись следующего DAG-run'а — incremental сам пересоздаст пустую таблицу)."

# Сбрасывает bronze.ingest_watermarks при corrupted Hudi timeline.
# Симптом: HoodieRollbackException "Found commits after time, please rollback greater commits first".
# Причина: multi-writer ситуация при сбое — inflight + completed коммиты вперемешку.
# bronze.ingest_watermarks — производная таблица: после сброса первый успешный
# foreachBatch / bootstrap её пересоздаст. Потеря данных — только история watermark'ов
# (sensor увидит партицию заново при следующем коммите).
reset-watermarks:
	@echo ">>> 1/3 Удаляю Hive table bronze.ingest_watermarks..."
	@kubectl -n data-platform exec deploy/trino-coordinator -- \
 	  trino --execute "DROP TABLE IF EXISTS hudi.bronze.ingest_watermarks" >/dev/null 2>&1 || true
	@echo ">>> 2/3 Чищу S3-данные таблицы..."
	@kubectl -n storage delete pod mc-reset-wm --ignore-not-found --wait=true >/dev/null 2>&1 || true
	@kubectl -n storage run mc-reset-wm \
 	  --image=minio/mc:RELEASE.2024-11-21T17-21-54Z --restart=Never \
 	  --env=MC_CONFIG_DIR=/tmp/.mc --env=HOME=/tmp \
 	  --command -- sh -c 'sleep 60' >/dev/null
	@kubectl -n storage wait --for=condition=Ready pod/mc-reset-wm --timeout=60s >/dev/null
	@kubectl -n storage exec mc-reset-wm -- sh -c '\
 	  mc alias set m http://minio.storage.svc.cluster.local minioadmin minioadmin123 >/dev/null && \
 	  mc rm --recursive --force m/lake/bronze/ingest_watermarks/ >/dev/null 2>&1 || true; \
 	  echo "    очищено: bronze/ingest_watermarks"'
	@kubectl -n storage delete pod mc-reset-wm --wait=false >/dev/null 2>&1 || true
	@echo ">>> 3/3 Рестартую bronze-s3-streaming чтобы пересоздать таблицу..."
	@kubectl -n spark-jobs delete sparkapplication bronze-s3-streaming --ignore-not-found --wait=true >/dev/null 2>&1 || true
	@kubectl apply -f k8s/spark-applications/bronze-s3-streaming.yaml >/dev/null
	@echo "    Готово. bootstrap_watermark_table создаст таблицу заново на старте."
