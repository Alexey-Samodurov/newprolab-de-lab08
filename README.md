# Lab08 — Транзакционная Аналитика

Аналитический пайплайн для финтех-данных: батч-загрузка из S3, стриминг из Kafka, medallion-хранилище на Hudi/MinIO, трансформации через dbt, визуализация в Superset.

## Quickstart

```bash
# Поднять весь стенд
make up

# Снести
make down
```

Перед первым `make up`: `cp k8s/secrets.example.yaml k8s/secrets.yaml` и заполнить креды. Подробности — в [Deployment Guide](docs/03-deployment-guide.md).

## Документация

- [Detailed Architecture](docs/01-detailed-architecture.md) — C4-диаграммы (Context / Container / Component), сценарии взаимодействия.
- [User Guide](docs/02-user-guide.md) — пошаговые сценарии работы со стендом, FAQ, глоссарий.
- [Deployment Guide](docs/03-deployment-guide.md) — требования, конфигурация, установка, upgrade / rollback / uninstall, troubleshooting.
