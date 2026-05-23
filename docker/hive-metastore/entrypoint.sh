#!/bin/sh
set -eu

export HADOOP_HOME=/opt/hadoop-3.2.0
export HIVE_HOME=/opt/apache-hive-metastore-3.0.0-bin
export HADOOP_CLASSPATH="${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar:${HIVE_HOME}/lib/postgresql-42.7.4.jar"
export JAVA_HOME=/usr/local/openjdk-8

DB_HOST="${METASTORE_DB_HOSTNAME:-postgres.data-platform.svc.cluster.local}"
DB_PORT="${METASTORE_DB_PORT:-5432}"

echo "[hms] Waiting postgres at ${DB_HOST}:${DB_PORT}..."
while ! nc -z "${DB_HOST}" "${DB_PORT}"; do
  sleep 2
done
echo "[hms] Postgres reachable."

echo "[hms] Checking if schema already initialized..."
if ${HIVE_HOME}/bin/schematool -info -dbType postgres >/tmp/info.log 2>&1; then
  echo "[hms] Schema present, skipping init."
  cat /tmp/info.log | tail -5
else
  echo "[hms] Schema missing, running initSchema..."
  ${HIVE_HOME}/bin/schematool -initSchema -dbType postgres -verbose
fi

echo "[hms] Starting metastore..."
exec ${HIVE_HOME}/bin/start-metastore
