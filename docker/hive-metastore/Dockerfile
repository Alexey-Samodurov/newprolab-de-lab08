# Минимальный Hive Metastore standalone (postgres backend, S3a).
# База — bitsondatadev/hive-metastore (внутри уже распакован hive-metastore-3.0.0 + hadoop-3.2.0).
# Подменяем entrypoint и докидываем postgres jdbc driver.
FROM bitsondatadev/hive-metastore:latest

USER root

ARG POSTGRES_JDBC_VERSION=42.7.4

# В базовом образе уже есть curl и netcat (busybox), apt-get не нужен.
RUN curl -fsSL -o /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY entrypoint.sh /entrypoint-pg.sh
RUN chmod +x /entrypoint-pg.sh

ENTRYPOINT ["/bin/sh", "-c", "/entrypoint-pg.sh"]
