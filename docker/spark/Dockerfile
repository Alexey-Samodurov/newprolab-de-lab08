# Кастомный Spark image для lab08:
#  - Spark 3.5.8 / Scala 2.12 / Java 17 / Python 3
#  - Hudi 1.1.1 (spark + utilities bundles, для DeltaStreamer)
#  - hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 (S3a)
#  - spark-sql-kafka 3.5.8 + transitive deps (Kafka source)
#  - dbt-core / dbt-spark для запуска моделей через Thrift

FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

ARG HUDI_VERSION=1.1.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG SPARK_VERSION=3.5.8
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN_BASE=https://repo1.maven.org/maven2

WORKDIR /opt/spark/jars

RUN set -eux; \
    # Hudi
    curl -fsSLO "${MAVEN_BASE}/org/apache/hudi/hudi-spark3.5-bundle_2.12/${HUDI_VERSION}/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/hudi/hudi-utilities-bundle_2.12/${HUDI_VERSION}/hudi-utilities-bundle_2.12-${HUDI_VERSION}.jar"; \
    # S3a
    curl -fsSLO "${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    # Kafka
    curl -fsSLO "${MAVEN_BASE}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    chown -R spark:spark /opt/spark/jars

# Python инструменты (dbt запускается из Spark Thrift Server pod через sidecar или отдельно)
RUN pip install --no-cache-dir \
        dbt-core==1.8.7 \
        dbt-spark[PyHive]==1.8.0

# Sane дефолты для S3a (значения переопределяются через spark-conf SparkApplication)
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*

USER spark
WORKDIR /opt/spark/work-dir
