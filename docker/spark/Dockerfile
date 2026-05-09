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
    curl -fsSLO "${MAVEN_BASE}/org/apache/hudi/hudi-spark3.5-bundle_2.12/${HUDI_VERSION}/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/hudi/hudi-utilities-bundle_2.12/${HUDI_VERSION}/hudi-utilities-bundle_2.12-${HUDI_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN_BASE}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    chown -R spark:spark /opt/spark/jars

RUN pip install --no-cache-dir \
        "pyspark==3.5.8" \
        "dbt-core==1.8.7" \
        "dbt-spark[session]==1.8.0"

ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*

COPY conf/metrics.properties /opt/spark/conf/metrics.properties

USER spark
WORKDIR /opt/spark/work-dir
