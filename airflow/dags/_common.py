"""Shared spec builders and constants for the lab08 Airflow DAGs."""

from __future__ import annotations

from typing import Iterable

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


NAMESPACE = "spark-jobs"
SPARK_IMAGE = "lab08/spark:3.5.8-hudi-1.1.1"
JOBS_DIR = "local:///opt/spark/jobs"

SOURCE_BUCKET = "npl-de18-lab8-data"
SOURCE_ENDPOINT = "https://storage.yandexcloud.net"
SOURCE_ROOT = f"s3a://{SOURCE_BUCKET}"

BRONZE_SOURCES: tuple[str, ...] = ("transactions", "cancellations", "exchange_rates")
WATERMARK_PRODUCER: str = "transactions"

BRONZE_PYFILES: tuple[str, ...] = (
    f"{JOBS_DIR}/log_utils.py",
    f"{JOBS_DIR}/hudi_utils.py",
    f"{JOBS_DIR}/watermark_utils.py",
)

SPARK_CONF: dict[str, str] = {
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": "s3a://lake/warehouse/",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore.data-platform.svc.cluster.local:9083",
    "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "spark.kubernetes.namespace": NAMESPACE,
    "spark.kubernetes.executor.deleteOnTermination": "true",
    "spark.sql.shuffle.partitions": "16",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": str(32 * 1024 * 1024),
    "spark.metrics.namespace": "spark",
    "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
    "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus/",
    "spark.metrics.conf.driver.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
    "spark.metrics.conf.executor.source.jvm.class": "org.apache.spark.metrics.source.JvmSource",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "3",
    "spark.dynamicAllocation.initialExecutors": "1",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    "spark.dynamicAllocation.shuffleTracking.timeout": "120s",
}

UPSTREAM_BUCKET_CONF: dict[str, str] = {
    f"spark.hadoop.fs.s3a.bucket.{SOURCE_BUCKET}.endpoint": SOURCE_ENDPOINT,
    f"spark.hadoop.fs.s3a.bucket.{SOURCE_BUCKET}.path.style.access": "true",
    f"spark.hadoop.fs.s3a.bucket.{SOURCE_BUCKET}.connection.ssl.enabled": "true",
    f"spark.hadoop.fs.s3a.bucket.{SOURCE_BUCKET}.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    ),
}

_AWS_ENV_REFS: dict[str, dict[str, str]] = {
    "AWS_ACCESS_KEY_ID": {"name": "lab08-credentials", "key": "AWS_ACCESS_KEY_ID"},
    "AWS_SECRET_ACCESS_KEY": {"name": "lab08-credentials", "key": "AWS_SECRET_ACCESS_KEY"},
}

DRIVER_BASE: dict = {
    "cores": 1,
    "coreLimit": "1200m",
    "memory": "1g",
    "memoryOverhead": "256m",
    "serviceAccount": "spark",
    "envSecretKeyRefs": _AWS_ENV_REFS,
    "volumeMounts": [
        {"name": "jobs-code", "mountPath": "/opt/spark/jobs"},
        {"name": "dbt-cm", "mountPath": "/tmp/cm"},
    ],
}

EXECUTOR_BASE: dict = {
    "cores": 1,
    "coreLimit": "1200m",
    "instances": 1,
    "memory": "1g",
    "memoryOverhead": "512m",
    "envSecretKeyRefs": _AWS_ENV_REFS,
}


def _profile(
    *,
    driver_memory: str,
    driver_overhead: str,
    executor_memory: str,
    executor_overhead: str,
    executor_instances: int,
    dyn_min: int,
    dyn_max: int,
    dyn_initial: int,
    shuffle_partitions: int,
) -> dict:
    """Build a resource profile (driver + executor overrides + dyn alloc conf)."""
    return {
        "driver": {"memory": driver_memory, "memoryOverhead": driver_overhead},
        "executor": {
            "memory": executor_memory,
            "memoryOverhead": executor_overhead,
            "instances": executor_instances,
        },
        "conf": {
            "spark.dynamicAllocation.minExecutors": str(dyn_min),
            "spark.dynamicAllocation.maxExecutors": str(dyn_max),
            "spark.dynamicAllocation.initialExecutors": str(dyn_initial),
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
        },
    }


BRONZE_RESOURCE_PROFILES: dict[str, dict] = {
    "transactions": _profile(
        driver_memory="1g",
        driver_overhead="256m",
        executor_memory="1g",
        executor_overhead="512m",
        executor_instances=2,
        dyn_min=2,
        dyn_max=2,
        dyn_initial=2,
        shuffle_partitions=16,
    ),
    "cancellations": _profile(
        driver_memory="512m",
        driver_overhead="128m",
        executor_memory="512m",
        executor_overhead="256m",
        executor_instances=1,
        dyn_min=1,
        dyn_max=1,
        dyn_initial=1,
        shuffle_partitions=4,
    ),
    "exchange_rates": _profile(
        driver_memory="512m",
        driver_overhead="128m",
        executor_memory="512m",
        executor_overhead="256m",
        executor_instances=1,
        dyn_min=1,
        dyn_max=1,
        dyn_initial=1,
        shuffle_partitions=4,
    ),
}

VOLUMES: list[dict] = [
    {"name": "jobs-code", "configMap": {"name": "spark-jobs-code"}},
    {"name": "dbt-cm", "configMap": {"name": "dbt-project"}},
]


def build_spark_application_spec(
    *,
    name: str,
    main_file: str,
    arguments: list[str],
    py_files: Iterable[str],
    app_label: str,
    extra_conf: dict[str, str] | None = None,
    resource_profile: dict | None = None,
) -> dict:
    """Build a SparkApplication CRD body shared by bronze and dbt tasks.

    Args:
        name: Metadata name (a ``ts_nodash`` suffix is appended).
        main_file: ``local://`` path to the entry Python file.
        arguments: CLI args passed to the main file.
        py_files: Auxiliary Python files mounted as ``deps.pyFiles``.
        app_label: Label applied to driver and executor pods.
        extra_conf: Optional Spark conf overrides merged on top of
            ``SPARK_CONF``.
        resource_profile: Optional dict with ``driver``/``executor``
            override fragments and a ``conf`` block merged into Spark
            conf (typically dynamic-allocation tuning). See
            ``BRONZE_RESOURCE_PROFILES`` for the expected shape.

    Returns:
        Template spec consumable by ``SparkKubernetesOperator``.
    """
    profile = resource_profile or {}
    spark_conf = {**SPARK_CONF, **(extra_conf or {}), **profile.get("conf", {})}
    driver = {**DRIVER_BASE, **profile.get("driver", {}), "labels": {"app": app_label}}
    executor = {**EXECUTOR_BASE, **profile.get("executor", {}), "labels": {"app": app_label}}
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": f"{name}-" + "{{ ts_nodash | lower }}",
            "namespace": NAMESPACE,
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": main_file,
            "arguments": arguments,
            "deps": {"pyFiles": list(py_files)},
            "sparkVersion": "3.5.8",
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 2,
                "onFailureRetryInterval": 30,
                "onSubmissionFailureRetries": 3,
                "onSubmissionFailureRetryInterval": 30,
            },
            "timeToLiveSeconds": 600,
            "sparkConf": spark_conf,
            "driver": driver,
            "executor": executor,
            "volumes": VOLUMES,
        },
    }


def make_spark_task(
    *,
    task_id: str,
    template_spec: dict,
) -> SparkKubernetesOperator:
    """Standard ``SparkKubernetesOperator`` factory with sane defaults."""
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=NAMESPACE,
        template_spec=template_spec,
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        delete_on_termination=True,
        do_xcom_push=False,
        startup_timeout_seconds=600,
        reattach_on_restart=True,
        success_run_history_limit=1,
    )
