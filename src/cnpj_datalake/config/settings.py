"""Configuração central do Data Lake com suporte a PySpark, MinIO e PostgreSQL."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv


load_dotenv()


@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def sqlalchemy_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    bucket_bronze: str
    bucket_silver: str
    bucket_gold: str

    @property
    def endpoint_url(self) -> str:
        protocol = "https" if self.secure else "http"
        return f"{protocol}://{self.endpoint}"


@dataclass
class SparkConfig:
    app_name: str
    master: str
    driver_memory: str
    executor_memory: str


@dataclass
class DataLakeConfig:
    project_root: Path
    data_version: str
    batch_size: int
    quality_threshold: float
    postgres: PostgresConfig
    minio: MinioConfig
    spark: SparkConfig
    input_file_encoding: str = "latin1"
    metrics_enabled: bool = True
    prometheus_pushgateway_url: str = "http://localhost:9091"
    prometheus_job_name: str = "cnpj_datalake_pipeline"

    @classmethod
    def from_env(cls) -> "DataLakeConfig":
        project_root = Path(os.getenv("PROJECT_ROOT", ".")).resolve()

        postgres = PostgresConfig(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432")),
            database=os.getenv("PG_DATABASE", "cnpj_datalake"),
            user=os.getenv("PG_USER", "datalake_app"),
            password=os.getenv("PG_PASSWORD", "datalake_app_change_me"),
        )

        minio = MinioConfig(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "cnpj_app_user"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "cnpj_app_change_me"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            bucket_bronze=os.getenv("MINIO_BUCKET_BRONZE", "cnpj-bronze"),
            bucket_silver=os.getenv("MINIO_BUCKET_SILVER", "cnpj-silver"),
            bucket_gold=os.getenv("MINIO_BUCKET_GOLD", "cnpj-gold"),
        )

        spark = SparkConfig(
            app_name=os.getenv("SPARK_APP_NAME", "cnpj-datalake"),
            master=os.getenv("SPARK_MASTER", "local[*]"),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        )

        return cls(
            project_root=project_root,
            data_version=(
                os.getenv("INGESTION_DATA_MONTH", "").strip()
                or os.getenv("INGESTION_DATA_VERSION", "").strip()
                or os.getenv("DATA_VERSION", "2026-03").strip()
            ),
            batch_size=int(os.getenv("BATCH_SIZE", "100000")),
            quality_threshold=float(os.getenv("QUALITY_THRESHOLD", "90.0")),
            postgres=postgres,
            minio=minio,
            spark=spark,
            input_file_encoding=os.getenv("INPUT_FILE_ENCODING", "latin1").strip() or "latin1",
            metrics_enabled=os.getenv("METRICS_ENABLED", "true").strip().lower() == "true",
            prometheus_pushgateway_url=(
                os.getenv("PROMETHEUS_PUSHGATEWAY_URL", "http://localhost:9091").strip()
                or "http://localhost:9091"
            ),
            prometheus_job_name=(
                os.getenv("PROMETHEUS_JOB_NAME", "cnpj_datalake_pipeline").strip()
                or "cnpj_datalake_pipeline"
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_root": str(self.project_root),
            "data_version": self.data_version,
            "batch_size": self.batch_size,
            "quality_threshold": self.quality_threshold,
            "postgres": {
                "host": self.postgres.host,
                "port": self.postgres.port,
                "database": self.postgres.database,
                "user": self.postgres.user,
            },
            "minio": {
                "endpoint": self.minio.endpoint,
                "secure": self.minio.secure,
                "bucket_bronze": self.minio.bucket_bronze,
                "bucket_silver": self.minio.bucket_silver,
                "bucket_gold": self.minio.bucket_gold,
            },
            "spark": {
                "app_name": self.spark.app_name,
                "master": self.spark.master,
                "driver_memory": self.spark.driver_memory,
                "executor_memory": self.spark.executor_memory,
            },
            "input_file_encoding": self.input_file_encoding,
            "metrics_enabled": self.metrics_enabled,
            "prometheus_pushgateway_url": self.prometheus_pushgateway_url,
            "prometheus_job_name": self.prometheus_job_name,
        }


def get_config() -> DataLakeConfig:
    if not hasattr(get_config, "_instance"):
        get_config._instance = DataLakeConfig.from_env()
    return get_config._instance