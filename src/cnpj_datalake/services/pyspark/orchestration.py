"""Simple Bronze -> Silver -> Gold orchestration with PostgreSQL tracking."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Iterable

from src.cnpj_datalake.config import DataLakeConfig
from src.cnpj_datalake.services.minio.client import MinioStorage
from src.cnpj_datalake.services.postgres.client import PostgresClient
from src.cnpj_datalake.services.pyspark.bronze import BronzeLayer
from src.cnpj_datalake.services.pyspark.gold import GoldLayer
from src.cnpj_datalake.services.pyspark.silver import SilverLayer
from src.cnpj_datalake.services.pyspark.spark import build_spark_session
from src.cnpj_datalake.utils.logger import get_logger


logger = get_logger(__name__)


def _resolve_config(data_version: str | None) -> DataLakeConfig:
    config = DataLakeConfig.from_env()
    if data_version:
        config = dataclasses.replace(config, data_version=data_version)
    return config


def _is_glob(source: str | Path) -> bool:
    s = str(source)
    return "*" in s or "?" in s


def run_bronze_stage(
    source_file: str | Path,
    file_type: str,
    data_version: str | None = None,
) -> str:
    config = _resolve_config(data_version)
    MinioStorage(config.minio).ensure_buckets()
    bronze = BronzeLayer(config)
    try:
        if _is_glob(source_file):
            path, count = bronze.ingest_glob(source_pattern=source_file, file_type=file_type)
        else:
            path, count = bronze.ingest_csv(source_file=Path(source_file), file_type=file_type)
        logger.info("Bronze concluida: %s (%d registros)", path, count)
        return path
    finally:
        bronze.stop()


def run_silver_stage(
    bronze_path: str,
    file_type: str,
    primary_keys: Iterable[str] | None = None,
    data_version: str | None = None,
) -> str:
    config = _resolve_config(data_version)
    silver = SilverLayer(config)
    try:
        path, count = silver.process(bronze_path=bronze_path, file_type=file_type, primary_keys=primary_keys)
        logger.info("Silver concluida: %s (%d registros)", path, count)
        return path
    finally:
        silver.stop()


def run_gold_stage(
    silver_path: str,
    file_type: str = "estabelecimentos",
    data_version: str | None = None,
) -> str:
    config = _resolve_config(data_version)
    gold = GoldLayer(config)
    try:
        path, count = gold.aggregate(silver_path=silver_path, file_type=file_type)
        logger.info("Gold concluida: %s (%d registros)", path, count)
        return path
    finally:
        gold.stop()


def run_pipeline(
    source_file: Path,
    file_type: str,
    primary_keys: Iterable[str] | None = None,
    data_version: str | None = None,
) -> dict:
    config = _resolve_config(data_version)
    MinioStorage(config.minio).ensure_buckets()

    postgres = PostgresClient(config.postgres)
    execution_id = postgres.start_pipeline_execution(
        "bronze_silver_gold",
        str(source_file),
        data_version=config.data_version,
    )

    spark = build_spark_session(config)
    try:
        bronze = BronzeLayer(config, spark=spark)
        silver = SilverLayer(config, spark=spark)
        gold = GoldLayer(config, spark=spark)

        if _is_glob(source_file):
            bronze_path, bronze_count = bronze.ingest_glob(source_pattern=source_file, file_type=file_type)
        else:
            bronze_path, bronze_count = bronze.ingest_csv(source_file=Path(source_file), file_type=file_type)
        logger.info("Bronze concluida: %s (%d registros)", bronze_path, bronze_count)

        silver_path, silver_count = silver.process(
            bronze_path=bronze_path,
            file_type=file_type,
            primary_keys=primary_keys,
        )
        logger.info("Silver concluida: %s (%d registros)", silver_path, silver_count)

        gold_path, gold_count = gold.aggregate(silver_path=silver_path, file_type=file_type)
        logger.info("Gold concluida: %s (%d registros)", gold_path, gold_count)

        postgres.finish_pipeline_execution(
            execution_id=execution_id,
            status="completed",
            records_processed=bronze_count,
            records_failed=0,
            output_path=gold_path,
        )

        logger.info("Pipeline concluido com sucesso")
        return {
            "status": "completed",
            "bronze_path": bronze_path,
            "silver_path": silver_path,
            "gold_path": gold_path,
            "records_processed": bronze_count,
        }
    except Exception as exc:
        postgres.finish_pipeline_execution(
            execution_id=execution_id,
            status="failed",
            records_processed=0,
            records_failed=1,
            output_path=None,
        )
        logger.exception("Falha na execucao do pipeline: %s", exc)
        raise
    finally:
        spark.stop()
