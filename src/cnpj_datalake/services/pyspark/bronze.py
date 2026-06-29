"""Bronze layer: raw text ingestion to Parquet in MinIO."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from src.cnpj_datalake.config import DataLakeConfig
from src.cnpj_datalake.domain import get_layout_columns, normalize_file_type
from src.cnpj_datalake.services.pyspark.spark import build_spark_session


class BronzeLayer:
    def __init__(self, config: DataLakeConfig, spark: SparkSession | None = None):
        self.config = config
        self._owns_spark = spark is None
        self.spark = spark if spark is not None else build_spark_session(config)

    def ingest_csv(self, source_file: Path, file_type: str, delimiter: str = ";") -> tuple[str, int]:
        source_file = source_file.resolve()
        normalized_type = normalize_file_type(file_type)
        layout_columns = get_layout_columns(normalized_type)

        raw = self.spark.read.option("encoding", "latin1").text(str(source_file))
        split_col = F.split(F.col("value"), delimiter, -1)

        if layout_columns:
            expected_cols = len(layout_columns)
            df = raw.select(
                *[
                    self._nullify(F.element_at(split_col, i + 1)).alias(name)
                    for i, name in enumerate(layout_columns)
                ],
                F.size(split_col).alias("_column_count"),
                F.lit(expected_cols).alias("_layout_expected"),
                F.when(F.size(split_col) == expected_cols, F.lit("ok")).otherwise(F.lit("mismatch")).alias("_layout_status"),
            )
        else:
            inferred_cols = raw.select(F.max(F.size(split_col)).alias("max_cols")).first()["max_cols"] or 1
            df = raw.select(
                *[
                    self._nullify(F.element_at(split_col, i)).alias(f"col_{i}")
                    for i in range(1, inferred_cols + 1)
                ],
                F.size(split_col).alias("_column_count"),
            )

        df = df.select(
            "*",
            F.current_timestamp().alias("ingestion_ts"),
            F.lit(source_file.name).alias("source_file"),
            F.lit(normalized_type).alias("file_type"),
            F.lit(self.config.data_version).alias("dataset_month"),
            F.lit(self.config.data_version).alias("data_version"),
        )

        target_path = f"s3a://{self.config.minio.bucket_bronze}/{self.config.data_version}/{normalized_type}/{source_file.stem}"
        df = df.cache()
        df.write.mode("overwrite").parquet(target_path)
        count = df.count()
        df.unpersist()
        return target_path, count

    @staticmethod
    def _nullify(column: F.Column) -> F.Column:
        cleaned = F.trim(column)
        return F.when(
            cleaned.isNull()
            | (cleaned == "")
            | (F.upper(cleaned).isin("NULL", "N/A", "NA", "NAN", "NAO APLICAVEL", "NÃO APLICÁVEL")),
            F.lit(None),
        ).otherwise(cleaned)

    def ingest_glob(self, source_pattern: str | Path, file_type: str, delimiter: str = ";") -> tuple[str, int]:
        pattern = Path(source_pattern)
        files = sorted(pattern.parent.glob(pattern.name))
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado para o padrão: {source_pattern}")

        normalized_type = normalize_file_type(file_type)
        layout_columns = get_layout_columns(normalized_type)
        file_paths = [str(f.resolve()) for f in files]

        raw = self.spark.read.option("encoding", "latin1").text(file_paths)
        split_col = F.split(F.col("value"), delimiter, -1)

        if layout_columns:
            expected_cols = len(layout_columns)
            df = raw.select(
                *[
                    self._nullify(F.element_at(split_col, i + 1)).alias(name)
                    for i, name in enumerate(layout_columns)
                ],
                F.size(split_col).alias("_column_count"),
                F.lit(expected_cols).alias("_layout_expected"),
                F.when(F.size(split_col) == expected_cols, F.lit("ok")).otherwise(F.lit("mismatch")).alias("_layout_status"),
            )
        else:
            inferred_cols = raw.select(F.max(F.size(split_col)).alias("max_cols")).first()["max_cols"] or 1
            df = raw.select(
                *[
                    self._nullify(F.element_at(split_col, i)).alias(f"col_{i}")
                    for i in range(1, inferred_cols + 1)
                ],
                F.size(split_col).alias("_column_count"),
            )

        df = df.select(
            "*",
            F.current_timestamp().alias("ingestion_ts"),
            F.input_file_name().alias("source_file"),
            F.lit(normalized_type).alias("file_type"),
            F.lit(self.config.data_version).alias("dataset_month"),
            F.lit(self.config.data_version).alias("data_version"),
        )

        target_path = f"s3a://{self.config.minio.bucket_bronze}/{self.config.data_version}/{normalized_type}"
        df = df.cache()
        df.write.mode("overwrite").parquet(target_path)
        count = df.count()
        df.unpersist()
        return target_path, count

    def stop(self) -> None:
        if self._owns_spark:
            self.spark.stop()
