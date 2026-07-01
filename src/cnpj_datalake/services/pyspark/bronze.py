"""Bronze layer: raw text ingestion to Parquet in MinIO."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from src.cnpj_datalake.config import DataLakeConfig
from src.cnpj_datalake.domain import get_layout_columns, normalize_file_type
from src.cnpj_datalake.services.observability.metrics import observe_encoding_fallback
from src.cnpj_datalake.services.pyspark.spark import build_spark_session
from src.cnpj_datalake.utils.logger import get_logger


logger = get_logger(__name__)


class BronzeLayer:
    _REFERENCE_DATASETS = {"cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes"}
    _ENCODING_CANDIDATES = ("latin1", "cp1252", "utf-8", "utf-8-sig")

    def __init__(self, config: DataLakeConfig, spark: SparkSession | None = None):
        self.config = config
        self._owns_spark = spark is None
        self.spark = spark if spark is not None else build_spark_session(config)

    def _sanitize_replacement_characters(self, df, file_type: str, layout_columns: list[str]):
        if file_type not in self._REFERENCE_DATASETS or not layout_columns:
            return df

        replacement_char = "\ufffd"
        checks = [
            F.max(F.when(F.instr(F.col(col_name), replacement_char) > 0, F.lit(1)).otherwise(F.lit(0))).alias(col_name)
            for col_name in layout_columns
        ]
        row = df.select(*checks).first()
        if not row:
            return df

        broken_columns = [col_name for col_name in layout_columns if int(row[col_name] or 0) > 0]
        if not broken_columns:
            return df

        cols = ", ".join(broken_columns)
        logger.warning(
            "Caracteres de substituicao detectados no Bronze para %s. Colunas afetadas: %s. "
            "Prosseguindo com sanitizacao para nao bloquear a ingestao.",
            file_type,
            cols,
        )

        cleaned_df = df
        for col_name in broken_columns:
            cleaned_df = cleaned_df.withColumn(col_name, F.regexp_replace(F.col(col_name), replacement_char, ""))

        return cleaned_df

    @staticmethod
    def _replacement_rows_count(df) -> int:
        replacement_char = "\ufffd"
        checks = [
            F.sum(F.when(F.instr(F.col(col_name), replacement_char) > 0, F.lit(1)).otherwise(F.lit(0))).alias(col_name)
            for col_name in df.columns
        ]
        row = df.select(*checks).first()
        if not row:
            return 0
        return int(sum(int(row[col_name] or 0) for col_name in df.columns))

    def _read_raw_csv(self, paths: str | list[str], delimiter: str, encoding: str):
        return (
            self.spark.read
            .option("encoding", encoding)
            .option("header", "false")
            .option("sep", delimiter)
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "false")
            .option("mode", "PERMISSIVE")
            .csv(paths)
        )

    def _read_csv_with_best_encoding(self, paths: str | list[str], delimiter: str, file_type: str):
        default_encoding = self.config.input_file_encoding
        base_df = self._read_raw_csv(paths=paths, delimiter=delimiter, encoding=default_encoding)

        if file_type not in self._REFERENCE_DATASETS:
            return base_df

        base_bad = self._replacement_rows_count(base_df)
        if base_bad == 0:
            return base_df

        candidates = [default_encoding, *self._ENCODING_CANDIDATES]
        seen: set[str] = set()
        ordered_candidates: list[str] = []
        for encoding in candidates:
            normalized = encoding.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered_candidates.append(normalized)

        best_df = base_df
        best_encoding = default_encoding
        best_bad = base_bad

        for encoding in ordered_candidates:
            if encoding == default_encoding:
                continue
            try:
                trial_df = self._read_raw_csv(paths=paths, delimiter=delimiter, encoding=encoding)
                trial_bad = self._replacement_rows_count(trial_df)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Falha ao testar encoding %s para %s: %s",
                    encoding,
                    file_type,
                    exc,
                )
                continue

            if trial_bad < best_bad:
                best_df = trial_df
                best_bad = trial_bad
                best_encoding = encoding
                if best_bad == 0:
                    break

        if best_encoding != default_encoding:
            logger.warning(
                "Encoding ajustado automaticamente para %s no dataset %s (substituicoes %d -> %d).",
                best_encoding,
                file_type,
                base_bad,
                best_bad,
            )
            observe_encoding_fallback(
                file_type=file_type,
                from_encoding=default_encoding,
                to_encoding=best_encoding,
                data_version=self.config.data_version,
                replaced_rows_before=base_bad,
                replaced_rows_after=best_bad,
            )

        return best_df

    def ingest_csv(self, source_file: Path, file_type: str, delimiter: str = ";") -> tuple[str, int]:
        source_file = source_file.resolve()
        normalized_type = normalize_file_type(file_type)
        layout_columns = get_layout_columns(normalized_type)

        df, _ = self._parse_csv_rows(str(source_file), layout_columns, delimiter, normalized_type)
        df = self._sanitize_replacement_characters(df, normalized_type, layout_columns)

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

        df, _ = self._parse_csv_rows(file_paths, layout_columns, delimiter, normalized_type)
        df = self._sanitize_replacement_characters(df, normalized_type, layout_columns)

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

    def _parse_csv_rows(
        self,
        paths: str | list[str],
        layout_columns: list[str],
        delimiter: str,
        file_type: str,
    ) -> tuple:
        csv_df = self._read_csv_with_best_encoding(paths=paths, delimiter=delimiter, file_type=file_type)

        parsed_cols = len(csv_df.columns)
        if layout_columns:
            expected_cols = len(layout_columns)
            select_columns = [
                self._nullify(F.col(f"_c{i}")).alias(name)
                if f"_c{i}" in csv_df.columns
                else F.lit(None).cast("string").alias(name)
                for i, name in enumerate(layout_columns)
            ]
            return (
                csv_df.select(
                    *select_columns,
                    F.lit(parsed_cols).alias("_column_count"),
                    F.lit(expected_cols).alias("_layout_expected"),
                    F.when(F.lit(parsed_cols) == F.lit(expected_cols), F.lit("ok")).otherwise(F.lit("mismatch")).alias("_layout_status"),
                ),
                parsed_cols,
            )

        return (
            csv_df.select(
                *[
                    self._nullify(F.col(col_name)).alias(f"col_{i}")
                    for i, col_name in enumerate(csv_df.columns, start=1)
                ],
                F.lit(parsed_cols).alias("_column_count"),
            ),
            parsed_cols,
        )

    def stop(self) -> None:
        if self._owns_spark:
            self.spark.stop()
