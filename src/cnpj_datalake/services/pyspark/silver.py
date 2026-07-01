"""Silver layer: cleansing, normalization, deduplication, and type casting."""

from __future__ import annotations

from typing import Callable, Iterable

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.cnpj_datalake.config import DataLakeConfig
from src.cnpj_datalake.services.pyspark.spark import build_spark_session


_DATE_FMT = "yyyyMMdd"


def _parse_capital_social() -> F.Column:
    raw = F.trim(F.col("capital_social"))
    # Remove aspas/moeda e preserva apenas digitos e separadores numericos.
    sanitized = F.regexp_replace(raw, r"[^0-9,\.\-]", "")
    # Se houver virgula, assume formato brasileiro: remove separador de milhar e troca decimal para ponto.
    normalized = F.when(
        F.instr(sanitized, ",") > 0,
        F.regexp_replace(F.regexp_replace(sanitized, r"\.", ""), ",", "."),
    ).otherwise(sanitized)
    return normalized.cast("decimal(18,2)")

_SILVER_CASTS: dict[str, list[tuple[str, Callable[[], F.Column]]]] = {
    "empresas": [
        ("capital_social", _parse_capital_social),
    ],
    "estabelecimentos": [
        ("data_situacao_cadastral", lambda: F.to_date(F.col("data_situacao_cadastral"), _DATE_FMT)),
        ("data_inicio_atividade", lambda: F.to_date(F.col("data_inicio_atividade"), _DATE_FMT)),
        ("data_situacao_especial", lambda: F.to_date(F.col("data_situacao_especial"), _DATE_FMT)),
    ],
    "socios": [
        ("data_entrada_sociedade", lambda: F.to_date(F.col("data_entrada_sociedade"), _DATE_FMT)),
    ],
}

_DEFAULT_PRIMARY_KEYS: dict[str, list[str]] = {
    "empresas": ["cnpj_basico"],
    "estabelecimentos": ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "cnpj_cpf_socio",
        "nome_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
    ],
    "cnaes": ["codigo"],
    "motivos": ["codigo"],
    "municipios": ["codigo"],
    "naturezas": ["codigo"],
    "paises": ["codigo"],
    "qualificacoes": ["codigo"],
}

_DATE_COLUMNS: dict[str, set[str]] = {
    "estabelecimentos": {
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    },
    "socios": {
        "data_entrada_sociedade",
    },
}


class SilverLayer:
    def __init__(self, config: DataLakeConfig, spark: SparkSession | None = None):
        self.config = config
        self._owns_spark = spark is None
        self.spark = spark if spark is not None else build_spark_session(config)

    def process(
        self,
        bronze_path: str,
        file_type: str,
        primary_keys: Iterable[str] | None = None,
    ) -> tuple[str, int]:
        df = self.spark.read.parquet(bronze_path)
        cleaned = self._normalize_strings(df)
        cleaned = self._cast_types(cleaned, file_type)
        resolved_primary_keys = self._resolve_primary_keys(file_type, primary_keys)
        if resolved_primary_keys:
            cleaned = cleaned.dropDuplicates(resolved_primary_keys)

        target_path = f"s3a://{self.config.minio.bucket_silver}/{self.config.data_version}/{file_type}/"

        cleaned = cleaned.cache()
        count = cleaned.count()
        self._validate_quality(cleaned, resolved_primary_keys, count)
        cleaned.write.mode("overwrite").parquet(target_path)
        cleaned.unpersist()
        return target_path, count

    @staticmethod
    def _resolve_primary_keys(file_type: str, primary_keys: Iterable[str] | None) -> list[str]:
        if primary_keys:
            return list(primary_keys)
        return _DEFAULT_PRIMARY_KEYS.get(file_type, [])

    def _validate_quality(
        self,
        df: DataFrame,
        primary_keys: Iterable[str] | None,
        total: int,
    ) -> None:
        if not primary_keys or total == 0:
            return
        pk = next(iter(primary_keys), None)
        if not pk or pk not in df.columns:
            return
        null_count = df.filter(F.col(pk).isNull()).count()
        null_rate = null_count / total
        threshold = 1 - self.config.quality_threshold / 100
        if null_rate > threshold:
            raise ValueError(
                f"Qualidade abaixo do threshold: {null_rate:.1%} nulos em '{pk}' "
                f"(limite: {threshold:.1%})"
            )

    @staticmethod
    def _cast_types(df: DataFrame, file_type: str) -> DataFrame:
        casts = dict(_SILVER_CASTS.get(file_type, []))
        if not casts:
            return df
        date_columns = _DATE_COLUMNS.get(file_type, set())
        selected_columns: list[F.Column] = []
        for field in df.schema.fields:
            if field.name in date_columns and f"{field.name}_raw" not in df.columns:
                selected_columns.append(F.col(field.name).alias(f"{field.name}_raw"))

            if field.name in casts:
                selected_columns.append(casts[field.name]().alias(field.name))
            else:
                selected_columns.append(F.col(field.name))

        return df.select(selected_columns)

    @staticmethod
    def _normalize_strings(df: DataFrame) -> DataFrame:
        return df.select([
            # Remove aspas residuais em campos textuais para evitar sujeira em joins/filtros.
            F.regexp_replace(F.trim(F.col(f.name)), r"[\"']", "").alias(f.name)
            if f.dataType.simpleString() == "string"
            else F.col(f.name)
            for f in df.schema.fields
        ])

    def stop(self) -> None:
        if self._owns_spark:
            self.spark.stop()
