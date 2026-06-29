"""Gold layer: clean curated datasets ready for ad-hoc analytics."""

from __future__ import annotations

import psycopg2
from pyspark.sql import DataFrame, SparkSession, functions as F

from src.cnpj_datalake.config import DataLakeConfig
from src.cnpj_datalake.domain import get_layout_columns, normalize_file_type
from src.cnpj_datalake.services.pyspark.spark import build_spark_session
from src.cnpj_datalake.utils.logger import get_logger


logger = get_logger(__name__)

_GOLD_TABLES: dict[str, str] = {
    "empresas": "cnpj_gold.empresas",
    "estabelecimentos": "cnpj_gold.estabelecimentos",
    "socios": "cnpj_gold.socios",
    "cnaes": "cnpj_gold.cnaes",
    "motivos": "cnpj_gold.motivos",
    "municipios": "cnpj_gold.municipios",
    "naturezas": "cnpj_gold.naturezas",
    "paises": "cnpj_gold.paises",
    "qualificacoes": "cnpj_gold.qualificacoes",
}


class GoldLayer:
    def __init__(self, config: DataLakeConfig, spark: SparkSession | None = None):
        self.config = config
        self._owns_spark = spark is None
        self.spark = spark if spark is not None else build_spark_session(config)

    def aggregate(self, silver_path: str, file_type: str) -> tuple[str, int]:
        return self._passthrough(silver_path, file_type)

    def _write(self, df: DataFrame, dataset_name: str) -> tuple[str, int]:
        df = df.withColumn("dataset_month", F.lit(self.config.data_version))
        df = df.withColumn("data_version", F.lit(self.config.data_version))

        target_path = f"s3a://{self.config.minio.bucket_gold}/{self.config.data_version}/{dataset_name}"
        df = df.cache()
        df.write.mode("overwrite").parquet(target_path)
        count = df.count()

        table = _GOLD_TABLES.get(dataset_name, f"cnpj_gold.{dataset_name}")
        self._write_to_postgres(df, table)

        df.unpersist()
        return target_path, count

    def _write_to_postgres(self, df: DataFrame, table: str) -> None:
        pg = self.config.postgres
        try:
            with psycopg2.connect(
                host=pg.host,
                port=pg.port,
                dbname=pg.database,
                user=pg.user,
                password=pg.password,
                sslmode="prefer",
            ) as conn:
                schema_name, table_name = self._split_table_name(table)
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {table} WHERE dataset_month = %s OR data_version = %s",
                        (self.config.data_version, self.config.data_version),
                    )
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = %s
                          AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (schema_name, table_name),
                    )
                    target_columns = [row[0] for row in cur.fetchall()]
                conn.commit()

            str_df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
            valid_columns = [c for c in target_columns if c in str_df.columns]
            if not valid_columns:
                logger.warning("Nenhuma coluna compativel para gravar em %s", table)
                return
            str_df = str_df.select(*valid_columns)
            (
                str_df.write
                .format("jdbc")
                .option("url", pg.jdbc_url())
                .option("dbtable", table)
                .option("user", pg.user)
                .option("password", pg.password)
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )
            logger.info("Data mart atualizado: %s (data_version=%s)", table, self.config.data_version)
        except Exception as exc:
            logger.warning("Falha ao gravar data mart %s: %s", table, exc)

    @staticmethod
    def _split_table_name(table: str) -> tuple[str, str]:
        if "." not in table:
            return "public", table
        schema_name, table_name = table.split(".", 1)
        return schema_name, table_name

    def _passthrough(self, silver_path: str, file_type: str) -> tuple[str, int]:
        normalized_type = normalize_file_type(file_type)
        df = self.spark.read.parquet(silver_path)
        business_columns = [col for col in get_layout_columns(normalized_type) if col in df.columns]
        if business_columns:
            df = df.select(*business_columns)
        return self._write(df, normalized_type)

    def aggregate_estabelecimentos_by_uf(self, silver_path: str) -> str:
        path, _ = self._passthrough(silver_path, "estabelecimentos")
        return path

    def stop(self) -> None:
        if self._owns_spark:
            self.spark.stop()
