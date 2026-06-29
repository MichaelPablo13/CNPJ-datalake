import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
import platform
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from src.cnpj_datalake.config import DataLakeConfig, MinioConfig, PostgresConfig, SparkConfig
from src.cnpj_datalake.services.pyspark.bronze import BronzeLayer
from src.cnpj_datalake.services.pyspark.gold import GoldLayer
from src.cnpj_datalake.services.pyspark.silver import SilverLayer


def _build_config(month: str = "2026-03") -> DataLakeConfig:
    return DataLakeConfig(
        project_root=Path("."),
        data_version=month,
        batch_size=100000,
        quality_threshold=90.0,
        postgres=PostgresConfig(host="localhost", port=5432, database="lake", user="app", password="secret"),
        minio=MinioConfig(
            endpoint="localhost:9000",
            access_key="access",
            secret_key="secret",
            secure=False,
            bucket_bronze="cnpj-bronze",
            bucket_silver="cnpj-silver",
            bucket_gold="cnpj-gold",
        ),
        spark=SparkConfig(app_name="tests", master="local[1]", driver_memory="1g", executor_memory="1g"),
    )


class MonthPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("month-pipeline-tests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_bronze_ingest_adds_dataset_month(self):
        config = _build_config("2026-03")
        bronze = BronzeLayer(config, spark=self.spark)
        captured = {}

        def fake_parquet(writer_self, path, *args, **kwargs):
            captured["path"] = path
            captured["rows"] = [row.asDict() for row in writer_self._df.collect()]

        with tempfile.TemporaryDirectory() as tmpdir, patch("pyspark.sql.readwriter.DataFrameWriter.parquet", new=fake_parquet):
            source = Path(tmpdir) / "Cnaes.txt"
            source.write_text("01;Comercio\n", encoding="latin1")
            target_path, count = bronze.ingest_csv(source, "cnaes")

        self.assertEqual(count, 1)
        self.assertEqual(target_path, "s3a://cnpj-bronze/2026-03/cnaes/Cnaes")
        self.assertEqual(captured["path"], target_path)
        self.assertEqual(captured["rows"][0]["dataset_month"], "2026-03")
        self.assertEqual(captured["rows"][0]["data_version"], "2026-03")

    def test_bronze_payload_contains_expected_columns_before_minio_write(self):
        config = _build_config("2026-03")
        bronze = BronzeLayer(config, spark=self.spark)
        captured = {}

        def fake_parquet(writer_self, path, *args, **kwargs):
            captured["path"] = path
            captured["rows"] = [row.asDict() for row in writer_self._df.collect()]

        with tempfile.TemporaryDirectory() as tmpdir, patch("pyspark.sql.readwriter.DataFrameWriter.parquet", new=fake_parquet):
            source = Path(tmpdir) / "Empresas.txt"
            source.write_text("123;\" ACME LTDA \";2062;49;120000,00;01;\n", encoding="latin1")
            target_path, count = bronze.ingest_csv(source, "empresas")

        row = captured["rows"][0]
        self.assertEqual(count, 1)
        self.assertEqual(target_path, "s3a://cnpj-bronze/2026-03/empresas/Empresas")
        self.assertEqual(captured["path"], target_path)

        self.assertEqual(row["cnpj_basico"], "123")
        self.assertEqual(row["razao_social"], '" ACME LTDA "')
        self.assertEqual(row["capital_social"], "120000,00")
        self.assertEqual(row["file_type"], "empresas")
        self.assertEqual(row["source_file"], "Empresas.txt")
        self.assertEqual(row["dataset_month"], "2026-03")
        self.assertEqual(row["data_version"], "2026-03")
        self.assertEqual(row["_layout_status"], "ok")
        self.assertIn("ingestion_ts", row)

    def test_silver_process_preserves_dataset_month(self):
        config = _build_config("2026-03")
        fake_df = MagicMock()
        fake_df.count.return_value = 1
        fake_df.cache.return_value = fake_df
        fake_df.unpersist.return_value = None
        fake_writer = MagicMock()
        fake_df.write.mode.return_value = fake_writer
        fake_spark = MagicMock()
        fake_spark.read.parquet.return_value = fake_df
        silver = SilverLayer(config, spark=fake_spark)

        with patch.object(SilverLayer, "_normalize_strings", return_value=fake_df) as normalize_mock, \
            patch.object(SilverLayer, "_cast_types", return_value=fake_df) as cast_mock, \
            patch.object(SilverLayer, "_validate_quality", return_value=None) as validate_mock:
            target_path, count = silver.process(bronze_path="dummy", file_type="cnaes")

        self.assertEqual(count, 1)
        self.assertEqual(target_path, "s3a://cnpj-silver/2026-03/cnaes/")
        normalize_mock.assert_called_once_with(fake_df)
        cast_mock.assert_called_once_with(fake_df, "cnaes")
        validate_mock.assert_called_once()
        fake_df.write.mode.assert_called_once_with("overwrite")
        fake_writer.parquet.assert_called_once_with(target_path)

    @unittest.skipIf(platform.system() == "Windows", "Instabilidade conhecida do worker PySpark no Windows para este teste de collect")
    def test_silver_payload_contains_cleaned_values_before_minio_write(self):
        config = _build_config("2026-03")
        captured = {}

        input_df = self.spark.createDataFrame(
            [(" 01 ", ' "Comercio" ', "2026-03", "2026-03")],
            ["codigo", "descricao", "dataset_month", "data_version"],
        )

        fake_spark = MagicMock()
        fake_spark.read.parquet.return_value = input_df
        silver = SilverLayer(config, spark=fake_spark)

        def fake_parquet(writer_self, path, *args, **kwargs):
            captured["path"] = path
            captured["rows"] = [row.asDict() for row in writer_self._df.collect()]

        with patch("pyspark.sql.readwriter.DataFrameWriter.parquet", new=fake_parquet):
            target_path, count = silver.process(bronze_path="dummy", file_type="cnaes")

        row = captured["rows"][0]
        self.assertEqual(count, 1)
        self.assertEqual(target_path, "s3a://cnpj-silver/2026-03/cnaes/")
        self.assertEqual(captured["path"], target_path)
        self.assertEqual(row["codigo"], "01")
        self.assertEqual(row["descricao"], "Comercio")
        self.assertEqual(row["dataset_month"], "2026-03")
        self.assertEqual(row["data_version"], "2026-03")

    @unittest.skipIf(platform.system() == "Windows", "Instabilidade conhecida do worker PySpark no Windows para este teste de collect")
    def test_silver_normalize_strings_removes_quotes(self):
        df = self.spark.createDataFrame(
            [
                (' "ACME LTDA" ',),
                ("'JOAO'",),
            ],
            ["razao_social"],
        )

        normalized = SilverLayer._normalize_strings(df)
        values = [row[0] for row in normalized.collect()]

        self.assertEqual(values[0], "ACME LTDA")
        self.assertEqual(values[1], "JOAO")

    @unittest.skipIf(platform.system() == "Windows", "Instabilidade conhecida do worker PySpark no Windows para este teste de collect")
    def test_silver_casts_capital_social_brazilian_formats(self):
        df = self.spark.createDataFrame(
            [
                ("0,00",),
                ("120000,00",),
                ("1.234.567,89",),
                ('"45,10"',),
            ],
            ["capital_social"],
        )

        casted = SilverLayer._cast_types(df, "empresas")
        values = [row[0] for row in casted.collect()]

        self.assertEqual(values[0], Decimal("0.00"))
        self.assertEqual(values[1], Decimal("120000.00"))
        self.assertEqual(values[2], Decimal("1234567.89"))
        self.assertEqual(values[3], Decimal("45.10"))

    def test_gold_passthrough_adds_dataset_month(self):
        config = _build_config("2026-03")
        fake_df = MagicMock()
        fake_df.withColumn.return_value = fake_df
        fake_df.cache.return_value = fake_df
        fake_df.count.return_value = 1
        fake_df.unpersist.return_value = None
        fake_writer = MagicMock()
        fake_df.write.mode.return_value = fake_writer
        fake_spark = MagicMock()
        fake_spark.read.parquet.return_value = fake_df
        gold = GoldLayer(config, spark=fake_spark)

        with patch.object(GoldLayer, "_write_to_postgres", return_value=None) as pg_mock:
            target_path, count = gold.aggregate("dummy", "cnaes")

        self.assertEqual(count, 1)
        self.assertEqual(target_path, "s3a://cnpj-gold/2026-03/cnaes")
        self.assertEqual(fake_df.withColumn.call_count, 2)
        fake_df.write.mode.assert_called_once_with("overwrite")
        fake_writer.parquet.assert_called_once_with(target_path)
        pg_mock.assert_called_once()

    def test_gold_empresas_uses_clean_table_name(self):
        config = _build_config("2026-03")
        fake_df = MagicMock()
        fake_df.columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "porte_empresa",
            "dataset_month",
            "data_version",
            "_column_count",
        ]
        fake_df.withColumn.return_value = fake_df
        fake_df.cache.return_value = fake_df
        fake_df.count.return_value = 1
        fake_df.unpersist.return_value = None
        fake_df.select.return_value = fake_df
        fake_writer = MagicMock()
        fake_df.write.mode.return_value = fake_writer
        fake_spark = MagicMock()
        fake_spark.read.parquet.return_value = fake_df
        gold = GoldLayer(config, spark=fake_spark)

        with patch.object(GoldLayer, "_write_to_postgres", return_value=None) as pg_mock:
            target_path, count = gold.aggregate("dummy", "empresas")

        self.assertEqual(count, 1)
        self.assertEqual(target_path, "s3a://cnpj-gold/2026-03/empresas")
        pg_mock.assert_called_once()
        self.assertEqual(pg_mock.call_args.args[1], "cnpj_gold.empresas")

    def test_gold_write_to_postgres_filters_extra_columns(self):
        config = _build_config("2026-03")
        gold = GoldLayer(config, spark=MagicMock())

        projected_df = MagicMock()
        write_builder = MagicMock()
        projected_df.write = write_builder
        write_builder.format.return_value = write_builder
        write_builder.option.return_value = write_builder
        write_builder.mode.return_value = write_builder

        string_df = MagicMock()
        string_df.columns = ["codigo", "descricao", "dataset_month", "data_version", "_column_count"]
        string_df.select.return_value = projected_df

        fake_df = MagicMock()
        fake_df.columns = ["codigo", "descricao", "dataset_month", "data_version", "_column_count"]
        fake_df.select.return_value = string_df

        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ("codigo",),
            ("descricao",),
            ("dataset_month",),
            ("data_version",),
        ]

        with patch("src.cnpj_datalake.services.pyspark.gold.psycopg2.connect") as connect_mock:
            connect_mock.return_value.__enter__.return_value = conn
            gold._write_to_postgres(fake_df, "cnpj_gold.cnaes")

        string_df.select.assert_called_once_with("codigo", "descricao", "dataset_month", "data_version")
        write_builder.save.assert_called_once()

    def test_gold_aggregate_mocks_insertion_targets_minio_and_postgres(self):
        config = _build_config("2026-03")

        fake_df = MagicMock()
        fake_df.columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ]
        fake_df.select.return_value = fake_df
        fake_df.withColumn.return_value = fake_df
        fake_df.cache.return_value = fake_df
        fake_df.count.return_value = 10
        fake_df.unpersist.return_value = None

        fake_writer = MagicMock()
        fake_df.write.mode.return_value = fake_writer

        fake_spark = MagicMock()
        fake_spark.read.parquet.return_value = fake_df

        gold = GoldLayer(config, spark=fake_spark)

        with patch.object(GoldLayer, "_write_to_postgres", return_value=None) as pg_mock:
            target_path, count = gold.aggregate("s3a://cnpj-silver/2026-03/empresas/", "empresas")

        self.assertEqual(count, 10)
        self.assertEqual(target_path, "s3a://cnpj-gold/2026-03/empresas")
        fake_writer.parquet.assert_called_once_with("s3a://cnpj-gold/2026-03/empresas")
        pg_mock.assert_called_once_with(fake_df, "cnpj_gold.empresas")

    def test_gold_write_to_postgres_uses_expected_jdbc_table(self):
        config = _build_config("2026-03")
        gold = GoldLayer(config, spark=MagicMock())

        projected_df = MagicMock()
        write_builder = MagicMock()
        projected_df.write = write_builder
        write_builder.format.return_value = write_builder
        write_builder.option.return_value = write_builder
        write_builder.mode.return_value = write_builder

        string_df = MagicMock()
        string_df.columns = ["cnpj_basico", "razao_social", "dataset_month", "data_version"]
        string_df.select.return_value = projected_df

        fake_df = MagicMock()
        fake_df.columns = ["cnpj_basico", "razao_social", "dataset_month", "data_version"]
        fake_df.select.return_value = string_df

        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ("cnpj_basico",),
            ("razao_social",),
            ("dataset_month",),
            ("data_version",),
        ]

        with patch("src.cnpj_datalake.services.pyspark.gold.psycopg2.connect") as connect_mock:
            connect_mock.return_value.__enter__.return_value = conn
            gold._write_to_postgres(fake_df, "cnpj_gold.empresas")

        write_builder.option.assert_any_call("dbtable", "cnpj_gold.empresas")
        write_builder.mode.assert_called_once_with("append")
        write_builder.save.assert_called_once()


if __name__ == "__main__":
    unittest.main()