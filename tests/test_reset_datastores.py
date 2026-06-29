import os
import unittest
from pathlib import Path
from unittest import mock

from src.cnpj_datalake.config import DataLakeConfig, MinioConfig, PostgresConfig, SparkConfig
from src.scripts.reset_datastores import reset_minio, reset_postgres


def _build_config() -> DataLakeConfig:
    return DataLakeConfig(
        project_root=Path("d:/Projetos/CNPJ-DataLake"),
        data_version="2026-03",
        batch_size=100000,
        quality_threshold=90.0,
        postgres=PostgresConfig(host="localhost", port=5432, database="cnpj_datalake", user="datalake_app", password="datalake"),
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


class ResetDatastoresTests(unittest.TestCase):
    def test_reset_postgres_uses_superuser_credentials(self):
        config = _build_config()
        conn = mock.MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        with mock.patch("src.scripts.reset_datastores.psycopg2.connect") as connect, \
            mock.patch.dict(os.environ, {"PG_SUPERUSER": "postgres", "PG_SUPERUSER_PASSWORD": "postgres"}, clear=False):
            connect.return_value.__enter__.return_value = conn
            reset_postgres(config)

        connect.assert_called_once()
        executed_sql = " ".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("DROP SCHEMA IF EXISTS cnpj_bronze CASCADE", executed_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS cnpj_metadata.pipeline_execution", executed_sql)

    def test_reset_minio_removes_objects_from_all_buckets(self):
        config = _build_config()
        obj = mock.Mock(object_name="2026-03/file.parquet")
        client = mock.MagicMock()
        client.bucket_exists.return_value = True
        client.list_objects.return_value = [obj]

        with mock.patch("src.scripts.reset_datastores.Minio", return_value=client):
            reset_minio(config)

        self.assertEqual(client.remove_object.call_count, 3)


if __name__ == "__main__":
    unittest.main()