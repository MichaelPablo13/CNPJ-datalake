
import os
import unittest
from unittest import mock

from src.cnpj_datalake.config import DataLakeConfig


class ConfigTests(unittest.TestCase):
    def test_config_from_env(self):
        env = {
            "PG_HOST": "db",
            "PG_PORT": "5433",
            "PG_USER": "app",
            "PG_PASSWORD": "secret",
            "PG_DATABASE": "lake",
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_ACCESS_KEY": "access",
            "MINIO_SECRET_KEY": "secret",
            "MINIO_SECURE": "false",
            "INGESTION_DATA_MONTH": "2026-03",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            config = DataLakeConfig.from_env()

        self.assertEqual(config.postgres.host, "db")
        self.assertEqual(config.postgres.port, 5433)
        self.assertEqual(config.postgres.user, "app")
        self.assertEqual(config.minio.endpoint, "minio:9000")
        self.assertFalse(config.minio.secure)
        self.assertEqual(config.data_version, "2026-03")


if __name__ == "__main__":
    unittest.main()
