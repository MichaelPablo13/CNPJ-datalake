"""Reset Postgres schemas and MinIO buckets for a clean reingestion."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from minio import Minio
import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cnpj_datalake.config import DataLakeConfig


SCHEMAS_TO_RESET = ("cnpj_bronze", "cnpj_silver", "cnpj_gold", "cnpj_metadata")


def reset_postgres(config: DataLakeConfig) -> None:
    schema_file = config.project_root / "services" / "postgres" / "schemas.sql"
    reset_sql = "; ".join(f"DROP SCHEMA IF EXISTS {schema} CASCADE" for schema in SCHEMAS_TO_RESET) + ";"

    with psycopg2.connect(
        host=os.getenv("PG_HOST", config.postgres.host),
        port=int(os.getenv("PG_PORT", str(config.postgres.port))),
        dbname=os.getenv("PG_DATABASE", config.postgres.database),
        user=os.getenv("PG_SUPERUSER", "postgres"),
        password=os.getenv("PG_SUPERUSER_PASSWORD", "postgres"),
        sslmode="prefer",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(reset_sql)
            cur.execute(schema_file.read_text(encoding="utf-8"))
        conn.commit()


def reset_minio(config: DataLakeConfig) -> None:
    client = Minio(
        endpoint=config.minio.endpoint,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=config.minio.secure,
    )

    for bucket in (
        config.minio.bucket_bronze,
        config.minio.bucket_silver,
        config.minio.bucket_gold,
    ):
        if client.bucket_exists(bucket):
            objects = [obj.object_name for obj in client.list_objects(bucket, recursive=True)]
            if objects:
                for object_name in objects:
                    client.remove_object(bucket, object_name)
        else:
            client.make_bucket(bucket)


def main() -> None:
    config = DataLakeConfig.from_env()
    reset_postgres(config)
    reset_minio(config)
    print("Postgres schemas e buckets MinIO reinicializados com sucesso.")


if __name__ == "__main__":
    main()