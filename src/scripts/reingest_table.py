"""Limpa uma tabela especifica por mes e executa nova ingestao do dataset."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable

from minio import Minio
from minio.deleteobjects import DeleteObject
import psycopg2
from psycopg2 import sql

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cnpj_datalake.config import DataLakeConfig
from src.cnpj_datalake.domain import normalize_file_type
from src.cnpj_datalake.services.pyspark.orchestration import run_pipeline


def _split_table_name(table: str) -> tuple[str, str]:
    if "." in table:
        schema_name, table_name = table.split(".", 1)
        return schema_name, table_name
    return "cnpj_gold", table


def _resolve_table(table: str, file_type: str) -> tuple[str, str]:
    if table:
        return _split_table_name(table)
    normalized = normalize_file_type(file_type)
    return "cnpj_gold", normalized


def _table_version_columns(conn: psycopg2.extensions.connection, schema_name: str, table_name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema_name, table_name),
        )
        return [row[0] for row in cur.fetchall()]


def _delete_postgres_rows(
    config: DataLakeConfig,
    schema_name: str,
    table_name: str,
    data_version: str,
    dry_run: bool,
) -> int:
    with psycopg2.connect(
        host=os.getenv("PG_HOST", config.postgres.host),
        port=int(os.getenv("PG_PORT", str(config.postgres.port))),
        dbname=os.getenv("PG_DATABASE", config.postgres.database),
        user=os.getenv("PG_USER", config.postgres.user),
        password=os.getenv("PG_PASSWORD", config.postgres.password),
        sslmode="prefer",
    ) as conn:
        columns = set(_table_version_columns(conn, schema_name, table_name))
        where_parts: list[str] = []
        params: list[str] = []
        if "dataset_month" in columns:
            where_parts.append("dataset_month = %s")
            params.append(data_version)
        if "data_version" in columns:
            where_parts.append("data_version = %s")
            params.append(data_version)

        if not where_parts:
            raise ValueError(
                f"Tabela {schema_name}.{table_name} nao possui colunas dataset_month/data_version para limpeza segura."
            )

        where_sql = " OR ".join(where_parts)

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE " + where_sql).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                ),
                params,
            )
            to_delete = int(cur.fetchone()[0])

            if dry_run:
                return to_delete

            cur.execute(
                sql.SQL("DELETE FROM {}.{} WHERE " + where_sql).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                ),
                params,
            )
            deleted = cur.rowcount
        conn.commit()
        return int(deleted)


def _delete_minio_prefix(client: Minio, bucket: str, prefix: str, dry_run: bool) -> int:
    objects = [obj.object_name for obj in client.list_objects(bucket, prefix=prefix, recursive=True)]
    if not objects:
        return 0
    if dry_run:
        return len(objects)

    errors = list(client.remove_objects(bucket, [DeleteObject(name) for name in objects]))
    if errors:
        first = errors[0]
        raise RuntimeError(f"Falha ao remover objetos de {bucket}/{prefix}: {first}")
    return len(objects)


def _cleanup_minio(config: DataLakeConfig, file_type: str, data_version: str, dry_run: bool) -> dict[str, int]:
    normalized = normalize_file_type(file_type)
    client = Minio(
        endpoint=config.minio.endpoint,
        access_key=config.minio.access_key,
        secret_key=config.minio.secret_key,
        secure=config.minio.secure,
    )

    targets = {
        config.minio.bucket_bronze: f"{data_version}/{normalized}/",
        config.minio.bucket_silver: f"{data_version}/{normalized}/",
        config.minio.bucket_gold: f"{data_version}/{normalized}",
    }

    removed: dict[str, int] = {}
    for bucket, prefix in targets.items():
        if not client.bucket_exists(bucket):
            removed[bucket] = 0
            continue
        removed[bucket] = _delete_minio_prefix(client, bucket, prefix, dry_run=dry_run)
    return removed


def _parse_primary_keys(raw: str | None) -> Iterable[str] | None:
    if not raw:
        return None
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    return keys or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Limpa dados de uma tabela por mes e reexecuta a ingestao para teste do agente.",
    )
    parser.add_argument("--file-type", required=True, help="Tipo do dataset (empresas, estabelecimentos, socios, etc.).")
    parser.add_argument("--source-file", required=True, help="Arquivo de entrada ou glob para nova ingestao.")
    parser.add_argument("--data-version", required=True, help="Mes alvo no formato YYYY-MM.")
    parser.add_argument(
        "--table",
        default="",
        help="Tabela alvo no Postgres (padrao: cnpj_gold.<file_type_normalizado>).",
    )
    parser.add_argument(
        "--primary-keys",
        default="",
        help="Chaves primarias separadas por virgula para deduplicacao na Silver.",
    )
    parser.add_argument("--skip-minio-clean", action="store_true", help="Nao limpa objetos do MinIO antes da reingestao.")
    parser.add_argument("--skip-reingest", action="store_true", help="Limpa e sai sem executar run_pipeline().")
    parser.add_argument("--dry-run", action="store_true", help="Mostra quantidades que seriam removidas sem alterar dados.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = DataLakeConfig.from_env()

    file_type = normalize_file_type(args.file_type)
    schema_name, table_name = _resolve_table(args.table, file_type)

    deleted_rows = _delete_postgres_rows(
        config=config,
        schema_name=schema_name,
        table_name=table_name,
        data_version=args.data_version,
        dry_run=args.dry_run,
    )
    print(f"[Postgres] {'Removeria' if args.dry_run else 'Removeu'} {deleted_rows} linha(s) de {schema_name}.{table_name}.")

    if args.skip_minio_clean:
        print("[MinIO] Limpeza ignorada por --skip-minio-clean.")
    else:
        removed = _cleanup_minio(config, file_type=file_type, data_version=args.data_version, dry_run=args.dry_run)
        for bucket, count in removed.items():
            action = "Removeria" if args.dry_run else "Removeu"
            print(f"[MinIO] {action} {count} objeto(s) em {bucket}/{args.data_version}/{file_type}.")

    if args.skip_reingest:
        print("[Pipeline] Reingestao ignorada por --skip-reingest.")
        return

    if args.dry_run:
        print("[Pipeline] Dry-run ativo: nenhuma ingestao executada.")
        return

    result = run_pipeline(
        source_file=Path(args.source_file),
        file_type=file_type,
        primary_keys=_parse_primary_keys(args.primary_keys),
        data_version=args.data_version,
    )

    print("[Pipeline] Reingestao concluida com sucesso.")
    print(f"[Pipeline] bronze: {result['bronze_path']}")
    print(f"[Pipeline] silver: {result['silver_path']}")
    print(f"[Pipeline] gold:   {result['gold_path']}")
    print(f"[Pipeline] registros processados: {result['records_processed']}")


if __name__ == "__main__":
    main()
