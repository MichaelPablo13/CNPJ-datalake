"""PostgreSQL client for pipeline execution metadata."""

from __future__ import annotations

from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from src.cnpj_datalake.config import PostgresConfig


class PostgresClient:
    def __init__(self, config: PostgresConfig):
        self._config = config

    def connect(self):
        return psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.database,
            user=self._config.user,
            password=self._config.password,
            sslmode="prefer",
        )

    def start_pipeline_execution(self, pipeline_name: str, source_file: str, data_version: str | None = None) -> int:
        with self.connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO cnpj_metadata.pipeline_execution
                    (pipeline_name, source_file, data_version, dataset_month, status)
                    VALUES (%s, %s, %s, %s, 'running')
                    RETURNING id
                    """,
                    (pipeline_name, source_file, data_version, data_version),
                )
                row = cur.fetchone()
                return int(row["id"])

    def finish_pipeline_execution(
        self,
        execution_id: int,
        status: str,
        records_processed: int,
        records_failed: int,
        output_path: Optional[str] = None,
    ) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cnpj_metadata.pipeline_execution
                    SET status = %s,
                        end_time = NOW(),
                        duration_seconds = EXTRACT(EPOCH FROM (NOW() - start_time))::INTEGER,
                        records_processed = %s,
                        records_failed = %s,
                        output_path = %s
                    WHERE id = %s
                    """,
                    (status, records_processed, records_failed, output_path, execution_id),
                )
