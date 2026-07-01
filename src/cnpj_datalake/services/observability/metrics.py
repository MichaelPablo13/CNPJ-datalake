"""Prometheus metrics helpers for batch pipeline observability."""

from __future__ import annotations

import os
import socket
import time
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, push_to_gateway

from src.cnpj_datalake.config import DataLakeConfig
from src.cnpj_datalake.utils.logger import get_logger


logger = get_logger(__name__)

_REGISTRY = CollectorRegistry()

_STAGE_RUNS = Counter(
    "cnpj_pipeline_stage_runs_total",
    "Total stage executions by status.",
    labelnames=("stage", "file_type", "status", "data_version"),
    registry=_REGISTRY,
)

_STAGE_DURATION = Histogram(
    "cnpj_pipeline_stage_duration_seconds",
    "Stage execution time in seconds.",
    labelnames=("stage", "file_type"),
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600),
    registry=_REGISTRY,
)

_STAGE_RECORDS = Counter(
    "cnpj_pipeline_records_total",
    "Total records processed by stage.",
    labelnames=("stage", "file_type"),
    registry=_REGISTRY,
)

_LAST_STAGE_RUN_TS = Gauge(
    "cnpj_pipeline_last_stage_run_timestamp_seconds",
    "Unix timestamp of the latest stage execution.",
    labelnames=("stage", "file_type", "status"),
    registry=_REGISTRY,
)

_ENCODING_FALLBACK_CORRECTED_ROWS = Counter(
    "cnpj_pipeline_encoding_fallback_corrected_rows_total",
    "Rows recovered from replacement characters by automatic encoding fallback.",
    labelnames=("file_type", "from_encoding", "to_encoding", "data_version"),
    registry=_REGISTRY,
)

_ENCODING_FALLBACK_EVENTS = Counter(
    "cnpj_pipeline_encoding_fallback_events_total",
    "Number of automatic encoding fallback switches performed.",
    labelnames=("file_type", "from_encoding", "to_encoding", "data_version"),
    registry=_REGISTRY,
)


def observe_encoding_fallback(
    *,
    file_type: str,
    from_encoding: str,
    to_encoding: str,
    data_version: str,
    replaced_rows_before: int,
    replaced_rows_after: int,
) -> None:
    """Record encoding fallback impact for observability dashboards."""

    normalized_from = from_encoding.strip().lower()
    normalized_to = to_encoding.strip().lower()
    _ENCODING_FALLBACK_EVENTS.labels(file_type, normalized_from, normalized_to, data_version).inc()

    corrected_rows = max(int(replaced_rows_before) - int(replaced_rows_after), 0)
    if corrected_rows > 0:
        _ENCODING_FALLBACK_CORRECTED_ROWS.labels(
            file_type,
            normalized_from,
            normalized_to,
            data_version,
        ).inc(corrected_rows)


class PipelineMetrics:
    """Collect and optionally push metrics for batch jobs."""

    def __init__(self, config: DataLakeConfig):
        self.enabled = config.metrics_enabled
        self.pushgateway_url = config.prometheus_pushgateway_url
        self.job_name = config.prometheus_job_name

    def observe_stage(
        self,
        stage: str,
        file_type: str,
        status: str,
        duration_seconds: float,
        records: int = 0,
        data_version: str = "",
    ) -> None:
        if not self.enabled:
            return

        duration_seconds = max(duration_seconds, 0.0)
        _STAGE_RUNS.labels(stage, file_type, status, data_version).inc()
        _STAGE_DURATION.labels(stage, file_type).observe(duration_seconds)
        _LAST_STAGE_RUN_TS.labels(stage, file_type, status).set(time.time())

        if records > 0:
            _STAGE_RECORDS.labels(stage, file_type).inc(records)

        self._push_best_effort()

    def _push_best_effort(self) -> None:
        if not self.pushgateway_url:
            return

        grouping_key: dict[str, Any] = {
            "instance": socket.gethostname(),
            "pid": str(os.getpid()),
        }

        dag_id = os.getenv("AIRFLOW_CTX_DAG_ID", "").strip()
        run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "").strip()
        if dag_id:
            grouping_key["dag_id"] = dag_id
        if run_id:
            grouping_key["dag_run_id"] = run_id

        try:
            push_to_gateway(
                gateway=self.pushgateway_url,
                job=self.job_name,
                registry=_REGISTRY,
                grouping_key=grouping_key,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Falha ao enviar metricas para Pushgateway: %s", exc)
