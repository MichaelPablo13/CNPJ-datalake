"""DAGs compartimentalizadas por tipo de arquivo CNPJ."""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.cnpj_datalake.services.pyspark.orchestration import run_bronze_stage, run_gold_stage, run_silver_stage


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _resolve_schedule() -> str | None:
    raw = os.getenv("AIRFLOW_PIPELINE_SCHEDULE", "@daily").strip()
    if raw.lower() in {"none", "manual", "off", "disabled"}:
        return None
    return raw


def _resolve_data_version_from_context() -> str:
    """Resolve data_version with manual override support.

    Priority:
    1) dag_run.conf.dataset_month / dag_run.conf.data_version (manual trigger payload)
    2) INGESTION_DATA_MONTH / INGESTION_DATA_VERSION / AIRFLOW_DATA_VERSION_OVERRIDE / DATA_VERSION (env override)
    3) logical_date (default Airflow behavior)
    """
    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    conf_version = str(conf.get("dataset_month") or conf.get("data_version") or "").strip()
    if conf_version:
        return conf_version

    env_version = (
        os.getenv("INGESTION_DATA_MONTH", "").strip()
        or os.getenv("INGESTION_DATA_VERSION", "").strip()
        or os.getenv("AIRFLOW_DATA_VERSION_OVERRIDE", "").strip()
        or os.getenv("DATA_VERSION", "").strip()
    )
    if env_version:
        return env_version

    return ctx["logical_date"].strftime("%Y-%m")


def _resolve_source_file(source_env: str, default_source: str) -> str:
    source_value = os.getenv(source_env, default_source).strip()
    source_candidates = [part.strip() for part in source_value.split("|") if part.strip()]

    for source_file in source_candidates:
        if _expand_source_paths(source_file):
            return source_file

    raise FileNotFoundError(
        "Nenhum arquivo valido encontrado para as fontes configuradas: "
        f"{source_candidates}. Verifique data/input e descompacte o zip antes da execucao."
    )


def _expand_source_paths(source_file: str) -> list[Path]:
    source_path = Path(source_file)

    if "*" in source_file or "?" in source_file or "[" in source_file:
        matches = sorted(source_path.parent.glob(source_path.name))
    elif source_path.exists():
        matches = [source_path]
    else:
        matches = []

    return [
        p
        for p in matches
        if p.is_file() and not p.name.startswith(".") and p.suffix.lower() != ".zip"
    ]


def _archive_consumed_files(source_file: str, file_type: str, data_version: str) -> list[str]:
    matches = _expand_source_paths(source_file)
    if not matches:
        raise FileNotFoundError(f"Nenhum arquivo para arquivar na fonte: {source_file}")

    archive_root = Path("/opt/airflow/data/consumed") / file_type / data_version
    archive_root.mkdir(parents=True, exist_ok=True)

    moved: list[str] = []
    for src in matches:
        target = archive_root / src.name
        if target.exists():
            stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            target = archive_root / f"{src.stem}_{stamp}{src.suffix}"
        shutil.move(str(src), str(target))
        moved.append(str(target))

    return moved


DATASETS = [
    # ── Tabelas principais (com agregações Gold) ────────────────────────────
    {
        "dag_id": "cnpj_empresas_pipeline",
        "file_type": "empresas",
        "source_env": "AIRFLOW_SOURCE_FILE_EMPRESAS",
        "default_source": "/opt/airflow/data/input/empresas/Empresas*.txt",
        "run_gold": True,
    },
    {
        "dag_id": "cnpj_estabelecimentos_pipeline",
        "file_type": "estabelecimentos",
        "source_env": "AIRFLOW_SOURCE_FILE_ESTABELECIMENTOS",
        "default_source": "/opt/airflow/data/input/estabelecimentos/Estabelecimentos*.txt",
        "run_gold": True,
    },
    {
        "dag_id": "cnpj_socios_pipeline",
        "file_type": "socios",
        "source_env": "AIRFLOW_SOURCE_FILE_SOCIOS",
        "default_source": "/opt/airflow/data/input/socios/Socios*.txt",
        "run_gold": True,
    },
    # ── Tabelas de referência (passthrough Gold — sem agregação) ────────────
    {
        "dag_id": "cnpj_cnaes_pipeline",
        "file_type": "cnaes",
        "source_env": "AIRFLOW_SOURCE_FILE_CNAES",
        "default_source": "/opt/airflow/data/input/cnaes/Cnaes.txt",
        "run_gold": True,
    },
    {
        "dag_id": "cnpj_motivos_pipeline",
        "file_type": "motivos",
        "source_env": "AIRFLOW_SOURCE_FILE_MOTIVOS",
        "default_source": "/opt/airflow/data/input/motivos/Motivos.txt",
        "run_gold": True,
    },
    {
        "dag_id": "cnpj_municipios_pipeline",
        "file_type": "municipios",
        "source_env": "AIRFLOW_SOURCE_FILE_MUNICIPIOS",
        "default_source": "/opt/airflow/data/input/municipios/Municipios.txt",
        "run_gold": True,
    },
    {
        "dag_id": "cnpj_naturezas_pipeline",
        "file_type": "naturezas",
        "source_env": "AIRFLOW_SOURCE_FILE_NATUREZAS",
        "default_source": "/opt/airflow/data/input/naturezas/Naturezas.txt",
        "run_gold": True,
    },
    {
        "dag_id": "cnpj_paises_pipeline",
        "file_type": "paises",
        "source_env": "AIRFLOW_SOURCE_FILE_PAISES",
        "default_source": "/opt/airflow/data/input/paises/Paises.txt",
        "run_gold": True,
    },
    {
        "dag_id": "cnpj_qualificacoes_pipeline",
        "file_type": "qualificacoes",
        "source_env": "AIRFLOW_SOURCE_FILE_QUALIFICACOES",
        "default_source": "/opt/airflow/data/input/qualificacoes/Qualificacoes.txt",
        "run_gold": True,
    },
]


def build_dataset_dag(dag_id: str, file_type: str, source_env: str, default_source: str, run_gold: bool):
    @dag(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        description=f"Pipeline CNPJ para {file_type}",
        start_date=datetime(2026, 1, 1),
        schedule=_resolve_schedule(),
        catchup=False,
        max_active_runs=1,
        tags=["cnpj", "datalake", file_type],
    )
    def _dataset_pipeline():
        @task
        def bronze_task() -> str:
            data_version = _resolve_data_version_from_context()
            source_file = _resolve_source_file(source_env=source_env, default_source=default_source)
            return run_bronze_stage(source_file=source_file, file_type=file_type, data_version=data_version)

        @task
        def archive_task() -> list[str]:
            data_version = _resolve_data_version_from_context()
            source_file = _resolve_source_file(source_env=source_env, default_source=default_source)
            return _archive_consumed_files(source_file=source_file, file_type=file_type, data_version=data_version)

        @task
        def silver_task(bronze_path: str) -> str:
            data_version = _resolve_data_version_from_context()
            return run_silver_stage(bronze_path=bronze_path, file_type=file_type, data_version=data_version)

        bronze_out = bronze_task()
        silver_out = silver_task(bronze_out)

        if run_gold:
            @task
            def gold_task(silver_path: str) -> str:
                data_version = _resolve_data_version_from_context()
                return run_gold_stage(silver_path=silver_path, file_type=file_type, data_version=data_version)

            gold_out = gold_task(silver_out)
            gold_out >> archive_task()
        else:
            silver_out >> archive_task()

    return _dataset_pipeline()


for _dataset in DATASETS:
    globals()[_dataset["dag_id"]] = build_dataset_dag(**_dataset)