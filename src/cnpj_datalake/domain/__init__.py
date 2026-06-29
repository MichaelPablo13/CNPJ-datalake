"""Artefatos de domínio do CNPJ Data Lake."""

from src.cnpj_datalake.domain.layouts import CNPJ_LAYOUTS, TYPE_ALIASES, get_layout_columns, normalize_file_type
from src.cnpj_datalake.domain.models import (
    AlertSeverity,
    FileMetadata,
    FileType,
    PipelineExecutionResult,
    ProcessingLog,
    ProcessingMetrics,
    ProcessingStatus,
    QualityAlert,
)

__all__ = [
    "CNPJ_LAYOUTS",
    "TYPE_ALIASES",
    "normalize_file_type",
    "get_layout_columns",
    "FileType",
    "ProcessingStatus",
    "AlertSeverity",
    "ProcessingMetrics",
    "QualityAlert",
    "FileMetadata",
    "ProcessingLog",
    "PipelineExecutionResult",
]