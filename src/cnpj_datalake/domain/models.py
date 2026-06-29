"""Modelos de dados para as camadas do Data Lake."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class FileType(Enum):
    ESTABELECIMENTOS = "estabelecimentos"
    EMPRESAS = "empresas"
    SOCIOS = "socios"
    SIMPLES = "simples"
    CNAES = "cnaes"
    MUNICIPIOS = "municipios"
    PAISES = "paises"
    NATUREZAS = "naturezas"
    QUALIFICACOES = "qualificacoes"


class ProcessingStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class AlertSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ProcessingMetrics:
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    quality_score: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.processed_records / self.total_records) * 100

    @property
    def error_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.failed_records / self.total_records) * 100


@dataclass
class QualityAlert:
    alert_type: str
    alert_message: str
    severity: AlertSeverity = AlertSeverity.WARNING
    record_line: Optional[int] = None
    field_name: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class FileMetadata:
    file_name: str
    file_type: FileType
    source_path: str
    size_bytes: int
    record_count: int
    processed_date: datetime = field(default_factory=datetime.now)
    version: str = "2026-03"
    hash_value: Optional[str] = None
    bucket_location: Optional[str] = None
    parquet_path: Optional[str] = None


@dataclass
class ProcessingLog:
    file_name: str
    file_type: FileType
    source_path: str
    total_records: int
    status: ProcessingStatus = ProcessingStatus.PENDING
    metrics: ProcessingMetrics = field(default_factory=ProcessingMetrics)
    alerts: List[QualityAlert] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    @property
    def execution_time_seconds(self) -> Optional[int]:
        if self.completed_at is None:
            return None
        delta = self.completed_at - self.started_at
        return int(delta.total_seconds())

    def add_alert(self, alert: QualityAlert) -> None:
        self.alerts.append(alert)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_name": self.file_name,
            "file_type": self.file_type.value,
            "source_path": self.source_path,
            "total_records": self.total_records,
            "processed_records": self.metrics.processed_records,
            "failed_records": self.metrics.failed_records,
            "quality_score": self.metrics.quality_score,
            "status": self.status.value,
            "success_rate": self.metrics.success_rate,
            "error_rate": self.metrics.error_rate,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "execution_time_seconds": self.execution_time_seconds,
            "alert_count": len(self.alerts),
            "critical_alerts": sum(1 for alert in self.alerts if alert.severity == AlertSeverity.CRITICAL),
        }


@dataclass
class PipelineExecutionResult:
    pipeline_name: str
    status: ProcessingStatus
    source_file: str
    records_processed: int
    records_failed: int
    output_path: str
    duration_seconds: int
    logs: List[ProcessingLog] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        total = self.records_processed + self.records_failed
        if total == 0:
            return 0.0
        return (self.records_processed / total) * 100