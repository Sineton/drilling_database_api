"""
Event schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date


class EventTypeBase(BaseModel):
    """Base event type schema"""
    event_code: str
    event_name: str
    is_complication: bool = True
    is_precursor: bool = False
    severity: int = 1
    target_label: Optional[int] = None
    description: Optional[str] = None


class EventTypeResponse(EventTypeBase):
    """Event type response"""
    event_type_id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


class EventBase(BaseModel):
    """Base event schema"""
    wellbore_id: int = Field(..., description="ID ствола скважины")
    event_type_id: int = Field(..., description="ID типа события")
    start_time: Optional[datetime] = Field(None, description="Время начала")
    end_time: Optional[datetime] = Field(None, description="Время окончания")
    start_md: Optional[float] = Field(None, description="Начальная глубина MD")
    end_md: Optional[float] = Field(None, description="Конечная глубина MD")
    annotation_source: str = Field(..., description="Источник разметки")
    annotator_name: Optional[str] = Field(None, description="Имя оператора разметки")
    confidence: float = Field(default=1.0, description="Уверенность (0-1)")
    notes: Optional[str] = Field(None, description="Примечания")


class EventCreate(EventBase):
    """Schema for creating an event"""
    pass


class EventResponse(EventBase):
    """Schema for event response"""
    event_id: int
    created_at: datetime
    event_type_code: Optional[str] = None
    event_type_name: Optional[str] = None
    
    class Config:
        from_attributes = True


class SvEventsSyncRequest(BaseModel):
    """Request for syncing events from supervisor operations."""
    well_number: Optional[str] = Field(default=None, description="Фильтр по номеру скважины")
    date_from: Optional[date] = Field(default=None, description="Дата начала (включительно)")
    date_to: Optional[date] = Field(default=None, description="Дата конца (включительно)")
    min_severity: int = Field(default=1, ge=1, le=3, description="Минимальная критичность anomaly_severity")
    dry_run: bool = Field(default=True, description="Только показать что будет создано")
    max_operations: int = Field(default=5000, ge=1, le=50000, description="Ограничение обрабатываемых операций")
    include_npv_balance: bool = Field(default=True, description="Добавлять события также из sv_npv_balance")


class SvEventsFillByWellRequest(BaseModel):
    """Request for filling events by a specific well."""
    date_from: Optional[date] = Field(default=None, description="Дата начала (включительно)")
    date_to: Optional[date] = Field(default=None, description="Дата конца (включительно)")
    min_severity: int = Field(default=1, ge=1, le=3, description="Минимальная критичность anomaly_severity")
    dry_run: bool = Field(default=False, description="Только показать что будет создано")
    max_operations: int = Field(default=5000, ge=1, le=50000, description="Ограничение обрабатываемых операций")
    include_npv_balance: bool = Field(default=True, description="Добавлять события также из sv_npv_balance")


class SvEventsCleanupRequest(BaseModel):
    """Request for cleanup auto-generated events by well."""
    date_from: Optional[date] = Field(default=None, description="Дата начала (включительно)")
    date_to: Optional[date] = Field(default=None, description="Дата конца (включительно)")
    dry_run: bool = Field(default=True, description="Только показать, какие события будут удалены")
    include_npv_balance: bool = Field(default=True, description="Удалять также source=supervisor_journal_npv")


class SvEventsCleanupResponse(BaseModel):
    """Response for cleanup auto-generated events."""
    dry_run: bool
    well_number: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    include_npv_balance: bool
    sources: List[str]
    found_events: int
    deleted_events: int
    by_source: Dict[str, int]
    preview: List[Dict[str, Any]] = []


class SvEventsRebuildRequest(BaseModel):
    """Request for cleanup + fill by well."""
    date_from: Optional[date] = Field(default=None, description="Дата начала (включительно)")
    date_to: Optional[date] = Field(default=None, description="Дата конца (включительно)")
    min_severity: int = Field(default=1, ge=1, le=3, description="Минимальная критичность anomaly_severity")
    dry_run: bool = Field(default=False, description="Dry-run для обоих шагов: cleanup и fill")
    max_operations: int = Field(default=5000, ge=1, le=50000, description="Ограничение обрабатываемых операций")
    include_npv_balance: bool = Field(default=True, description="Работать также с sv_npv_balance")


class SvEventsRebuildResponse(BaseModel):
    """Response for cleanup + fill pipeline."""
    well_number: str
    dry_run: bool
    cleanup: SvEventsCleanupResponse
    fill: "SvEventsSyncResponse"


class SvEventsDiagnoseRequest(BaseModel):
    """Request for diagnostics before fill/rebuild."""
    date_from: Optional[date] = Field(default=None, description="Дата начала (включительно)")
    date_to: Optional[date] = Field(default=None, description="Дата конца (включительно)")
    min_severity: int = Field(default=1, ge=1, le=3, description="Минимальная критичность anomaly_severity")
    max_operations: int = Field(default=5000, ge=1, le=50000, description="Ограничение обрабатываемых операций")
    include_npv_balance: bool = Field(default=True, description="Учитывать sv_npv_balance")


class SvEventsDiagnoseResponse(BaseModel):
    """Diagnostics response for event sync pipeline."""
    well_number: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    min_severity: int
    include_npv_balance: bool
    operations: Dict[str, Any]
    npv: Dict[str, Any]
    totals: Dict[str, int]


class SvEventsSyncResponse(BaseModel):
    """Response for supervisor -> events sync."""
    dry_run: bool
    well_number: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    min_severity: int
    scanned_operations: int
    candidate_operations: int
    created_event_types: int
    created_events: int
    skipped_existing: int
    include_npv_balance: bool = True
    scanned_npv_items: int = 0
    candidate_npv_items: int = 0
    created_npv_events: int = 0
    skipped_existing_npv: int = 0
    preview: List[Dict[str, Any]] = []
