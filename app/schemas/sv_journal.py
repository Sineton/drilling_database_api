"""
Pydantic schemas for supervisor journal import
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import date, datetime


# ============== Import Request/Response ==============

class SvJournalImportRequest(BaseModel):
    """Запрос на импорт журнала супервайзера"""
    file_path: Optional[str] = Field(None, description="Путь к файлу на сервере")
    project_code: str = Field(default="pao-tatneft", description="Код проекта")
    well_number: Optional[str] = Field(None, description="Номер скважины (если не парсится из файла)")
    dry_run: bool = Field(default=False, description="Режим проверки без записи в БД")


class SvJournalParseResponse(BaseModel):
    """Результат предварительного разбора журнала"""
    success: bool
    file_info: Dict[str, Any]
    well_info: Dict[str, Any]
    daily_blocks_count: int
    date_range: Optional[Dict[str, str]] = None
    npv_count: int = 0
    warnings: List[str] = []


class DailyReportSummary(BaseModel):
    """Краткая информация по ежедневному отчёту"""
    report_date: str
    construction_stage: Optional[str] = None
    current_depth_m: Optional[float] = None
    penetration_m: Optional[float] = None
    operations_count: int = 0
    bha_count: int = 0
    npv_found: bool = False


class SvJournalImportSummary(BaseModel):
    """Сводка по импорту журнала"""
    well_id: int
    well_number: str
    wellbore_id: int
    daily_reports_created: int = 0
    operations_created: int = 0
    bha_runs_created: int = 0
    drilling_regimes_created: int = 0
    mud_accounting_created: int = 0
    chemical_reagents_created: int = 0
    npv_records_created: int = 0
    contractors_created: int = 0
    construction_items_created: int = 0
    equipment_created: int = 0
    timing_records_created: int = 0
    mud_properties_created: int = 0
    warnings: List[str] = []
    errors: List[str] = []


class SvJournalImportResponse(BaseModel):
    """Ответ импорта журнала"""
    success: bool
    job_id: str
    summary: SvJournalImportSummary
    daily_reports: List[DailyReportSummary] = []


# ============== Detail Schemas ==============

class SvDailyReportDetail(BaseModel):
    """Детали ежедневного отчёта"""
    report_id: int
    report_date: date
    construction_stage: Optional[str] = None
    interval_from_m: Optional[float] = None
    interval_to_m: Optional[float] = None
    current_depth_m: Optional[float] = None
    penetration_m: Optional[float] = None
    drilling_time_h: Optional[float] = None
    rop_plan: Optional[float] = None
    rop_fact: Optional[float] = None
    drilling_comment: Optional[str] = None

    class Config:
        from_attributes = True


class SvDailyOperationDetail(BaseModel):
    """Детали операции"""
    operation_id: int
    sequence_number: int
    time_from: Optional[str] = None
    time_to: Optional[str] = None
    duration_text: Optional[str] = None
    duration_minutes: Optional[int] = None
    description: str
    operation_category: Optional[str] = None
    is_npv: bool = False
    is_complication: bool = False
    anomaly_severity: int = 0

    class Config:
        from_attributes = True


class SvNpvBalanceDetail(BaseModel):
    """Детали записи НПВ"""
    npv_id: int
    incident_date: date
    description: str
    duration_hours: Optional[float] = None
    responsible_party: Optional[str] = None
    category: str
    operation_type: Optional[str] = None

    class Config:
        from_attributes = True


class SvJournalOverview(BaseModel):
    """Обзор загруженного журнала по скважине"""
    well_id: int
    well_number: str
    wellbore_id: int
    total_reports: int
    date_range: Optional[Dict[str, str]] = None
    total_operations: int
    total_npv: int
    total_bha: int
    construction_stages: List[str] = []
    max_depth_m: Optional[float] = None
