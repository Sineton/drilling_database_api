"""
Import operation schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


# ============== Excel Import ==============

class ExcelColumnMapping(BaseModel):
    """Column mapping for Excel import"""
    well_number: str = "№ скв"
    bush_number: Optional[str] = "№ куста"
    field: Optional[str] = "Площадь"
    category: Optional[str] = "Категория"
    ngdu: Optional[str] = "НГДУ"
    construction: Optional[str] = "Конструкция скважины"
    completion_date: Optional[str] = "Дата завершения скважины"
    circulation: Optional[str] = "Характер циркуляции, %"
    gdi_data: Optional[str] = "Данные ГДИ на глубине перехода на раствор"
    injectivity: Optional[str] = "Коэффицииент приёмистости"


class ExcelImportRequest(BaseModel):
    """Request for Excel wells import"""
    file_path: Optional[str] = Field(None, description="Путь к файлу на сервере")
    project_code: str = Field(..., description="Код проекта")
    company: str = Field(default="pao-tatneft", description="Компания")
    create_wellbores: bool = Field(default=True, description="Создавать стволы")
    dry_run: bool = Field(default=False, description="Режим проверки")
    column_mapping: Optional[ExcelColumnMapping] = Field(None, description="Маппинг колонок")


class WellImportResult(BaseModel):
    """Single well import result"""
    well_id: int
    well_number: str
    field: Optional[str] = None
    wellbore_id: Optional[int] = None
    status: str = "created"  # created, updated, skipped


class ExcelImportSummary(BaseModel):
    """Summary of Excel import"""
    total_rows: int
    wells_created: int
    wells_updated: int
    wells_skipped: int
    wellbores_created: int
    errors: List[str] = []


class ExcelImportResponse(BaseModel):
    """Response for Excel import"""
    success: bool
    job_id: str
    summary: ExcelImportSummary
    wells: List[WellImportResult] = []


class ExcelColumnInfo(BaseModel):
    """Column information from Excel"""
    name: str
    index: int
    dtype: str
    sample_values: List[Any] = []
    null_count: int = 0
    suggested_mapping: Optional[str] = None


class ExcelParseResponse(BaseModel):
    """Response for Excel parse/preview"""
    success: bool
    file_info: Dict[str, Any]
    columns: List[ExcelColumnInfo]
    auto_detected_mapping: Dict[str, str] = {}


# ============== Events Import ==============

class ComplicationRule(BaseModel):
    """Rule for detecting complications"""
    pattern: str = Field(..., description="Regex pattern")
    event_type: str = Field(..., description="Event type code")
    confidence: float = Field(default=0.9, description="Confidence level")
    extract_value: bool = Field(default=False, description="Extract value from pattern")


class EventsImportRequest(BaseModel):
    """Request for events import from Excel"""
    file_path: Optional[str] = None
    annotation_source: str
    parse_complications: bool = True
    column_mapping: Optional[Dict[str, str]] = None
    complication_rules: Optional[List[ComplicationRule]] = None


class EventImportResult(BaseModel):
    """Single event import result"""
    event_id: int
    well_number: str
    wellbore_id: int
    event_type: str
    notes: Optional[str] = None
    confidence: float = 1.0


class EventsImportResponse(BaseModel):
    """Response for events import"""
    success: bool
    job_id: str
    summary: Dict[str, Any]
    events: List[EventImportResult] = []


# ============== LAS Import ==============

class ChannelMapping(BaseModel):
    """Channel mapping for LAS import"""
    # Standard mappings (LAS mnemonic -> DB column)
    Zab: Optional[str] = "dbtm"
    W: Optional[str] = "wob"
    Hkr: Optional[str] = "bpos"
    M: Optional[str] = "trq"
    W_kr: Optional[str] = Field(default="hkld", alias="W kr")
    P_vkh: Optional[str] = Field(default="spp", alias="P vkh")
    N_rot: Optional[str] = Field(default="rpm", alias="N rot")
    V_sum: Optional[str] = Field(default="tvol", alias="V sum")
    Q_vkh: Optional[str] = Field(default="mfip", alias="Q vkh")
    Q_vyikh: Optional[str] = Field(default="mfop", alias="Q vyikh")
    G_vkh: Optional[str] = Field(default="mwin", alias="G vkh")
    G_vyikh: Optional[str] = Field(default="mwop", alias="G vyikh")
    G_sum: Optional[str] = Field(default="tgas", alias="G sum")
    Gl_dol: Optional[str] = Field(default="dmea", alias="Gl.dol")

    class Config:
        populate_by_name = True


class UnitConversion(BaseModel):
    """Unit conversion settings"""
    from_unit: str = Field(..., alias="from")
    to_unit: str = Field(..., alias="to")
    factor: float = 1.0
    
    class Config:
        populate_by_name = True


class LASImportRequest(BaseModel):
    """Request for LAS file import"""
    file_path: Optional[str] = Field(None, description="Путь к файлу на сервере")
    well_number: Optional[str] = Field(None, description="Номер скважины")
    create_well: bool = Field(default=False, description="Создать скважину, если не существует")
    validate_only: bool = Field(default=False, description="Только валидация")
    channel_mapping: Optional[Dict[str, str]] = Field(None, description="Маппинг каналов")
    unit_conversions: Optional[Dict[str, UnitConversion]] = Field(None, description="Конвертация единиц")
    batch_size: int = Field(default=10000, description="Размер батча для вставки")


class LASFileInfo(BaseModel):
    """LAS file information"""
    filename: str
    las_version: str
    well_name: Optional[str] = None
    field: Optional[str] = None
    company: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    step_sec: Optional[float] = None
    channels_count: int = 0
    records_count: int = 0


class LASMappingResult(BaseModel):
    """Result of well/wellbore mapping"""
    well_id: int
    wellbore_id: int
    log_id: int
    well_number: str


class ImportProgress(BaseModel):
    """Import progress information"""
    total_records: int
    imported_records: int
    percentage: float
    current_batch: Optional[int] = None
    total_batches: Optional[int] = None
    elapsed_sec: Optional[float] = None
    estimated_remaining_sec: Optional[float] = None


class LASImportResponse(BaseModel):
    """Response for LAS import"""
    success: bool
    job_id: str
    status: str  # queued, processing, completed, failed
    file_info: Optional[LASFileInfo] = None
    mapping_result: Optional[LASMappingResult] = None
    import_progress: Optional[ImportProgress] = None
    error: Optional[str] = None


# ============== LAS Batch Import ==============

class LASBatchImportRequest(BaseModel):
    """Request for batch LAS import"""
    folder_path: str = Field(..., description="Путь к папке с LAS файлами")
    recursive: bool = Field(default=True, description="Рекурсивный поиск")
    file_pattern: str = Field(default="*.las", description="Паттерн файлов")
    well_number_from_folder: bool = Field(default=True, description="Номер скважины из имени папки")
    create_wells: bool = Field(default=False, description="Создавать скважины")
    parallel_jobs: int = Field(default=4, description="Параллельных задач")
    channel_mapping: Optional[Dict[str, str]] = None


class BatchFileStatus(BaseModel):
    """Status of single file in batch"""
    file_path: str
    well_number: Optional[str] = None
    status: str  # queued, processing, completed, failed
    job_id: Optional[str] = None
    records: Optional[int] = None
    error: Optional[str] = None


class BatchSummary(BaseModel):
    """Summary of batch import"""
    total_files: int
    queued: int
    processing: int
    completed: int
    failed: int


class LASBatchImportResponse(BaseModel):
    """Response for batch LAS import"""
    success: bool
    batch_id: str
    status: str
    summary: BatchSummary
    files: List[BatchFileStatus] = []
    status_url: str


# ============== LAS Parse ==============

class LASCurveInfo(BaseModel):
    """LAS curve information"""
    mnemonic: str
    unit: str
    description: str
    suggested_mapping: Optional[str] = None
    sample_values: List[Any] = []
    min_value: Optional[float] = Field(None, alias="min")
    max_value: Optional[float] = Field(None, alias="max")
    null_count: int = 0
    
    class Config:
        populate_by_name = True


class LASStatistics(BaseModel):
    """LAS file statistics"""
    total_records: int
    time_range_hours: Optional[float] = None
    sampling_rate_sec: Optional[float] = None
    null_percentage: Dict[str, float] = {}


class ExistingWell(BaseModel):
    """Existing well info"""
    found: bool
    well_id: Optional[int] = None
    well_number: Optional[str] = None
    wellbore_id: Optional[int] = None


class LASParseResponse(BaseModel):
    """Response for LAS parse/preview"""
    success: bool
    las_version: str
    well_info: Dict[str, Any]
    curves: List[LASCurveInfo]
    statistics: LASStatistics
    auto_mapping: Dict[str, str]
    existing_well: ExistingWell


# ============== Job Status ==============

class ImportJobStatus(BaseModel):
    """Import job status"""
    job_id: str
    status: str  # queued, processing, completed, failed
    progress: Optional[ImportProgress] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
