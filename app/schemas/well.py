"""
Well schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime, date


class WellBase(BaseModel):
    """Base well schema"""
    well_number: str = Field(..., description="Номер скважины")
    well_name: Optional[str] = Field(None, description="Название скважины")
    field: Optional[str] = Field(None, description="Площадь/месторождение (лат.)")
    field_name: Optional[str] = Field("Миннибаевское", description="Название месторождения")
    project_code: str = Field(..., description="Код проекта")
    company: str = Field(default="ПАО Татнефть", description="Компания")
    pad_number: Optional[str] = Field(None, description="Номер куста")
    category: Optional[str] = Field(None, description="Категория скважины")
    ngdu: Optional[str] = Field(None, description="НГДУ")
    well_category: Optional[str] = Field(None, description="Подкатегория")
    completion_date: Optional[date] = Field(None, description="Дата завершения скважины")
    metadata_: Optional[Dict[str, Any]] = Field(None, alias="metadata", description="Метаданные")


class WellCreate(WellBase):
    """Schema for creating a well"""
    pass


class WellUpdate(BaseModel):
    """Schema for updating a well"""
    well_name: Optional[str] = None
    field: Optional[str] = None
    field_name: Optional[str] = None
    project_code: Optional[str] = None
    company: Optional[str] = None
    pad_number: Optional[str] = None
    category: Optional[str] = None
    ngdu: Optional[str] = None
    well_category: Optional[str] = None
    completion_date: Optional[date] = None
    storage_path: Optional[str] = None
    file_storage_url: Optional[str] = None
    has_realtime_data: Optional[bool] = None
    has_reports: Optional[bool] = None
    has_drilling_program: Optional[bool] = None
    has_supervision_log: Optional[bool] = None
    documents: Optional[Dict[str, Any]] = None
    metadata_: Optional[Dict[str, Any]] = Field(None, alias="metadata")


class WellboreShort(BaseModel):
    """Short wellbore info for well detail response"""
    wellbore_id: int
    wellbore_number: str
    construction: Optional[str] = None
    casing_diameter_mm: Optional[float] = None
    tubing_diameter_mm: Optional[float] = None
    gdi_data: Optional[str] = None
    injectivity_coefficient: Optional[float] = None
    circulation_character: Optional[str] = None
    circulation_percent: Optional[float] = None

    class Config:
        from_attributes = True


class GtiLogShort(BaseModel):
    """Short GTI log info for well response"""
    log_id: int
    start_time: datetime
    end_time: datetime
    total_records: Optional[int] = None
    quality_status: Optional[str] = None

    class Config:
        from_attributes = True


class WellResponse(BaseModel):
    """Schema for well list response"""
    well_id: int
    well_number: str
    well_name: Optional[str] = None
    field: Optional[str] = None
    field_name: Optional[str] = None
    project_code: str
    company: Optional[str] = None
    pad_number: Optional[str] = None
    category: Optional[str] = None
    ngdu: Optional[str] = None
    well_category: Optional[str] = None
    completion_date: Optional[date] = None
    storage_path: Optional[str] = None
    file_storage_url: Optional[str] = None
    has_realtime_data: bool = False
    has_reports: bool = False
    has_drilling_program: bool = False
    has_supervision_log: bool = False
    created_at: datetime
    wellbores_count: int = 0
    logs_count: int = 0

    class Config:
        from_attributes = True
        populate_by_name = True


class WellDetailResponse(WellResponse):
    """Detailed well response with related data"""
    documents: Optional[Dict[str, Any]] = None
    metadata_: Optional[Dict[str, Any]] = Field(None, alias="metadata")
    wellbores: List[WellboreShort] = []
    gti_logs: List[GtiLogShort] = []

    class Config:
        from_attributes = True
        populate_by_name = True


class WellListResponse(BaseModel):
    """Paginated list of wells"""
    total: int
    limit: int
    offset: int
    items: List[WellResponse]
