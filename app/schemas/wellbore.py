"""
Wellbore schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime


class WellboreBase(BaseModel):
    """Base wellbore schema"""
    wellbore_number: str = Field(default="main", description="Номер ствола")
    diameter_mm: Optional[float] = Field(None, description="Диаметр ствола (мм)")
    azimuth_avg: Optional[float] = Field(None, description="Средний азимут")
    inclination_avg: Optional[float] = Field(None, description="Средний зенитный угол")
    construction: Optional[str] = Field(None, description="Конструкция скважины")
    casing_diameter_mm: Optional[float] = Field(None, description="Диаметр обсадной колонны, мм")
    tubing_diameter_mm: Optional[float] = Field(None, description="Диаметр хвостовика, мм")
    gdi_data: Optional[str] = Field(None, description="Данные ГДИ")
    injectivity_coefficient: Optional[float] = Field(None, description="Коэффициент приёмистости")
    circulation_character: Optional[str] = Field(None, description="Характер циркуляции")
    circulation_percent: Optional[float] = Field(None, description="Циркуляция (числовое 0-1)")
    properties: Optional[Dict[str, Any]] = Field(None, description="Дополнительные свойства")


class WellboreCreate(WellboreBase):
    """Schema for creating a wellbore"""
    pass


class WellboreResponse(WellboreBase):
    """Schema for wellbore response"""
    wellbore_id: int
    well_id: int
    created_at: datetime

    class Config:
        from_attributes = True
