"""
Schemas for analytics and datasets endpoints.
"""
from datetime import datetime
from typing import Optional, List, Dict

from pydantic import BaseModel, Field


class AnomalyItem(BaseModel):
    time_utc: datetime
    well_number: str
    depth_md: Optional[float] = None
    operation: Optional[str] = None
    torque: Optional[float] = None
    hookload: Optional[float] = None
    spp: Optional[float] = None
    flow_in: Optional[float] = None
    flow_out: Optional[float] = None
    gas: Optional[float] = None
    event_code: Optional[str] = None
    event_name: Optional[str] = None
    anomaly_score: int
    anomaly_reasons: List[str] = Field(default_factory=list)


class AnomaliesResponse(BaseModel):
    total: int
    items: List[AnomalyItem]


class FieldSummaryResponse(BaseModel):
    field: str
    wells_count: int
    wellbores_count: int
    logs_count: int
    snapshots_count: int
    events_count: int
    first_timestamp: Optional[datetime] = None
    last_timestamp: Optional[datetime] = None
    channel_fill_rates: Dict[str, float] = Field(default_factory=dict)


class DatasetBuildRequest(BaseModel):
    field: Optional[str] = None
    well_numbers: Optional[List[str]] = None
    before_minutes: int = Field(default=60, ge=5, le=360)
    after_minutes: int = Field(default=30, ge=0, le=180)
    include_negative: bool = True
    negatives_per_positive: int = Field(default=1, ge=0, le=5)
    max_samples: int = Field(default=500, ge=1, le=5000)


class DatasetSample(BaseModel):
    well_number: str
    wellbore_id: int
    event_id: Optional[int] = None
    target_label: int
    window_start: datetime
    window_end: datetime
    operation_id_mode: Optional[int] = None
    diameter_mm: Optional[float] = None
    azimuth_avg: Optional[float] = None
    inclination_avg: Optional[float] = None
    f_torque_mean: Optional[float] = None
    f_torque_std: Optional[float] = None
    f_hookload_mean: Optional[float] = None
    f_spp_mean: Optional[float] = None
    f_flow_imbalance_mean: Optional[float] = None
    f_gas_mean: Optional[float] = None
    f_depth_mean: Optional[float] = None
    points_count: int


class DatasetBuildResponse(BaseModel):
    total_samples: int
    positives: int
    negatives: int
    samples: List[DatasetSample]
