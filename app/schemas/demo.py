"""
Schemas for demo overview and well parameters endpoints.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class DemoOperationContext(BaseModel):
    operation_code: Optional[str] = None
    operation_name: Optional[str] = None
    source: Optional[str] = None
    description: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    depth_from_m: Optional[float] = None
    depth_to_m: Optional[float] = None


class DemoWarningContext(BaseModel):
    event_id: Optional[int] = None
    event_code: Optional[str] = None
    event_name: Optional[str] = None
    severity: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    start_md: Optional[float] = None
    end_md: Optional[float] = None


class DemoGeologyContext(BaseModel):
    top_md: Optional[float] = None
    base_md: Optional[float] = None
    formation_name: Optional[str] = None
    lithology: Optional[str] = None
    kg: Optional[float] = None


class WellOverviewItem(BaseModel):
    well_id: int
    well_number: str
    well_name: Optional[str] = None
    field_name: Optional[str] = None
    pad_number: Optional[str] = None
    wellbore_id: int
    wellbore_number: str
    snapshot_time: Optional[datetime] = None
    depth_md: Optional[float] = None
    tvd: Optional[float] = None
    rop: Optional[float] = None
    wob: Optional[float] = None
    rpm: Optional[float] = None
    torque: Optional[float] = None
    spp: Optional[float] = None
    flow_in: Optional[float] = None
    flow_out: Optional[float] = None
    gas: Optional[float] = None
    hookload: Optional[float] = None
    operation: Optional[DemoOperationContext] = None
    warning: Optional[DemoWarningContext] = None
    geology: Optional[DemoGeologyContext] = None


class WellsOverviewResponse(BaseModel):
    target_time: datetime
    window_start: datetime
    window_end: datetime
    total: int
    items: List[WellOverviewItem]


class WellParametersResponse(BaseModel):
    target_time: datetime
    window_start: datetime
    window_end: datetime
    bucket: str
    requested_params: List[str] = Field(default_factory=list)
    points_count: int
    well: Dict[str, Any]
    latest: Dict[str, Any] = Field(default_factory=dict)
    operation: Optional[DemoOperationContext] = None
    warning: Optional[DemoWarningContext] = None
    geology: Optional[DemoGeologyContext] = None
    stats: Dict[str, Dict[str, Optional[float]]] = Field(default_factory=dict)
    points: List[Dict[str, Any]] = Field(default_factory=list)
