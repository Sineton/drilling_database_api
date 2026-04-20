"""
Schemas for markup xlsx import.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MarkupSampleRow(BaseModel):
    """Preview row from markup workbook."""

    row_number: int
    code: Optional[str] = None
    operation_label: Optional[str] = None
    risk_level_id: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    top_md: Optional[float] = None
    base_md: Optional[float] = None
    description: Optional[str] = None


class MarkupParseSummary(BaseModel):
    """Summary of parsed markup workbook."""

    sheet_name: str
    well_number: str
    total_rows: int
    operation_rows: int
    event_rows: int
    geology_candidates: int
    unique_operation_codes: List[str]
    unique_event_codes: List[str]
    missing_operation_codes: List[str]
    missing_event_codes: List[str]
    samples: List[MarkupSampleRow] = []


class MarkupParseResponse(BaseModel):
    """Response for markup parse endpoint."""

    success: bool = True
    summary: MarkupParseSummary


class MarkupImportSummary(BaseModel):
    """Summary of markup import execution."""

    well_id: int
    wellbore_id: int
    well_number: str
    dry_run: bool
    total_rows: int
    operation_rows: int
    event_rows: int
    operations_created: int
    actual_operations_created: int
    events_created: int
    geology_intervals_created: int
    summary_rows_created: int
    missing_operation_codes: List[str]
    missing_event_codes: List[str]
    warnings: List[str] = []
    errors: List[str] = []
    samples: List[MarkupSampleRow] = []


class MarkupImportResponse(BaseModel):
    """Response for markup import endpoint."""

    success: bool = True
    job_id: str
    summary: MarkupImportSummary


class MarkupImportDetailsResponse(BaseModel):
    """Optional detailed response payload."""

    counts: Dict[str, int]
    unresolved_codes: Dict[str, List[str]]
    sample_rows: List[Dict[str, Any]] = Field(default_factory=list)
