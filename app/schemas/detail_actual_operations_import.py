"""
Schemas for importing the "Детализация" sheet into actual_operations.
"""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class DetailActualOperationSample(BaseModel):
    """Sample imported actual operation row."""

    sequence_number: int
    start_time: datetime
    end_time: datetime
    operation_label: Optional[str] = None
    description: str


class DetailActualOperationsImportSummary(BaseModel):
    """Summary for detail-sheet import into actual_operations."""

    well_id: int
    wellbore_id: int
    well_number: str
    dry_run: bool
    source_file: str
    total_rows: int
    imported_rows: int
    skipped_rows: int
    deleted_existing_rows: int
    matched_operations: int
    unmatched_operations: int
    rows_with_depth_interval: int
    warnings: List[str] = Field(default_factory=list)
    samples: List[DetailActualOperationSample] = Field(default_factory=list)


class DetailActualOperationsImportResponse(BaseModel):
    """Response for detail-sheet import endpoint."""

    success: bool = True
    job_id: str
    summary: DetailActualOperationsImportSummary
