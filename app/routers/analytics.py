"""
Analytics endpoints (draft): anomalies and field summary.
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas.analytics import AnomaliesResponse, FieldSummaryResponse
from ..services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/anomalies", response_model=AnomaliesResponse)
def get_anomalies(
    well_number: str = Query(..., description="Номер скважины"),
    date_from: Optional[datetime] = Query(None, description="Начало интервала"),
    date_to: Optional[datetime] = Query(None, description="Конец интервала"),
    min_score: int = Query(2, ge=1, le=10, description="Минимальный балл аномалии"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    service = AnalyticsService(db)
    total, items = service.get_anomalies(
        well_number=well_number,
        date_from=date_from,
        date_to=date_to,
        min_score=min_score,
        limit=limit,
        offset=offset,
    )
    return AnomaliesResponse(total=total, items=items)


@router.get("/field-summary", response_model=FieldSummaryResponse)
def get_field_summary(
    field: str = Query(..., description="Название месторождения"),
    db: Session = Depends(get_db),
):
    service = AnalyticsService(db)
    summary = service.get_field_summary(field=field)
    if not summary:
        raise HTTPException(status_code=404, detail=f"Field '{field}' not found")
    return FieldSummaryResponse(**summary)
