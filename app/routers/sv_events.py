"""
Dedicated router for filling events from supervisor journal tables.
"""
from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import Well
from ..schemas.event import (
    SvEventsFillByWellRequest,
    SvEventsSyncResponse,
    SvEventsCleanupRequest,
    SvEventsCleanupResponse,
    SvEventsRebuildRequest,
    SvEventsRebuildResponse,
    SvEventsDiagnoseRequest,
    SvEventsDiagnoseResponse,
)
from ..services.sv_events_service import SvEventsService

router = APIRouter(prefix="/sv-events", tags=["Supervisor Events"])


@router.post("/fill/{well_number}", response_model=SvEventsSyncResponse)
def fill_events_by_well(
    payload: SvEventsFillByWellRequest,
    well_number: str = Path(..., description="Номер скважины, например 3189Д"),
    db: Session = Depends(get_db),
):
    """
    Заполняет таблицу events по одной скважине на основании:
    - sv_daily_operations (флаги аномалий, severity, описание)
    - sv_npv_balance (баланс НПВ), если include_npv_balance=true
    """
    well = db.query(Well).filter(Well.well_number == well_number).first()
    if not well:
        raise HTTPException(status_code=404, detail=f"Скважина {well_number} не найдена")

    service = SvEventsService(db)
    result = service.sync_events_from_supervisor(
        well_number=well_number,
        date_from=payload.date_from,
        date_to=payload.date_to,
        min_severity=payload.min_severity,
        dry_run=payload.dry_run,
        max_operations=payload.max_operations,
        include_npv_balance=payload.include_npv_balance,
    )
    return SvEventsSyncResponse(**result)


@router.post("/cleanup/{well_number}", response_model=SvEventsCleanupResponse)
def cleanup_events_by_well(
    payload: SvEventsCleanupRequest,
    well_number: str = Path(..., description="Номер скважины, например 3189Д"),
    db: Session = Depends(get_db),
):
    """
    Удаляет из events автоматически созданные записи по одной скважине.
    Удаляются только источники:
    - supervisor_journal
    - supervisor_journal_npv (если include_npv_balance=true)
    """
    well = db.query(Well).filter(Well.well_number == well_number).first()
    if not well:
        raise HTTPException(status_code=404, detail=f"Скважина {well_number} не найдена")

    service = SvEventsService(db)
    result = service.cleanup_events_from_supervisor(
        well_number=well_number,
        date_from=payload.date_from,
        date_to=payload.date_to,
        dry_run=payload.dry_run,
        include_npv_balance=payload.include_npv_balance,
    )
    return SvEventsCleanupResponse(**result)


@router.post("/rebuild/{well_number}", response_model=SvEventsRebuildResponse)
def rebuild_events_by_well(
    payload: SvEventsRebuildRequest,
    well_number: str = Path(..., description="Номер скважины, например 3189Д"),
    db: Session = Depends(get_db),
):
    """
    Полный цикл по скважине:
      1) cleanup авто-событий из supervisor_journal
      2) повторное заполнение events из sv_* таблиц
    """
    well = db.query(Well).filter(Well.well_number == well_number).first()
    if not well:
        raise HTTPException(status_code=404, detail=f"Скважина {well_number} не найдена")

    service = SvEventsService(db)
    cleanup_result = service.cleanup_events_from_supervisor(
        well_number=well_number,
        date_from=payload.date_from,
        date_to=payload.date_to,
        dry_run=payload.dry_run,
        include_npv_balance=payload.include_npv_balance,
    )

    fill_result = service.sync_events_from_supervisor(
        well_number=well_number,
        date_from=payload.date_from,
        date_to=payload.date_to,
        min_severity=payload.min_severity,
        dry_run=payload.dry_run,
        max_operations=payload.max_operations,
        include_npv_balance=payload.include_npv_balance,
    )

    return SvEventsRebuildResponse(
        well_number=well_number,
        dry_run=payload.dry_run,
        cleanup=SvEventsCleanupResponse(**cleanup_result),
        fill=SvEventsSyncResponse(**fill_result),
    )


@router.post("/diagnose/{well_number}", response_model=SvEventsDiagnoseResponse)
def diagnose_events_by_well(
    payload: SvEventsDiagnoseRequest,
    well_number: str = Path(..., description="Номер скважины, например 3189Д"),
    db: Session = Depends(get_db),
):
    """
    Диагностика причин, почему события по sv_* не создаются в events.
    Ничего не пишет в БД, только возвращает breakdown по причинам:
    - ready_to_create
    - already_exists
    - missing_event_type
    """
    well = db.query(Well).filter(Well.well_number == well_number).first()
    if not well:
        raise HTTPException(status_code=404, detail=f"Скважина {well_number} не найдена")

    service = SvEventsService(db)
    result = service.diagnose_events_from_supervisor(
        well_number=well_number,
        date_from=payload.date_from,
        date_to=payload.date_to,
        min_severity=payload.min_severity,
        max_operations=payload.max_operations,
        include_npv_balance=payload.include_npv_balance,
    )
    return SvEventsDiagnoseResponse(**result)

