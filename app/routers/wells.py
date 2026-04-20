"""
Wells API router
"""
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from ..database import get_db
from ..schemas.demo import WellParametersResponse, WellsOverviewResponse
from ..services.well_service import WellService
from ..services.demo_service import DemoService
from ..schemas.well import (
    WellCreate,
    WellUpdate,
    WellResponse,
    WellDetailResponse,
    WellListResponse,
)
from ..schemas.wellbore import WellboreCreate, WellboreResponse

router = APIRouter(prefix="/wells", tags=["Wells"])


@router.get("/overview", response_model=WellsOverviewResponse)
def get_wells_overview(
    target_time: Optional[datetime] = Query(
        None,
        description="Момент времени для выбора активных скважин. По умолчанию используется demo-срез.",
    ),
    window_start: Optional[datetime] = Query(
        None,
        description="Начало окна для выбора последнего GTI snapshot.",
    ),
    window_end: Optional[datetime] = Query(
        None,
        description="Конец окна для выбора последнего GTI snapshot.",
    ),
    field: Optional[str] = Query(
        None,
        description="Фильтр по месторождению/площади.",
    ),
    db: Session = Depends(get_db),
):
    """Get demo overview for wells active at a given moment."""
    service = DemoService(db)
    try:
        return service.get_wells_overview(
            target_time=target_time,
            window_start=window_start,
            window_end=window_end,
            field=field,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{well_number}/parameters", response_model=WellParametersResponse)
def get_well_parameters(
    well_number: str,
    target_time: Optional[datetime] = Query(
        None,
        description="Момент времени для определения контекста операции/события. По умолчанию используется demo-срез.",
    ),
    window_start: Optional[datetime] = Query(
        None,
        description="Начало интервала временного ряда.",
    ),
    window_end: Optional[datetime] = Query(
        None,
        description="Конец интервала временного ряда.",
    ),
    params: Optional[str] = Query(
        None,
        description="CSV список параметров, например torque,wob,rpm,flow_in.",
    ),
    bucket: str = Query(
        "minute",
        description="Агрегация временного ряда: raw, minute, 5min, hour.",
    ),
    db: Session = Depends(get_db),
):
    """Get well parameters and demo context for a selected interval."""
    service = DemoService(db)
    try:
        return service.get_well_parameters(
            well_number=well_number,
            target_time=target_time,
            window_start=window_start,
            window_end=window_end,
            params=params.split(",") if params else None,
            bucket=bucket,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("", response_model=WellListResponse)
def get_wells(
    project_code: Optional[str] = Query(None, description="Фильтр по проекту"),
    field: Optional[str] = Query(None, description="Фильтр по месторождению"),
    pad_number: Optional[str] = Query(None, description="Фильтр по кусту"),
    search: Optional[str] = Query(None, description="Поиск по номеру скважины"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Get list of wells with filters"""
    service = WellService(db)
    wells, total = service.get_wells(
        project_code=project_code,
        field=field,
        pad_number=pad_number,
        search=search,
        limit=limit,
        offset=offset,
    )

    items = []
    for well in wells:
        well_data = WellResponse.model_validate(well)
        well_data.wellbores_count = service.count_wellbores(well.well_id)
        well_data.logs_count = service.count_logs(well.well_id)
        items.append(well_data)

    return WellListResponse(total=total, limit=limit, offset=offset, items=items)


@router.get("/{well_id}", response_model=WellDetailResponse)
def get_well(well_id: int, db: Session = Depends(get_db)):
    """Get well details by ID"""
    service = WellService(db)
    well = service.get_well(well_id)

    if not well:
        raise HTTPException(status_code=404, detail="Well not found")

    response = WellDetailResponse.model_validate(well)
    response.wellbores_count = service.count_wellbores(well.well_id)
    response.logs_count = service.count_logs(well.well_id)
    return response


@router.post("", response_model=WellResponse, status_code=201)
def create_well(well_data: WellCreate, db: Session = Depends(get_db)):
    """Create a new well"""
    service = WellService(db)

    existing = service.get_well_by_number(well_data.well_number)
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Well with number '{well_data.well_number}' already exists",
        )

    well = service.create_well(well_data)
    return well


@router.put("/{well_id}", response_model=WellResponse)
def update_well(well_id: int, well_data: WellUpdate, db: Session = Depends(get_db)):
    """Update existing well"""
    service = WellService(db)
    well = service.update_well(well_id, well_data)

    if not well:
        raise HTTPException(status_code=404, detail="Well not found")

    return well


@router.delete("/{well_id}", status_code=204)
def delete_well(well_id: int, db: Session = Depends(get_db)):
    """Delete well"""
    service = WellService(db)

    if not service.delete_well(well_id):
        raise HTTPException(status_code=404, detail="Well not found")


@router.post("/{well_id}/wellbores", response_model=WellboreResponse, status_code=201)
def create_wellbore(
    well_id: int,
    wellbore_data: WellboreCreate,
    db: Session = Depends(get_db),
):
    """Create a wellbore for a well"""
    service = WellService(db)

    well = service.get_well(well_id)
    if not well:
        raise HTTPException(status_code=404, detail="Well not found")

    existing = service.get_wellbore_by_well(well_id, wellbore_data.wellbore_number)
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Wellbore '{wellbore_data.wellbore_number}' already exists for this well",
        )

    wellbore = service.create_wellbore(
        well_id=well_id,
        wellbore_number=wellbore_data.wellbore_number,
        diameter_mm=wellbore_data.diameter_mm,
        construction=wellbore_data.construction,
        casing_diameter_mm=wellbore_data.casing_diameter_mm,
        tubing_diameter_mm=wellbore_data.tubing_diameter_mm,
        gdi_data=wellbore_data.gdi_data,
        injectivity_coefficient=wellbore_data.injectivity_coefficient,
        circulation_character=wellbore_data.circulation_character,
        circulation_percent=wellbore_data.circulation_percent,
        properties=wellbore_data.properties,
    )
    return wellbore
