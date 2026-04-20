"""
Events API router
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from ..database import get_db
from ..models import Event, EventType, Wellbore
from ..schemas.event import (
    EventCreate,
    EventResponse,
    EventTypeResponse,
    SvEventsSyncRequest,
    SvEventsSyncResponse,
)
from ..services.sv_events_service import SvEventsService

router = APIRouter(prefix="/events", tags=["Events"])


@router.get("/types", response_model=List[EventTypeResponse])
def get_event_types(db: Session = Depends(get_db)):
    """Get all event types"""
    event_types = db.query(EventType).all()
    return event_types


@router.post("/types", response_model=EventTypeResponse, status_code=201)
def create_event_type(
    event_code: str,
    event_name: str,
    is_complication: bool = True,
    is_precursor: bool = False,
    severity: int = 1,
    target_label: Optional[int] = None,
    description: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Create a new event type"""
    # Check if exists
    existing = db.query(EventType).filter(EventType.event_code == event_code).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Event type '{event_code}' already exists")
    
    event_type = EventType(
        event_code=event_code,
        event_name=event_name,
        is_complication=is_complication,
        is_precursor=is_precursor,
        severity=severity,
        target_label=target_label,
        description=description
    )
    db.add(event_type)
    db.commit()
    db.refresh(event_type)
    return event_type


@router.get("", response_model=List[EventResponse])
def get_events(
    wellbore_id: Optional[int] = Query(None),
    event_type_id: Optional[int] = Query(None),
    annotation_source: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """Get events with filters"""
    query = db.query(Event)
    
    if wellbore_id:
        query = query.filter(Event.wellbore_id == wellbore_id)
    if event_type_id:
        query = query.filter(Event.event_type_id == event_type_id)
    if annotation_source:
        query = query.filter(Event.annotation_source == annotation_source)
    
    events = query.offset(offset).limit(limit).all()
    
    # Add event type info
    result = []
    for event in events:
        event_data = EventResponse.model_validate(event)
        if event.event_type:
            event_data.event_type_code = event.event_type.event_code
            event_data.event_type_name = event.event_type.event_name
        result.append(event_data)
    
    return result


@router.post("", response_model=EventResponse, status_code=201)
def create_event(
    event_data: EventCreate,
    db: Session = Depends(get_db)
):
    """Create a new event"""
    # Check wellbore exists
    wellbore = db.query(Wellbore).filter(Wellbore.wellbore_id == event_data.wellbore_id).first()
    if not wellbore:
        raise HTTPException(status_code=404, detail="Wellbore not found")
    
    # Check event type exists
    event_type = db.query(EventType).filter(EventType.event_type_id == event_data.event_type_id).first()
    if not event_type:
        raise HTTPException(status_code=404, detail="Event type not found")
    
    event = Event(
        wellbore_id=event_data.wellbore_id,
        event_type_id=event_data.event_type_id,
        start_time=event_data.start_time,
        end_time=event_data.end_time,
        start_md=event_data.start_md,
        end_md=event_data.end_md,
        annotation_source=event_data.annotation_source,
        annotator_name=event_data.annotator_name,
        confidence=event_data.confidence,
        notes=event_data.notes
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    
    result = EventResponse.model_validate(event)
    result.event_type_code = event_type.event_code
    result.event_type_name = event_type.event_name
    
    return result


@router.get("/{event_id}", response_model=EventResponse)
def get_event(event_id: int, db: Session = Depends(get_db)):
    """Get event by ID"""
    event = db.query(Event).filter(Event.event_id == event_id).first()
    
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    result = EventResponse.model_validate(event)
    if event.event_type:
        result.event_type_code = event.event_type.event_code
        result.event_type_name = event.event_type.event_name
    
    return result


@router.delete("/{event_id}", status_code=204)
def delete_event(event_id: int, db: Session = Depends(get_db)):
    """Delete event"""
    event = db.query(Event).filter(Event.event_id == event_id).first()
    
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    db.delete(event)
    db.commit()


@router.post("/sync-from-supervisor", response_model=SvEventsSyncResponse)
def sync_events_from_supervisor(
    payload: SvEventsSyncRequest,
    db: Session = Depends(get_db),
):
    """
    Создание записей в events из sv_daily_operations.
    Источник: anomaly_flags/anomaly_severity/description.
    """
    service = SvEventsService(db)
    result = service.sync_events_from_supervisor(
        well_number=payload.well_number,
        date_from=payload.date_from,
        date_to=payload.date_to,
        min_severity=payload.min_severity,
        dry_run=payload.dry_run,
        max_operations=payload.max_operations,
        include_npv_balance=payload.include_npv_balance,
    )
    return SvEventsSyncResponse(**result)
