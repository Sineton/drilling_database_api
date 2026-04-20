"""
Event and EventType models
"""
from sqlalchemy import Column, Integer, Text, Float, Boolean, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from ..database import Base


class EventType(Base):
    """Типы событий/осложнений"""
    __tablename__ = "event_types"

    event_type_id = Column(Integer, primary_key=True, index=True)
    event_code = Column(Text, nullable=False, unique=True)
    event_name = Column(Text, nullable=False)
    parent_code = Column(Text, nullable=True)
    category = Column(Text, nullable=False, default="complication")
    is_complication = Column(Boolean, default=True)
    is_precursor = Column(Boolean, default=False)
    severity = Column(Integer, default=1)
    target_label = Column(Integer, nullable=True)
    detection_threshold = Column(JSONB, nullable=True)
    description = Column(Text, nullable=True)
    typical_duration_min = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    events = relationship("Event", back_populates="event_type")

    def __repr__(self):
        return f"<EventType(event_type_id={self.event_type_id}, event_code='{self.event_code}')>"


class Event(Base):
    """События/осложнения"""
    __tablename__ = "events"

    event_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)
    event_type_id = Column(Integer, ForeignKey("event_types.event_type_id"), nullable=False, index=True)

    # Временная привязка
    start_time = Column(DateTime(timezone=True), nullable=True)
    end_time = Column(DateTime(timezone=True), nullable=True)

    # Глубинная привязка
    start_md = Column(Float, nullable=True)
    end_md = Column(Float, nullable=True)

    # Метаданные разметки
    annotation_source = Column(Text, nullable=False)  # kgkm, ugkm, manual, etc.
    annotator_name = Column(Text, nullable=True)
    confidence = Column(Float, default=1.0)
    notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    wellbore = relationship("Wellbore", back_populates="events")
    event_type = relationship("EventType", back_populates="events")

    def __repr__(self):
        return f"<Event(event_id={self.event_id}, event_type_id={self.event_type_id})>"
