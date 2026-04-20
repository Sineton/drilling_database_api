"""
Supervisor construction timing model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvConstructionTiming(Base):
    """Плановая/фактическая длительность по интервалам"""
    __tablename__ = "sv_construction_timing"

    timing_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)

    interval_name = Column(Text, nullable=False)
    plan_hours = Column(Float, nullable=True)
    plan_days = Column(Float, nullable=True)
    fact_hours = Column(Float, nullable=True)
    fact_days = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    wellbore = relationship("Wellbore", back_populates="sv_construction_timing")

    def __repr__(self):
        return f"<SvConstructionTiming(id={self.timing_id}, interval={self.interval_name})>"
