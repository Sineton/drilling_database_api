"""
Supervisor chemical reagents model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvChemicalReagent(Base):
    """Химические реагенты"""
    __tablename__ = "sv_chemical_reagents"

    reagent_id = Column(Integer, primary_key=True, index=True)
    report_id = Column(Integer, ForeignKey("sv_daily_reports.report_id", ondelete="CASCADE"), nullable=False, index=True)
    reagent_name = Column(Text, nullable=False)
    unit = Column(Text, nullable=True)

    total_received = Column(Float, nullable=True)
    used_preparation = Column(Float, nullable=True)
    used_treatment = Column(Float, nullable=True)
    used_regeneration = Column(Float, nullable=True)
    exported = Column(Float, nullable=True)
    remaining = Column(Float, nullable=True)

    source_row = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    report = relationship("SvDailyReport", back_populates="chemical_reagents")

    def __repr__(self):
        return f"<SvChemicalReagent(id={self.reagent_id}, name={self.reagent_name})>"
