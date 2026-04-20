"""
Supervisor mud accounting model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvMudAccounting(Base):
    """Учёт бурового раствора (баланс)"""
    __tablename__ = "sv_mud_accounting"

    accounting_id = Column(Integer, primary_key=True, index=True)
    report_id = Column(Integer, ForeignKey("sv_daily_reports.report_id", ondelete="CASCADE"), nullable=False, index=True)
    mud_type = Column(Text, nullable=False)

    volume_start = Column(Float, nullable=True)
    volume_prepared = Column(Float, nullable=True)
    volume_weighted = Column(Float, nullable=True)
    volume_delivered = Column(Float, nullable=True)
    volume_exported = Column(Float, nullable=True)
    volume_disposed = Column(Float, nullable=True)
    volume_increased = Column(Float, nullable=True)

    total_losses = Column(Float, nullable=True)
    surface_losses = Column(Float, nullable=True)
    cleaning_losses = Column(Float, nullable=True)
    spo_losses = Column(Float, nullable=True)
    spill_losses = Column(Float, nullable=True)
    tank_cleaning = Column(Float, nullable=True)
    pit_discharge = Column(Float, nullable=True)
    cement_zone = Column(Float, nullable=True)
    mud_transition = Column(Float, nullable=True)

    downhole_losses = Column(Float, nullable=True)
    absorption = Column(Float, nullable=True)
    washout = Column(Float, nullable=True)
    wellbore_remain = Column(Float, nullable=True)
    filtration = Column(Float, nullable=True)
    circulation_pump = Column(Float, nullable=True)

    volume_remaining = Column(Float, nullable=True)

    source_row = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    report = relationship("SvDailyReport", back_populates="mud_accounting")

    def __repr__(self):
        return f"<SvMudAccounting(id={self.accounting_id}, type={self.mud_type})>"
