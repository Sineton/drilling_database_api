"""
Supervisor drilling regime model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvDrillingRegime(Base):
    """Режимы бурения (план/факт)"""
    __tablename__ = "sv_drilling_regime"

    regime_id = Column(Integer, primary_key=True, index=True)
    report_id = Column(Integer, ForeignKey("sv_daily_reports.report_id", ondelete="CASCADE"), nullable=False, index=True)
    status = Column(Text, nullable=False, default="fact")

    wob_min_ton = Column(Float, nullable=True)
    wob_max_ton = Column(Float, nullable=True)
    rpm_min = Column(Float, nullable=True)
    rpm_max = Column(Float, nullable=True)
    pressure_min = Column(Float, nullable=True)
    pressure_max = Column(Float, nullable=True)
    flow_rate_l_s = Column(Float, nullable=True)
    delta_p = Column(Float, nullable=True)
    pump_count = Column(Integer, nullable=True)
    liner_diameter_mm = Column(Float, nullable=True)
    notes = Column(Text, nullable=True)

    source_row = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    report = relationship("SvDailyReport", back_populates="drilling_regimes")

    def __repr__(self):
        return f"<SvDrillingRegime(regime_id={self.regime_id}, status={self.status})>"
