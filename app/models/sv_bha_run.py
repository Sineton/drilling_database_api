"""
Supervisor BHA (Bottom Hole Assembly) runs model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, DateTime, ForeignKey, func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from ..database import Base


class SvBhaRun(Base):
    """КНБК — компоновки низа бурильной колонны"""
    __tablename__ = "sv_bha_runs"

    bha_id = Column(Integer, primary_key=True, index=True)
    report_id = Column(Integer, ForeignKey("sv_daily_reports.report_id", ondelete="CASCADE"), nullable=False, index=True)
    bha_number = Column(Integer, nullable=True)
    status = Column(Text, nullable=False, default="fact")

    description = Column(Text, nullable=False)

    bit_type = Column(Text, nullable=True)
    bit_size_mm = Column(Float, nullable=True)
    bit_serial = Column(Text, nullable=True)
    motor_type = Column(Text, nullable=True)
    motor_angle = Column(Float, nullable=True)
    mwd_type = Column(Text, nullable=True)

    components = Column(JSONB, nullable=True)
    total_length_m = Column(Float, nullable=True)

    source_row = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    report = relationship("SvDailyReport", back_populates="bha_runs")

    def __repr__(self):
        return f"<SvBhaRun(bha_id={self.bha_id}, num={self.bha_number})>"
