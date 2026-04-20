"""
Supervisor daily operations model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, Boolean, Time, DateTime,
    ForeignKey, func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from ..database import Base


class SvDailyOperation(Base):
    """Технологические операции (посуточно)"""
    __tablename__ = "sv_daily_operations"

    operation_id = Column(Integer, primary_key=True, index=True)
    report_id = Column(Integer, ForeignKey("sv_daily_reports.report_id", ondelete="CASCADE"), nullable=False, index=True)
    sequence_number = Column(Integer, nullable=False)

    time_from = Column(Time, nullable=True)
    time_to = Column(Time, nullable=True)
    duration_text = Column(Text, nullable=True)
    duration_minutes = Column(Integer, nullable=True)

    description = Column(Text, nullable=False)

    operation_category = Column(Text, nullable=True, index=True)
    is_npv = Column(Boolean, default=False)
    is_complication = Column(Boolean, default=False)
    is_repair = Column(Boolean, default=False)

    extracted_params = Column(JSONB, nullable=True)
    depth_from_m = Column(Float, nullable=True)
    depth_to_m = Column(Float, nullable=True)

    anomaly_flags = Column(JSONB, nullable=True)
    anomaly_severity = Column(Integer, default=0)

    source_row = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    report = relationship("SvDailyReport", back_populates="operations")

    def __repr__(self):
        return f"<SvDailyOperation(id={self.operation_id}, seq={self.sequence_number})>"
