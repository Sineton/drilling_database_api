"""
Actual operations imported from markup and other factual sources.
"""
from sqlalchemy import Column, Integer, Float, Text, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import ARRAY

from ..database import Base


class ActualOperation(Base):
    """Фактические операции по стволу скважины."""
    __tablename__ = "actual_operations"

    actual_operation_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(
        Integer,
        ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    operation_id = Column(
        Integer,
        ForeignKey("operations.operation_id"),
        nullable=True,
        index=True,
    )

    source_file = Column(Text, nullable=False, index=True)
    sequence_number = Column(Integer, nullable=True)

    start_time = Column(DateTime(timezone=True), nullable=True, index=True)
    end_time = Column(DateTime(timezone=True), nullable=True)
    duration_minutes = Column(Integer, nullable=True)

    depth_from_m = Column(Float, nullable=True)
    depth_to_m = Column(Float, nullable=True)

    operation_code = Column(Text, nullable=True, index=True)
    operation_label = Column(Text, nullable=True)
    description = Column(Text, nullable=False)
    risk_level_id = Column(Integer, nullable=True)
    markup_code = Column(Text, nullable=True)
    event_codes = Column(ARRAY(Text), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return (
            f"<ActualOperation(actual_operation_id={self.actual_operation_id}, "
            f"wellbore_id={self.wellbore_id}, operation_code='{self.operation_code}')>"
        )
