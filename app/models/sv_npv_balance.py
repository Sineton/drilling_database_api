"""
Supervisor NPV (non-productive time) balance model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, Boolean, Date, DateTime,
    ForeignKey, func
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvNpvBalance(Base):
    """Баланс непроизводительного времени"""
    __tablename__ = "sv_npv_balance"

    npv_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)

    incident_date = Column(Date, nullable=False, index=True)
    description = Column(Text, nullable=False)
    duration_hours = Column(Float, nullable=True)
    responsible_party = Column(Text, nullable=True)
    category = Column(Text, nullable=False, index=True)
    operation_type = Column(Text, nullable=True)

    root_cause = Column(Text, nullable=True)
    prevention_possible = Column(Boolean, nullable=True)

    source_file_id = Column(Integer, ForeignKey("files.file_id"), nullable=True)
    source_row = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    wellbore = relationship("Wellbore", back_populates="sv_npv_balance")
    source_file = relationship("File")

    def __repr__(self):
        return f"<SvNpvBalance(npv_id={self.npv_id}, cat={self.category})>"
