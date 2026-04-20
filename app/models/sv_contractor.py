"""
Supervisor contractors model
"""
from sqlalchemy import (
    Column, Integer, Text, DateTime, ForeignKey, func, UniqueConstraint
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvContractor(Base):
    """Подрядчики по скважине"""
    __tablename__ = "sv_contractors"

    contractor_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)

    role = Column(Text, nullable=False)
    company_name = Column(Text, nullable=False)

    evaluation_text = Column(Text, nullable=True)
    evaluation_score = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("wellbore_id", "role", name="sv_contractors_unique"),
    )

    # Relationships
    wellbore = relationship("Wellbore", back_populates="sv_contractors")

    def __repr__(self):
        return f"<SvContractor(id={self.contractor_id}, role={self.role})>"
