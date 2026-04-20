"""
Supervisor well construction model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvWellConstruction(Base):
    """Конструкция скважины (детальная)"""
    __tablename__ = "sv_well_construction"

    construction_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)

    casing_type = Column(Text, nullable=False)
    outer_diameter_mm = Column(Float, nullable=True)
    depth_m = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    wellbore = relationship("Wellbore", back_populates="sv_well_construction")

    def __repr__(self):
        return f"<SvWellConstruction(id={self.construction_id}, type={self.casing_type})>"
