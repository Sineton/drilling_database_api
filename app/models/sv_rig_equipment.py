"""
Supervisor rig equipment model
"""
from sqlalchemy import (
    Column, Integer, Text, DateTime, ForeignKey, func
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvRigEquipment(Base):
    """Буровое оборудование"""
    __tablename__ = "sv_rig_equipment"

    equipment_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)

    rig_type = Column(Text, nullable=True)
    talev_system = Column(Text, nullable=True)
    pump_type = Column(Text, nullable=True)
    shaker_type = Column(Text, nullable=True)
    hydrocyclone_type = Column(Text, nullable=True)
    tank_system = Column(Text, nullable=True)
    pit_description = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    wellbore = relationship("Wellbore", back_populates="sv_rig_equipment")

    def __repr__(self):
        return f"<SvRigEquipment(id={self.equipment_id})>"
