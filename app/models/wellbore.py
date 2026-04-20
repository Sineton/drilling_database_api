"""
Wellbore model
"""
from sqlalchemy import Column, Integer, Text, Float, DateTime, ForeignKey, func, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from ..database import Base


class Wellbore(Base):
    """Стволы скважин"""
    __tablename__ = "wellbores"

    wellbore_id = Column(Integer, primary_key=True, index=True)
    well_id = Column(Integer, ForeignKey("wells.well_id", ondelete="CASCADE"), nullable=False, index=True)
    wellbore_number = Column(Text, nullable=False, default="main")
    diameter_mm = Column(Float, nullable=True)
    azimuth_avg = Column(Float, nullable=True)
    inclination_avg = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    properties = Column(JSONB, nullable=True)

    # Поля из wells_list.xlsx
    construction = Column(Text, nullable=True)
    casing_diameter_mm = Column(Float, nullable=True)
    tubing_diameter_mm = Column(Float, nullable=True)
    gdi_data = Column(Text, nullable=True)
    injectivity_coefficient = Column(Float, nullable=True)
    circulation_character = Column(Text, nullable=True)
    circulation_percent = Column(Float, nullable=True)

    # Поля из журнала супервайзера
    abs_mark_bottom_m = Column(Float, nullable=True)
    design_depth_vertical_m = Column(Float, nullable=True)
    design_depth_md_m = Column(Float, nullable=True)
    offset_to_bottom_m = Column(Float, nullable=True)
    azimuth_to_bottom = Column(Float, nullable=True)

    # Constraints
    __table_args__ = (
        UniqueConstraint("well_id", "wellbore_number", name="wellbores_unique"),
    )

    # Relationships
    well = relationship("Well", back_populates="wellbores")
    gti_logs = relationship("GtiLog", back_populates="wellbore", cascade="all, delete-orphan")
    events = relationship("Event", back_populates="wellbore", cascade="all, delete-orphan")
    sv_daily_reports = relationship("SvDailyReport", back_populates="wellbore", cascade="all, delete-orphan")
    sv_npv_balance = relationship("SvNpvBalance", back_populates="wellbore", cascade="all, delete-orphan")
    sv_contractors = relationship("SvContractor", back_populates="wellbore", cascade="all, delete-orphan")
    sv_well_construction = relationship("SvWellConstruction", back_populates="wellbore", cascade="all, delete-orphan")
    sv_rig_equipment = relationship("SvRigEquipment", back_populates="wellbore", cascade="all, delete-orphan")
    sv_construction_timing = relationship("SvConstructionTiming", back_populates="wellbore", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Wellbore(wellbore_id={self.wellbore_id}, well_id={self.well_id})>"
