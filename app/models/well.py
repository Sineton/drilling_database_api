"""
Well model
"""
from sqlalchemy import Column, Integer, Text, Float, Date, DateTime, Boolean, func
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import relationship
from ..database import Base


class Well(Base):
    """Скважины"""
    __tablename__ = "wells"

    well_id = Column(Integer, primary_key=True, index=True)
    well_number = Column(Text, nullable=False, unique=True, index=True)
    well_name = Column(Text, nullable=True)
    field = Column(Text, nullable=True)
    project_code = Column(Text, nullable=False, index=True)
    company = Column(Text, default="ПАО Татнефть")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    metadata_ = Column("metadata", JSONB, nullable=True)

    # Поля из wells_list.xlsx
    field_name = Column(Text, default="Миннибаевское")
    pad_number = Column(Text, nullable=True)
    category = Column(Text, nullable=True)
    ngdu = Column(Text, nullable=True)
    well_category = Column(Text, nullable=True)
    completion_date = Column(Date, nullable=True)

    # Файловое хранилище
    storage_path = Column(Text, nullable=True)
    file_storage_url = Column(Text, nullable=True)
    has_realtime_data = Column(Boolean, default=False)
    has_reports = Column(Boolean, default=False)
    has_drilling_program = Column(Boolean, default=False)
    has_supervision_log = Column(Boolean, default=False)
    documents = Column(JSONB, nullable=True)

    # Паспорт скважины (из журнала супервайзера)
    well_purpose = Column(Text, nullable=True)
    group_project = Column(Text, nullable=True)
    target_horizon = Column(Text, nullable=True)
    productive_layer = Column(Text, nullable=True)
    altitude_rotor_m = Column(Float, nullable=True)
    abs_mark_top_m = Column(Float, nullable=True)
    design_depth_vertical_m = Column(Float, nullable=True)
    design_depth_md_m = Column(Float, nullable=True)
    magnetic_azimuth = Column(Float, nullable=True)
    design_offset_m = Column(Float, nullable=True)
    tolerance_radius_m = Column(Float, nullable=True)
    rig_type = Column(Text, nullable=True)
    mounting_type = Column(Text, nullable=True)
    drilling_start_date = Column(DateTime(timezone=True), nullable=True)
    drilling_end_date = Column(DateTime(timezone=True), nullable=True)
    calendar_days = Column(Float, nullable=True)
    unplanned_days = Column(Float, nullable=True)
    npv_repair_days = Column(Float, nullable=True)
    idle_days = Column(Float, nullable=True)
    complication_days = Column(Float, nullable=True)
    supervisors = Column(ARRAY(Text), nullable=True)

    # Relationships
    wellbores = relationship("Wellbore", back_populates="well", cascade="all, delete-orphan")
    files = relationship("File", back_populates="well")

    def __repr__(self):
        return f"<Well(well_id={self.well_id}, well_number='{self.well_number}')>"
