"""
Supervisor daily report model
"""
from sqlalchemy import (
    Column, Integer, Text, Float, Date, DateTime, ForeignKey,
    func, UniqueConstraint
)
from sqlalchemy.orm import relationship
from ..database import Base


class SvDailyReport(Base):
    """Ежедневные отчёты супервайзера"""
    __tablename__ = "sv_daily_reports"

    report_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)
    report_date = Column(Date, nullable=False)

    construction_stage = Column(Text, nullable=True)
    interval_from_m = Column(Float, nullable=True)
    interval_to_m = Column(Float, nullable=True)

    current_depth_m = Column(Float, nullable=True)
    penetration_m = Column(Float, nullable=True)
    drilling_time_h = Column(Float, nullable=True)
    rop_plan = Column(Float, nullable=True)
    rop_fact = Column(Float, nullable=True)
    cum_drilling_time_plan_h = Column(Float, nullable=True)
    cum_drilling_time_fact_h = Column(Float, nullable=True)
    cum_penetration_m = Column(Float, nullable=True)
    cum_avg_rop = Column(Float, nullable=True)
    drilling_comment = Column(Text, nullable=True)

    source_file_id = Column(Integer, ForeignKey("files.file_id"), nullable=True)
    source_row_start = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("wellbore_id", "report_date", name="sv_daily_reports_unique"),
    )

    # Relationships
    wellbore = relationship("Wellbore", back_populates="sv_daily_reports")
    source_file = relationship("File")
    operations = relationship("SvDailyOperation", back_populates="report", cascade="all, delete-orphan")
    bha_runs = relationship("SvBhaRun", back_populates="report", cascade="all, delete-orphan")
    drilling_regimes = relationship("SvDrillingRegime", back_populates="report", cascade="all, delete-orphan")
    mud_accounting = relationship("SvMudAccounting", back_populates="report", cascade="all, delete-orphan")
    chemical_reagents = relationship("SvChemicalReagent", back_populates="report", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<SvDailyReport(report_id={self.report_id}, date={self.report_date})>"
