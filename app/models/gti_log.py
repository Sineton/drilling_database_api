"""
GTI Log model
"""
from sqlalchemy import Column, Integer, BigInteger, String, Text, Float, DateTime, ForeignKey, func, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from ..database import Base


class GtiLog(Base):
    """Сессии логирования ГТИ"""
    __tablename__ = "gti_logs"
    
    log_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(Integer, ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"), nullable=False, index=True)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=False)
    sampling_rate_sec = Column(Float, nullable=True)
    total_records = Column(BigInteger, nullable=True)
    quality_status = Column(Text, default="pending")  # pending, validated, has_issues
    quality_report = Column(JSONB, nullable=True)
    source_file_path = Column(Text, nullable=True)
    file_format = Column(Text, nullable=True)  # las, csv, witsml
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Constraints
    __table_args__ = (
        CheckConstraint("end_time > start_time", name="gti_logs_time_range"),
    )
    
    # Relationships
    wellbore = relationship("Wellbore", back_populates="gti_logs")
    snapshots = relationship("GtiSnapshot", back_populates="gti_log", cascade="all, delete-orphan")
    log_channels = relationship("LogChannel", back_populates="gti_log", cascade="all, delete-orphan")
    files = relationship("File", back_populates="gti_log")
    
    def __repr__(self):
        return f"<GtiLog(log_id={self.log_id}, wellbore_id={self.wellbore_id})>"
