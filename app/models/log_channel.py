"""
Log channel model
"""
from sqlalchemy import Column, Integer, BigInteger, Text, Float, DateTime, Boolean, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import relationship
from ..database import Base


class LogChannel(Base):
    """Метаданные каналов из LAS для конкретного лога"""
    __tablename__ = "log_channels"

    channel_id = Column(Integer, primary_key=True, index=True)
    log_id = Column(Integer, ForeignKey("gti_logs.log_id", ondelete="CASCADE"), nullable=False, index=True)

    las_mnemonic = Column(Text, nullable=False)
    las_unit = Column(Text, nullable=True)
    las_description = Column(Text, nullable=True)
    db_column_name = Column(Text, nullable=True)

    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    null_count = Column(BigInteger, nullable=True)
    total_count = Column(BigInteger, nullable=True)

    was_converted = Column(Boolean, default=False)
    conversion_formula = Column(Text, nullable=True)
    import_status = Column(Text, default="mapped")  # mapped, imported, skipped, error
    import_notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("log_id", "las_mnemonic", name="log_channels_unique"),
    )

    gti_log = relationship("GtiLog", back_populates="log_channels")

    def __repr__(self):
        return f"<LogChannel(channel_id={self.channel_id}, log_id={self.log_id}, mnemonic='{self.las_mnemonic}')>"
