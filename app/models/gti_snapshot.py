"""
GTI Snapshot model - Time series data
"""
from sqlalchemy import Column, Integer, BigInteger, Float, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from ..database import Base


class GtiSnapshot(Base):
    """Кадры ГТИ - временные ряды параметров бурения"""
    __tablename__ = "gti_snapshots"
    
    snapshot_id = Column(BigInteger, primary_key=True, index=True)
    log_id = Column(Integer, ForeignKey("gti_logs.log_id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Временная и глубинная привязка
    time_utc = Column(DateTime(timezone=True), nullable=False)
    dbtm = Column(Float, nullable=True)   # Depth below total measured
    dmea = Column(Float, nullable=True)   # Measured depth (MD)
    tvd = Column(Float, nullable=True)    # True vertical depth
    
    # Основные параметры бурения (column names match SQL schema with 'a' suffix)
    woba = Column('woba', Float, nullable=True)    # Weight on bit (нагрузка на долото)
    ropa = Column('ropa', Float, nullable=True)    # Rate of penetration (мех. скорость)
    rpma = Column('rpma', Float, nullable=True)    # Revolutions per minute (обороты)
    tqa = Column('tqa', Float, nullable=True)      # Torque (крутящий момент)
    bpos = Column(Float, nullable=True)            # Bit position (положение долота)
    
    # Параметры давления
    sppa = Column('sppa', Float, nullable=True)    # Standpipe pressure (давление в стояке)
    
    # Параметры бурового раствора
    mfia = Column('mfia', Float, nullable=True)    # Mud flow in (расход на входе)
    mfoa = Column('mfoa', Float, nullable=True)    # Mud flow out (расход на выходе)
    mdia = Column('mdia', Float, nullable=True)    # Mud weight in (плотность на входе)
    mdoa = Column('mdoa', Float, nullable=True)    # Mud weight out (плотность на выходе)
    mtia = Column(Float, nullable=True)            # Mud temperature in
    mtoa = Column(Float, nullable=True)            # Mud temperature out
    tvt = Column('tvt', Float, nullable=True)      # Total volume
    
    # Параметры насосов
    spm1 = Column(Float, nullable=True)            # Strokes per minute (pump 1)
    spm2 = Column(Float, nullable=True)            # Strokes per minute (pump 2)
    
    # Газовые параметры
    gasa = Column('gasa', Float, nullable=True)    # Total gas
    c1c5 = Column(Float, nullable=True)            # C1-C5 gas concentration
    
    # Механические параметры
    hkla = Column('hkla', Float, nullable=True)    # Hookload (нагрузка на крюке)
    
    # Дополнительные параметры
    params_extra = Column(JSONB, nullable=True)
    
    # Связи
    operation_id = Column(Integer, ForeignKey("operations.operation_id"), nullable=True)
    event_id = Column(Integer, ForeignKey("events.event_id"), nullable=True)
    
    # Метки качества
    quality_flags = Column(Integer, default=0)  # Битовые флаги
    
    # Constraints
    __table_args__ = (
        UniqueConstraint("log_id", "time_utc", name="gti_snapshots_unique_time"),
    )
    
    # Relationships
    gti_log = relationship("GtiLog", back_populates="snapshots")
    
    def __repr__(self):
        return f"<GtiSnapshot(snapshot_id={self.snapshot_id}, time_utc={self.time_utc})>"
