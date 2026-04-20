"""
File catalog model
"""
from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, ForeignKey, func
from sqlalchemy.orm import relationship
from ..database import Base


class File(Base):
    """Каталог файлов"""
    __tablename__ = "files"
    
    file_id = Column(Integer, primary_key=True, index=True)
    file_name = Column(Text, nullable=False)
    file_path = Column(Text, nullable=False)
    file_type = Column(Text, nullable=False)  # las, pdf, xlsx
    category = Column(Text, nullable=False)   # gti_data, report, documentation
    
    # Связи
    well_id = Column(Integer, ForeignKey("wells.well_id"), nullable=True, index=True)
    log_id = Column(Integer, ForeignKey("gti_logs.log_id"), nullable=True, index=True)
    
    # Метаданные
    file_size_bytes = Column(BigInteger, nullable=True)
    md5_hash = Column(Text, nullable=True)
    upload_time = Column(DateTime(timezone=True), server_default=func.now())
    
    # Статус обработки
    processing_status = Column(Text, default="pending")  # pending, processing, completed, error
    processing_log = Column(Text, nullable=True)
    
    # Relationships
    well = relationship("Well", back_populates="files")
    gti_log = relationship("GtiLog", back_populates="files")
    
    def __repr__(self):
        return f"<File(file_id={self.file_id}, file_name='{self.file_name}')>"
