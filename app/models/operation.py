"""
Operation model
"""
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, func
from ..database import Base


class Operation(Base):
    """Типы операций при бурении"""
    __tablename__ = "operations"
    
    operation_id = Column(Integer, primary_key=True, index=True)
    operation_code = Column(Text, nullable=False, unique=True)
    operation_name = Column(Text, nullable=False)
    is_drilling = Column(Boolean, default=False)
    risk_level = Column(Integer, default=0)  # 0-3
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    def __repr__(self):
        return f"<Operation(operation_id={self.operation_id}, operation_code='{self.operation_code}')>"
