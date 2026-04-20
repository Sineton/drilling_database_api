"""
Summary table for markup file rows.
"""
from sqlalchemy import Column, Integer, Float, Text, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import ARRAY

from ..database import Base


class MarkupFileRow(Base):
    """Сводная запись по одной строке файла разметки."""
    __tablename__ = "markup_file_rows"

    markup_row_id = Column(Integer, primary_key=True, index=True)
    source_file = Column(Text, nullable=False, index=True)

    wellbore_id = Column(
        Integer,
        ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_well_number = Column(Text, nullable=True)
    sequence_no = Column(Integer, nullable=True)
    field_id = Column(Text, nullable=True)
    pad_id = Column(Text, nullable=True)

    lithology = Column(Text, nullable=True)
    formation_name = Column(Text, nullable=True)
    kg = Column(Float, nullable=True)
    hole_diameter_mm = Column(Float, nullable=True)
    bha_diameter_mm = Column(Float, nullable=True)
    inclination_deg = Column(Float, nullable=True)

    operation_label = Column(Text, nullable=True)
    risk_level_id = Column(Integer, nullable=True)
    markup_code = Column(Text, nullable=True)
    operation_code = Column(Text, nullable=True)
    operation_id = Column(Integer, ForeignKey("operations.operation_id"), nullable=True)
    event_codes = Column(ARRAY(Text), nullable=True)
    event_type_ids = Column(ARRAY(Integer), nullable=True)

    start_time = Column(DateTime(timezone=True), nullable=True)
    end_time = Column(DateTime(timezone=True), nullable=True)
    top_md = Column(Float, nullable=True)
    base_md = Column(Float, nullable=True)
    work_description = Column(Text, nullable=True)
    final_note = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return (
            f"<MarkupFileRow(markup_row_id={self.markup_row_id}, "
            f"source_file='{self.source_file}')>"
        )
