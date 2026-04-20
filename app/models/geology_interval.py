"""
Geology interval model.
"""
from sqlalchemy import Column, Integer, Float, Text, DateTime, ForeignKey, func

from ..database import Base


class GeologyInterval(Base):
    """Геологические интервалы по стволу."""
    __tablename__ = "geology_intervals"

    interval_id = Column(Integer, primary_key=True, index=True)
    wellbore_id = Column(
        Integer,
        ForeignKey("wellbores.wellbore_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    top_md = Column(Float, nullable=False)
    base_md = Column(Float, nullable=False)
    kg = Column(Float, nullable=True)
    lithology = Column(Text, nullable=True)
    formation_name = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return (
            f"<GeologyInterval(interval_id={self.interval_id}, "
            f"wellbore_id={self.wellbore_id}, top_md={self.top_md}, base_md={self.base_md})>"
        )
