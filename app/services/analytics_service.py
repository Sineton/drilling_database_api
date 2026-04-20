"""
Analytics service with draft business logic for anomalies and summaries.
"""
from datetime import datetime
from typing import Optional, List

from sqlalchemy import func, case
from sqlalchemy.orm import Session

from ..models import GtiSnapshot, GtiLog, Well, Wellbore, Operation, Event, EventType


class AnalyticsService:
    def __init__(self, db: Session):
        self.db = db

    def get_anomalies(
        self,
        well_number: str,
        date_from: Optional[datetime],
        date_to: Optional[datetime],
        min_score: int = 2,
        limit: int = 200,
        offset: int = 0,
    ) -> tuple[int, List[dict]]:
        query = (
            self.db.query(
                GtiSnapshot.time_utc,
                GtiSnapshot.dmea,
                GtiSnapshot.tqa,
                GtiSnapshot.hkla,
                GtiSnapshot.sppa,
                GtiSnapshot.mfia,
                GtiSnapshot.mfoa,
                GtiSnapshot.gasa,
                GtiSnapshot.quality_flags,
                Operation.operation_name,
                EventType.event_code,
                EventType.event_name,
            )
            .join(GtiLog, GtiLog.log_id == GtiSnapshot.log_id)
            .join(Wellbore, Wellbore.wellbore_id == GtiLog.wellbore_id)
            .join(Well, Well.well_id == Wellbore.well_id)
            .outerjoin(Operation, Operation.operation_id == GtiSnapshot.operation_id)
            .outerjoin(Event, Event.event_id == GtiSnapshot.event_id)
            .outerjoin(EventType, EventType.event_type_id == Event.event_type_id)
            .filter(Well.well_number == well_number)
            .order_by(GtiSnapshot.time_utc.desc())
        )

        if date_from:
            query = query.filter(GtiSnapshot.time_utc >= date_from)
        if date_to:
            query = query.filter(GtiSnapshot.time_utc <= date_to)

        rows = query.limit(5000).all()
        items: List[dict] = []
        for row in rows:
            reasons = []
            score = 0

            if row.tqa is not None and row.tqa >= 12:
                score += 1
                reasons.append("Высокий крутящий момент")
            if row.hkla is not None and row.hkla >= 180:
                score += 1
                reasons.append("Высокая нагрузка на крюке")
            if row.sppa is not None and row.sppa >= 220:
                score += 1
                reasons.append("Высокое давление в стояке")
            if row.mfia is not None and row.mfoa is not None and abs(row.mfia - row.mfoa) >= 3:
                score += 1
                reasons.append("Дисбаланс расхода вход/выход")
            if row.gasa is not None and row.gasa >= 2:
                score += 1
                reasons.append("Повышенные газопоказания")
            if row.quality_flags is not None and row.quality_flags > 0:
                score += 1
                reasons.append("Флаги качества данных")
            if row.event_code is not None:
                score += 2
                reasons.append("Привязка к событию/осложнению")

            if score >= min_score:
                items.append(
                    {
                        "time_utc": row.time_utc,
                        "well_number": well_number,
                        "depth_md": row.dmea,
                        "operation": row.operation_name,
                        "torque": row.tqa,
                        "hookload": row.hkla,
                        "spp": row.sppa,
                        "flow_in": row.mfia,
                        "flow_out": row.mfoa,
                        "gas": row.gasa,
                        "event_code": row.event_code,
                        "event_name": row.event_name,
                        "anomaly_score": score,
                        "anomaly_reasons": reasons,
                    }
                )

        total = len(items)
        paginated = items[offset: offset + limit]
        return total, paginated

    def get_field_summary(self, field: str) -> Optional[dict]:
        agg = (
            self.db.query(
                Well.field.label("field"),
                func.count(func.distinct(Well.well_id)).label("wells_count"),
                func.count(func.distinct(Wellbore.wellbore_id)).label("wellbores_count"),
                func.count(func.distinct(GtiLog.log_id)).label("logs_count"),
                func.count(GtiSnapshot.snapshot_id).label("snapshots_count"),
                func.count(func.distinct(Event.event_id)).label("events_count"),
                func.min(GtiSnapshot.time_utc).label("first_timestamp"),
                func.max(GtiSnapshot.time_utc).label("last_timestamp"),
                func.avg(case((GtiSnapshot.tqa.is_not(None), 1.0), else_=0.0)).label("fill_tqa"),
                func.avg(case((GtiSnapshot.hkla.is_not(None), 1.0), else_=0.0)).label("fill_hkla"),
                func.avg(case((GtiSnapshot.sppa.is_not(None), 1.0), else_=0.0)).label("fill_sppa"),
                func.avg(case((GtiSnapshot.mfia.is_not(None), 1.0), else_=0.0)).label("fill_mfia"),
                func.avg(case((GtiSnapshot.mfoa.is_not(None), 1.0), else_=0.0)).label("fill_mfoa"),
                func.avg(case((GtiSnapshot.gasa.is_not(None), 1.0), else_=0.0)).label("fill_gasa"),
            )
            .outerjoin(Wellbore, Wellbore.well_id == Well.well_id)
            .outerjoin(GtiLog, GtiLog.wellbore_id == Wellbore.wellbore_id)
            .outerjoin(GtiSnapshot, GtiSnapshot.log_id == GtiLog.log_id)
            .outerjoin(Event, Event.wellbore_id == Wellbore.wellbore_id)
            .filter(Well.field == field)
            .group_by(Well.field)
            .first()
        )

        if not agg:
            return None

        channel_fill_rates = {
            "tqa": float(agg.fill_tqa or 0.0),
            "hkla": float(agg.fill_hkla or 0.0),
            "sppa": float(agg.fill_sppa or 0.0),
            "mfia": float(agg.fill_mfia or 0.0),
            "mfoa": float(agg.fill_mfoa or 0.0),
            "gasa": float(agg.fill_gasa or 0.0),
        }

        return {
            "field": agg.field,
            "wells_count": int(agg.wells_count or 0),
            "wellbores_count": int(agg.wellbores_count or 0),
            "logs_count": int(agg.logs_count or 0),
            "snapshots_count": int(agg.snapshots_count or 0),
            "events_count": int(agg.events_count or 0),
            "first_timestamp": agg.first_timestamp,
            "last_timestamp": agg.last_timestamp,
            "channel_fill_rates": channel_fill_rates,
        }
