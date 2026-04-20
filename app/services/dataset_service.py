"""
Draft dataset builder service for stuck pipe ML training.
"""
from datetime import timedelta
from statistics import mean, pstdev
from typing import Optional, List

from sqlalchemy.orm import Session

from ..models import Well, Wellbore, GtiLog, GtiSnapshot, Event, EventType


class DatasetService:
    def __init__(self, db: Session):
        self.db = db

    @staticmethod
    def _safe_mean(values: List[float]) -> Optional[float]:
        return mean(values) if values else None

    @staticmethod
    def _safe_std(values: List[float]) -> Optional[float]:
        return pstdev(values) if len(values) > 1 else 0.0 if values else None

    def _build_window_features(
        self,
        well_number: str,
        wellbore: Wellbore,
        event_id: Optional[int],
        window_start,
        window_end,
        target_label: int,
    ) -> Optional[dict]:
        rows = (
            self.db.query(
                GtiSnapshot.tqa,
                GtiSnapshot.hkla,
                GtiSnapshot.sppa,
                GtiSnapshot.mfia,
                GtiSnapshot.mfoa,
                GtiSnapshot.gasa,
                GtiSnapshot.dmea,
                GtiSnapshot.operation_id,
            )
            .join(GtiLog, GtiLog.log_id == GtiSnapshot.log_id)
            .filter(
                GtiLog.wellbore_id == wellbore.wellbore_id,
                GtiSnapshot.time_utc >= window_start,
                GtiSnapshot.time_utc <= window_end,
            )
            .all()
        )

        if not rows:
            return None

        torque = [float(r.tqa) for r in rows if r.tqa is not None]
        hookload = [float(r.hkla) for r in rows if r.hkla is not None]
        spp = [float(r.sppa) for r in rows if r.sppa is not None]
        gas = [float(r.gasa) for r in rows if r.gasa is not None]
        depth = [float(r.dmea) for r in rows if r.dmea is not None]
        flow_imbalance = [
            abs(float(r.mfia) - float(r.mfoa))
            for r in rows
            if r.mfia is not None and r.mfoa is not None
        ]

        operation_ids = [r.operation_id for r in rows if r.operation_id is not None]
        operation_mode = max(set(operation_ids), key=operation_ids.count) if operation_ids else None

        return {
            "well_number": well_number,
            "wellbore_id": wellbore.wellbore_id,
            "event_id": event_id,
            "target_label": target_label,
            "window_start": window_start,
            "window_end": window_end,
            "operation_id_mode": operation_mode,
            "diameter_mm": wellbore.diameter_mm,
            "azimuth_avg": wellbore.azimuth_avg,
            "inclination_avg": wellbore.inclination_avg,
            "f_torque_mean": self._safe_mean(torque),
            "f_torque_std": self._safe_std(torque),
            "f_hookload_mean": self._safe_mean(hookload),
            "f_spp_mean": self._safe_mean(spp),
            "f_flow_imbalance_mean": self._safe_mean(flow_imbalance),
            "f_gas_mean": self._safe_mean(gas),
            "f_depth_mean": self._safe_mean(depth),
            "points_count": len(rows),
        }

    def build_stuck_pipe_dataset(
        self,
        field: Optional[str],
        well_numbers: Optional[List[str]],
        before_minutes: int,
        after_minutes: int,
        include_negative: bool,
        negatives_per_positive: int,
        max_samples: int,
    ) -> dict:
        event_query = (
            self.db.query(Event, Well, Wellbore)
            .join(EventType, EventType.event_type_id == Event.event_type_id)
            .join(Wellbore, Wellbore.wellbore_id == Event.wellbore_id)
            .join(Well, Well.well_id == Wellbore.well_id)
            .filter(EventType.event_code == "stuck_pipe")
            .order_by(Event.start_time.desc())
        )

        if field:
            event_query = event_query.filter(Well.field == field)
        if well_numbers:
            event_query = event_query.filter(Well.well_number.in_(well_numbers))

        events = event_query.limit(max_samples).all()
        samples: List[dict] = []
        positives = 0
        negatives = 0

        for event, well, wellbore in events:
            if not event.start_time:
                continue
            window_start = event.start_time - timedelta(minutes=before_minutes)
            window_end = event.start_time + timedelta(minutes=after_minutes)
            pos_row = self._build_window_features(
                well_number=well.well_number,
                wellbore=wellbore,
                event_id=event.event_id,
                window_start=window_start,
                window_end=window_end,
                target_label=1,
            )
            if pos_row:
                samples.append(pos_row)
                positives += 1

            if include_negative and negatives_per_positive > 0:
                for i in range(negatives_per_positive):
                    shift_minutes = (before_minutes + after_minutes + 30) * (i + 1)
                    neg_end = event.start_time - timedelta(minutes=shift_minutes)
                    neg_start = neg_end - timedelta(minutes=(before_minutes + after_minutes))
                    neg_row = self._build_window_features(
                        well_number=well.well_number,
                        wellbore=wellbore,
                        event_id=None,
                        window_start=neg_start,
                        window_end=neg_end,
                        target_label=0,
                    )
                    if neg_row:
                        samples.append(neg_row)
                        negatives += 1

            if len(samples) >= max_samples:
                break

        samples = samples[:max_samples]
        return {
            "total_samples": len(samples),
            "positives": positives,
            "negatives": negatives,
            "samples": samples,
        }
