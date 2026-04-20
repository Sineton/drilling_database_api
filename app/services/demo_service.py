"""
Demo service for overview and well parameter scenarios.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


DEMO_TARGET_TIME = datetime(2024, 8, 5, 20, 12, 0)
DEMO_WINDOW_START = datetime(2024, 8, 5, 20, 10, 0)
DEMO_WINDOW_END = datetime(2024, 8, 5, 20, 15, 0)

DEFAULT_PARAMETER_KEYS = [
    "depth_md",
    "rop",
    "wob",
    "rpm",
    "torque",
    "spp",
    "flow_in",
    "flow_out",
    "gas",
    "hookload",
]

PARAMETER_ALIASES = {
    "depth_md": "depth_md",
    "depth": "depth_md",
    "dmea": "depth_md",
    "tvd": "tvd",
    "rop": "rop",
    "ropa": "rop",
    "wob": "wob",
    "woba": "wob",
    "rpm": "rpm",
    "rpma": "rpm",
    "torque": "torque",
    "trq": "torque",
    "tqa": "torque",
    "spp": "spp",
    "sppa": "spp",
    "flow_in": "flow_in",
    "mfia": "flow_in",
    "mfip": "flow_in",
    "flow_out": "flow_out",
    "mfoa": "flow_out",
    "mfop": "flow_out",
    "gas": "gas",
    "gasa": "gas",
    "hookload": "hookload",
    "hkla": "hookload",
    "mud_density_in": "mud_density_in",
    "mdia": "mud_density_in",
    "mud_density_out": "mud_density_out",
    "mdoa": "mud_density_out",
    "mud_temp_in": "mud_temp_in",
    "mtia": "mud_temp_in",
    "mud_temp_out": "mud_temp_out",
    "mtoa": "mud_temp_out",
}

PARAMETER_COLUMNS = {
    "depth_md": "gs.dmea AS depth_md",
    "tvd": "gs.tvd AS tvd",
    "rop": "gs.ropa AS rop",
    "wob": "gs.woba AS wob",
    "rpm": "gs.rpma AS rpm",
    "torque": "gs.tqa AS torque",
    "spp": "gs.sppa AS spp",
    "flow_in": "gs.mfia AS flow_in",
    "flow_out": "gs.mfoa AS flow_out",
    "gas": "gs.gasa AS gas",
    "hookload": "gs.hkla AS hookload",
    "mud_density_in": "gs.mdia AS mud_density_in",
    "mud_density_out": "gs.mdoa AS mud_density_out",
    "mud_temp_in": "gs.mtia AS mud_temp_in",
    "mud_temp_out": "gs.mtoa AS mud_temp_out",
}

AGGREGATED_PARAMETER_COLUMNS = {
    "depth_md": "AVG(gs.dmea) AS depth_md",
    "tvd": "AVG(gs.tvd) AS tvd",
    "rop": "AVG(gs.ropa) AS rop",
    "wob": "AVG(gs.woba) AS wob",
    "rpm": "AVG(gs.rpma) AS rpm",
    "torque": "AVG(gs.tqa) AS torque",
    "spp": "AVG(gs.sppa) AS spp",
    "flow_in": "AVG(gs.mfia) AS flow_in",
    "flow_out": "AVG(gs.mfoa) AS flow_out",
    "gas": "AVG(gs.gasa) AS gas",
    "hookload": "AVG(gs.hkla) AS hookload",
    "mud_density_in": "AVG(gs.mdia) AS mud_density_in",
    "mud_density_out": "AVG(gs.mdoa) AS mud_density_out",
    "mud_temp_in": "AVG(gs.mtia) AS mud_temp_in",
    "mud_temp_out": "AVG(gs.mtoa) AS mud_temp_out",
}

BUCKET_EXPRESSIONS = {
    "minute": "date_trunc('minute', gs.time_utc)",
    "5min": "date_trunc('hour', gs.time_utc) + floor(extract(minute from gs.time_utc) / 5) * interval '5 minutes'",
    "hour": "date_trunc('hour', gs.time_utc)",
}


class DemoService:
    """Service for demo scenario endpoints."""

    def __init__(self, db: Session):
        self.db = db

    def get_wells_overview(
        self,
        *,
        target_time: Optional[datetime] = None,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
        field: Optional[str] = None,
    ) -> dict:
        target_time, window_start, window_end = self._resolve_time_window(
            target_time=target_time,
            window_start=window_start,
            window_end=window_end,
        )

        rows = self._fetch_overview_rows(
            target_time=target_time,
            window_start=window_start,
            window_end=window_end,
            field=field,
        )
        items = [self._build_overview_item(row) for row in rows]
        return {
            "target_time": target_time,
            "window_start": window_start,
            "window_end": window_end,
            "total": len(items),
            "items": items,
        }

    def get_well_parameters(
        self,
        *,
        well_number: str,
        target_time: Optional[datetime] = None,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
        params: Optional[Iterable[str]] = None,
        bucket: str = "minute",
    ) -> dict:
        target_time, window_start, window_end = self._resolve_time_window(
            target_time=target_time,
            window_start=window_start,
            window_end=window_end,
        )
        bucket = bucket.lower().strip()
        if bucket not in {"raw", *BUCKET_EXPRESSIONS.keys()}:
            allowed = ", ".join(["raw", *BUCKET_EXPRESSIONS.keys()])
            raise ValueError(f"Unsupported bucket '{bucket}'. Allowed values: {allowed}")

        canonical_params = self._normalize_params(params)
        context_row = self._fetch_well_context(
            well_number=well_number,
            target_time=target_time,
            window_start=window_start,
            window_end=window_end,
        )
        if not context_row:
            return {
                "target_time": target_time,
                "window_start": window_start,
                "window_end": window_end,
                "bucket": bucket,
                "requested_params": canonical_params,
                "points_count": 0,
                "well": {"well_number": well_number},
                "latest": {},
                "operation": None,
                "warning": None,
                "geology": None,
                "stats": {},
                "points": [],
            }

        points = self._fetch_parameter_points(
            wellbore_id=context_row["wellbore_id"],
            window_start=window_start,
            window_end=window_end,
            params=canonical_params,
            bucket=bucket,
        )
        latest = points[-1] if points else self._build_latest_snapshot(context_row)

        return {
            "target_time": target_time,
            "window_start": window_start,
            "window_end": window_end,
            "bucket": bucket,
            "requested_params": canonical_params,
            "points_count": len(points),
            "well": {
                "well_id": context_row["well_id"],
                "well_number": context_row["well_number"],
                "well_name": context_row["well_name"],
                "field_name": context_row["field_name"],
                "pad_number": context_row["pad_number"],
                "wellbore_id": context_row["wellbore_id"],
                "wellbore_number": context_row["wellbore_number"],
            },
            "latest": latest,
            "operation": self._build_operation_context(context_row),
            "warning": self._build_warning_context(context_row),
            "geology": self._build_geology_context(context_row),
            "stats": self._build_stats(points, canonical_params),
            "points": points,
        }

    def _resolve_time_window(
        self,
        *,
        target_time: Optional[datetime],
        window_start: Optional[datetime],
        window_end: Optional[datetime],
    ) -> tuple[datetime, datetime, datetime]:
        if target_time is None and window_start is None and window_end is None:
            return DEMO_TARGET_TIME, DEMO_WINDOW_START, DEMO_WINDOW_END

        if target_time is None:
            if window_start and window_end:
                target_time = window_start + (window_end - window_start) / 2
            elif window_end:
                target_time = window_end
            else:
                target_time = window_start

        if window_start is None:
            window_start = target_time - timedelta(minutes=2)
        if window_end is None:
            window_end = target_time + timedelta(minutes=3)

        if window_start > window_end:
            raise ValueError("window_start must be less than or equal to window_end")

        return target_time, window_start, window_end

    def _normalize_dt(self, value: datetime) -> tuple[datetime, datetime]:
        if value.tzinfo is None:
            aware_value = value.replace(tzinfo=timezone.utc)
            return value, aware_value
        aware_value = value.astimezone(timezone.utc)
        naive_value = aware_value.replace(tzinfo=None)
        return naive_value, aware_value

    def _normalize_params(self, params: Optional[Iterable[str]]) -> List[str]:
        if not params:
            return list(DEFAULT_PARAMETER_KEYS)

        normalized: List[str] = []
        for raw_value in params:
            if raw_value is None:
                continue
            for item in str(raw_value).split(","):
                cleaned = item.strip().lower()
                if not cleaned:
                    continue
                canonical = PARAMETER_ALIASES.get(cleaned)
                if canonical is None:
                    allowed = ", ".join(sorted(PARAMETER_ALIASES.keys()))
                    raise ValueError(f"Unsupported parameter '{item}'. Allowed values: {allowed}")
                if canonical not in normalized:
                    normalized.append(canonical)

        return normalized or list(DEFAULT_PARAMETER_KEYS)

    def _fetch_overview_rows(
        self,
        *,
        target_time: datetime,
        window_start: datetime,
        window_end: datetime,
        field: Optional[str],
        well_number: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        target_time_ts, target_time_tz = self._normalize_dt(target_time)
        window_start_ts, _ = self._normalize_dt(window_start)
        window_end_ts, _ = self._normalize_dt(window_end)
        field_pattern = f"%{field.strip()}%" if field else None

        sql = text(
            """
            WITH active_wells AS (
                SELECT DISTINCT
                    w.well_id,
                    w.well_number,
                    w.well_name,
                    COALESCE(w.field_name, w.field) AS field_name,
                    w.pad_number,
                    wb.wellbore_id,
                    wb.wellbore_number
                FROM wells w
                JOIN wellbores wb ON wb.well_id = w.well_id
                JOIN gti_logs gl ON gl.wellbore_id = wb.wellbore_id
                WHERE :target_time_ts BETWEEN gl.start_time AND gl.end_time
                  AND (:field_pattern IS NULL OR COALESCE(w.field_name, w.field) ILIKE :field_pattern)
                  AND (:well_number IS NULL OR w.well_number = :well_number)
            )
            SELECT
                aw.well_id,
                aw.well_number,
                aw.well_name,
                aw.field_name,
                aw.pad_number,
                aw.wellbore_id,
                aw.wellbore_number,
                snap.snapshot_time,
                snap.depth_md,
                snap.tvd,
                snap.rop,
                snap.wob,
                snap.rpm,
                snap.torque,
                snap.spp,
                snap.flow_in,
                snap.flow_out,
                snap.gas,
                snap.hookload,
                COALESCE(ao.operation_code, mr.operation_code) AS operation_code,
                COALESCE(ao.operation_label, mr.operation_label) AS operation_name,
                CASE
                    WHEN ao.actual_operation_id IS NOT NULL THEN 'actual_operations'
                    WHEN mr.markup_row_id IS NOT NULL THEN 'markup_file_rows'
                    ELSE NULL
                END AS operation_source,
                COALESCE(ao.description, mr.work_description) AS operation_description,
                COALESCE(ao.start_time, mr.start_time) AS operation_start_time,
                COALESCE(ao.end_time, mr.end_time) AS operation_end_time,
                COALESCE(ao.depth_from_m, mr.top_md) AS operation_depth_from_m,
                COALESCE(ao.depth_to_m, mr.base_md) AS operation_depth_to_m,
                ev.event_id,
                ev.event_code,
                ev.event_name,
                ev.severity,
                ev.start_time AS event_start_time,
                ev.end_time AS event_end_time,
                ev.start_md AS event_start_md,
                ev.end_md AS event_end_md,
                gi.top_md AS geology_top_md,
                gi.base_md AS geology_base_md,
                gi.formation_name,
                gi.lithology,
                gi.kg
            FROM active_wells aw
            LEFT JOIN LATERAL (
                SELECT
                    gs.time_utc AS snapshot_time,
                    gs.dmea AS depth_md,
                    gs.tvd AS tvd,
                    gs.ropa AS rop,
                    gs.woba AS wob,
                    gs.rpma AS rpm,
                    gs.tqa AS torque,
                    gs.sppa AS spp,
                    gs.mfia AS flow_in,
                    gs.mfoa AS flow_out,
                    gs.gasa AS gas,
                    gs.hkla AS hookload
                FROM gti_logs gl
                JOIN gti_snapshots gs ON gs.log_id = gl.log_id
                WHERE gl.wellbore_id = aw.wellbore_id
                  AND gs.time_utc BETWEEN :window_start_ts AND :window_end_ts
                ORDER BY gs.time_utc DESC
                LIMIT 1
            ) snap ON TRUE
            LEFT JOIN LATERAL (
                SELECT *
                FROM actual_operations ao
                WHERE ao.wellbore_id = aw.wellbore_id
                  AND :target_time_tz BETWEEN ao.start_time AND COALESCE(ao.end_time, ao.start_time)
                ORDER BY ao.start_time DESC NULLS LAST
                LIMIT 1
            ) ao ON TRUE
            LEFT JOIN LATERAL (
                SELECT *
                FROM markup_file_rows mr
                WHERE mr.wellbore_id = aw.wellbore_id
                  AND :target_time_tz BETWEEN mr.start_time AND COALESCE(mr.end_time, mr.start_time)
                ORDER BY mr.start_time DESC NULLS LAST
                LIMIT 1
            ) mr ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    e.event_id,
                    et.event_code,
                    et.event_name,
                    et.severity,
                    e.start_time,
                    e.end_time,
                    e.start_md,
                    e.end_md
                FROM events e
                JOIN event_types et ON et.event_type_id = e.event_type_id
                WHERE e.wellbore_id = aw.wellbore_id
                  AND :target_time_ts BETWEEN e.start_time AND COALESCE(e.end_time, e.start_time)
                ORDER BY et.severity DESC, e.start_time DESC NULLS LAST
                LIMIT 1
            ) ev ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    gi.top_md,
                    gi.base_md,
                    gi.formation_name,
                    gi.lithology,
                    gi.kg
                FROM geology_intervals gi
                WHERE gi.wellbore_id = aw.wellbore_id
                  AND snap.depth_md IS NOT NULL
                  AND snap.depth_md BETWEEN gi.top_md AND gi.base_md
                ORDER BY gi.top_md DESC
                LIMIT 1
            ) gi ON TRUE
            ORDER BY aw.well_number, aw.wellbore_number
            """
        )

        rows = self.db.execute(
            sql,
            {
                "target_time_ts": target_time_ts,
                "target_time_tz": target_time_tz,
                "window_start_ts": window_start_ts,
                "window_end_ts": window_end_ts,
                "field_pattern": field_pattern,
                "well_number": well_number,
            },
        ).mappings()
        return [dict(row) for row in rows]

    def _fetch_well_context(
        self,
        *,
        well_number: str,
        target_time: datetime,
        window_start: datetime,
        window_end: datetime,
    ) -> Optional[Dict[str, Any]]:
        rows = self._fetch_overview_rows(
            target_time=target_time,
            window_start=window_start,
            window_end=window_end,
            field=None,
            well_number=well_number,
        )
        if rows:
            return rows[0]

        target_time_ts, target_time_tz = self._normalize_dt(target_time)
        window_start_ts, _ = self._normalize_dt(window_start)
        window_end_ts, _ = self._normalize_dt(window_end)

        sql = text(
            """
            WITH candidate_well AS (
                SELECT
                    w.well_id,
                    w.well_number,
                    w.well_name,
                    COALESCE(w.field_name, w.field) AS field_name,
                    w.pad_number,
                    wb.wellbore_id,
                    wb.wellbore_number
                FROM wells w
                JOIN wellbores wb ON wb.well_id = w.well_id
                WHERE w.well_number = :well_number
                ORDER BY wb.wellbore_number
                LIMIT 1
            )
            SELECT
                cw.well_id,
                cw.well_number,
                cw.well_name,
                cw.field_name,
                cw.pad_number,
                cw.wellbore_id,
                cw.wellbore_number,
                snap.snapshot_time,
                snap.depth_md,
                snap.tvd,
                snap.rop,
                snap.wob,
                snap.rpm,
                snap.torque,
                snap.spp,
                snap.flow_in,
                snap.flow_out,
                snap.gas,
                snap.hookload,
                COALESCE(ao.operation_code, mr.operation_code) AS operation_code,
                COALESCE(ao.operation_label, mr.operation_label) AS operation_name,
                CASE
                    WHEN ao.actual_operation_id IS NOT NULL THEN 'actual_operations'
                    WHEN mr.markup_row_id IS NOT NULL THEN 'markup_file_rows'
                    ELSE NULL
                END AS operation_source,
                COALESCE(ao.description, mr.work_description) AS operation_description,
                COALESCE(ao.start_time, mr.start_time) AS operation_start_time,
                COALESCE(ao.end_time, mr.end_time) AS operation_end_time,
                COALESCE(ao.depth_from_m, mr.top_md) AS operation_depth_from_m,
                COALESCE(ao.depth_to_m, mr.base_md) AS operation_depth_to_m,
                ev.event_id,
                ev.event_code,
                ev.event_name,
                ev.severity,
                ev.start_time AS event_start_time,
                ev.end_time AS event_end_time,
                ev.start_md AS event_start_md,
                ev.end_md AS event_end_md,
                gi.top_md AS geology_top_md,
                gi.base_md AS geology_base_md,
                gi.formation_name,
                gi.lithology,
                gi.kg
            FROM candidate_well cw
            LEFT JOIN LATERAL (
                SELECT
                    gs.time_utc AS snapshot_time,
                    gs.dmea AS depth_md,
                    gs.tvd AS tvd,
                    gs.ropa AS rop,
                    gs.woba AS wob,
                    gs.rpma AS rpm,
                    gs.tqa AS torque,
                    gs.sppa AS spp,
                    gs.mfia AS flow_in,
                    gs.mfoa AS flow_out,
                    gs.gasa AS gas,
                    gs.hkla AS hookload
                FROM gti_logs gl
                JOIN gti_snapshots gs ON gs.log_id = gl.log_id
                WHERE gl.wellbore_id = cw.wellbore_id
                  AND gs.time_utc BETWEEN :window_start_ts AND :window_end_ts
                ORDER BY gs.time_utc DESC
                LIMIT 1
            ) snap ON TRUE
            LEFT JOIN LATERAL (
                SELECT *
                FROM actual_operations ao
                WHERE ao.wellbore_id = cw.wellbore_id
                  AND :target_time_tz BETWEEN ao.start_time AND COALESCE(ao.end_time, ao.start_time)
                ORDER BY ao.start_time DESC NULLS LAST
                LIMIT 1
            ) ao ON TRUE
            LEFT JOIN LATERAL (
                SELECT *
                FROM markup_file_rows mr
                WHERE mr.wellbore_id = cw.wellbore_id
                  AND :target_time_tz BETWEEN mr.start_time AND COALESCE(mr.end_time, mr.start_time)
                ORDER BY mr.start_time DESC NULLS LAST
                LIMIT 1
            ) mr ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    e.event_id,
                    et.event_code,
                    et.event_name,
                    et.severity,
                    e.start_time,
                    e.end_time,
                    e.start_md,
                    e.end_md
                FROM events e
                JOIN event_types et ON et.event_type_id = e.event_type_id
                WHERE e.wellbore_id = cw.wellbore_id
                  AND :target_time_ts BETWEEN e.start_time AND COALESCE(e.end_time, e.start_time)
                ORDER BY et.severity DESC, e.start_time DESC NULLS LAST
                LIMIT 1
            ) ev ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    gi.top_md,
                    gi.base_md,
                    gi.formation_name,
                    gi.lithology,
                    gi.kg
                FROM geology_intervals gi
                WHERE gi.wellbore_id = cw.wellbore_id
                  AND snap.depth_md IS NOT NULL
                  AND snap.depth_md BETWEEN gi.top_md AND gi.base_md
                ORDER BY gi.top_md DESC
                LIMIT 1
            ) gi ON TRUE
            """
        )

        row = self.db.execute(
            sql,
            {
                "well_number": well_number,
                "target_time_ts": target_time_ts,
                "target_time_tz": target_time_tz,
                "window_start_ts": window_start_ts,
                "window_end_ts": window_end_ts,
            },
        ).mappings().first()
        return dict(row) if row else None

    def _fetch_parameter_points(
        self,
        *,
        wellbore_id: int,
        window_start: datetime,
        window_end: datetime,
        params: List[str],
        bucket: str,
    ) -> List[Dict[str, Any]]:
        window_start_ts, _ = self._normalize_dt(window_start)
        window_end_ts, _ = self._normalize_dt(window_end)

        if bucket == "raw":
            select_columns = ",\n                    ".join(PARAMETER_COLUMNS[param] for param in params)
            sql = text(
                f"""
                SELECT
                    gs.time_utc AS time_utc,
                    {select_columns}
                FROM gti_logs gl
                JOIN gti_snapshots gs ON gs.log_id = gl.log_id
                WHERE gl.wellbore_id = :wellbore_id
                  AND gs.time_utc BETWEEN :window_start_ts AND :window_end_ts
                ORDER BY gs.time_utc
                """
            )
        else:
            select_columns = ",\n                    ".join(
                AGGREGATED_PARAMETER_COLUMNS[param] for param in params
            )
            sql = text(
                f"""
                SELECT
                    {BUCKET_EXPRESSIONS[bucket]} AS time_utc,
                    {select_columns}
                FROM gti_logs gl
                JOIN gti_snapshots gs ON gs.log_id = gl.log_id
                WHERE gl.wellbore_id = :wellbore_id
                  AND gs.time_utc BETWEEN :window_start_ts AND :window_end_ts
                GROUP BY 1
                ORDER BY 1
                """
            )

        rows = self.db.execute(
            sql,
            {
                "wellbore_id": wellbore_id,
                "window_start_ts": window_start_ts,
                "window_end_ts": window_end_ts,
            },
        ).mappings()
        return [dict(row) for row in rows]

    def _build_overview_item(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "well_id": row["well_id"],
            "well_number": row["well_number"],
            "well_name": row.get("well_name"),
            "field_name": row.get("field_name"),
            "pad_number": row.get("pad_number"),
            "wellbore_id": row["wellbore_id"],
            "wellbore_number": row["wellbore_number"],
            "snapshot_time": row.get("snapshot_time"),
            "depth_md": row.get("depth_md"),
            "tvd": row.get("tvd"),
            "rop": row.get("rop"),
            "wob": row.get("wob"),
            "rpm": row.get("rpm"),
            "torque": row.get("torque"),
            "spp": row.get("spp"),
            "flow_in": row.get("flow_in"),
            "flow_out": row.get("flow_out"),
            "gas": row.get("gas"),
            "hookload": row.get("hookload"),
            "operation": self._build_operation_context(row),
            "warning": self._build_warning_context(row),
            "geology": self._build_geology_context(row),
        }

    def _build_operation_context(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not row.get("operation_code") and not row.get("operation_name"):
            return None
        return {
            "operation_code": row.get("operation_code"),
            "operation_name": row.get("operation_name"),
            "source": row.get("operation_source"),
            "description": row.get("operation_description"),
            "start_time": row.get("operation_start_time"),
            "end_time": row.get("operation_end_time"),
            "depth_from_m": row.get("operation_depth_from_m"),
            "depth_to_m": row.get("operation_depth_to_m"),
        }

    def _build_warning_context(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not row.get("event_id"):
            return None
        return {
            "event_id": row.get("event_id"),
            "event_code": row.get("event_code"),
            "event_name": row.get("event_name"),
            "severity": row.get("severity"),
            "start_time": row.get("event_start_time"),
            "end_time": row.get("event_end_time"),
            "start_md": row.get("event_start_md"),
            "end_md": row.get("event_end_md"),
        }

    def _build_geology_context(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if (
            row.get("geology_top_md") is None
            and row.get("geology_base_md") is None
            and not row.get("formation_name")
            and not row.get("lithology")
        ):
            return None
        return {
            "top_md": row.get("geology_top_md"),
            "base_md": row.get("geology_base_md"),
            "formation_name": row.get("formation_name"),
            "lithology": row.get("lithology"),
            "kg": row.get("kg"),
        }

    def _build_latest_snapshot(self, row: Dict[str, Any]) -> Dict[str, Any]:
        latest = {"time_utc": row.get("snapshot_time")}
        for key in DEFAULT_PARAMETER_KEYS + ["tvd"]:
            latest[key] = row.get(key)
        return latest

    def _build_stats(self, points: List[Dict[str, Any]], params: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        stats: Dict[str, Dict[str, Optional[float]]] = {}
        for param in params:
            values = [point[param] for point in points if point.get(param) is not None]
            if not values:
                stats[param] = {"min": None, "max": None, "avg": None}
                continue
            stats[param] = {
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
            }
        return stats
