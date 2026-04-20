"""
Service for creating events from supervisor journal tables (sv_*).
"""
from datetime import date, datetime, time, timedelta
from typing import Optional, Dict, Any, List, Tuple

from sqlalchemy.orm import Session

from ..models import (
    Well,
    Wellbore,
    Event,
    EventType,
    SvDailyReport,
    SvDailyOperation,
    SvNpvBalance,
)


FLAG_TO_EVENT_TYPE: Dict[str, Dict[str, Any]] = {
    "stuck_pipe": {"code": "stuck_pipe", "name": "Прихват", "severity": 3, "is_complication": True, "target_label": 1},
    "circulation_loss": {"code": "circulation_loss", "name": "Потеря циркуляции", "severity": 3, "is_complication": True, "target_label": 2},
    "absorption": {"code": "absorption", "name": "Поглощение", "severity": 2, "is_complication": True, "target_label": 4},
    "drag": {"code": "sv_drag", "name": "Затяжки", "severity": 2, "is_complication": True, "target_label": None},
    "set_down": {"code": "sv_set_down", "name": "Посадки", "severity": 2, "is_complication": True, "target_label": None},
    "pressure_rise": {"code": "sv_pressure_rise", "name": "Рост давления", "severity": 2, "is_complication": True, "target_label": None},
    "geological_complication": {"code": "sv_geological_complication", "name": "Геологическое осложнение", "severity": 3, "is_complication": True, "target_label": None},
    "equipment_failure": {"code": "sv_equipment_failure", "name": "Отказ оборудования", "severity": 2, "is_complication": False, "target_label": None},
    "mud_motor_plugged": {"code": "sv_mud_motor_plugged", "name": "Забитие ВЗД", "severity": 2, "is_complication": False, "target_label": None},
    "plugging": {"code": "sv_plugging", "name": "Засорение", "severity": 2, "is_complication": False, "target_label": None},
    "additional_work": {"code": "sv_additional_work", "name": "Дополнительные работы", "severity": 1, "is_complication": False, "target_label": None},
    "repair": {"code": "sv_repair", "name": "Ремонт", "severity": 2, "is_complication": False, "target_label": None},
    "npv": {"code": "sv_npv", "name": "НПВ", "severity": 2, "is_complication": False, "target_label": None},
}

PREFERRED_EVENT_CODE_BY_GENERATED: Dict[str, str] = {
    "stuck_pipe": "3.1",
    "circulation_loss": "3.2",
    "absorption": "3.3",
    "sv_drag": "2.1",
    "sv_set_down": "2.2",
    "sv_pressure_rise": "2.5",
    "sv_geological_complication": "2.5",
    "sv_equipment_failure": "2.6",
    "sv_mud_motor_plugged": "2.4",
    "sv_plugging": "2.3",
    "sv_additional_work": "2.6",
    "sv_repair": "2.6",
    "sv_npv": "2.6",
}

EVENT_NAME_KEYWORDS_BY_GENERATED: Dict[str, List[str]] = {
    "stuck_pipe": ["прихват"],
    "circulation_loss": ["потеря циркуляции"],
    "absorption": ["поглощение"],
    "sv_drag": ["затяжка"],
    "sv_set_down": ["посадка"],
    "sv_pressure_rise": ["аномаль", "отклонение параметров"],
    "sv_geological_complication": ["аномаль", "осложнение"],
    "sv_equipment_failure": ["простой", "технологический"],
    "sv_mud_motor_plugged": ["зашламовывание", "запаковка"],
    "sv_plugging": ["запаковка", "зашламовывание"],
    "sv_additional_work": ["простой", "технологический"],
    "sv_repair": ["простой", "технологический"],
    "sv_npv": ["простой", "технологический"],
}

NPV_CATEGORY_TO_FLAG: Dict[str, str] = {
    "допработы": "additional_work",
    "дополнительные работы": "additional_work",
    "неплановыеработы": "npv",
    "неплановые работы": "npv",
    "ремонт": "repair",
}


class SvEventsService:
    def __init__(self, db: Session):
        self.db = db
        self._event_type_cache: Dict[str, EventType] = {}
        self._all_event_types_cache: Optional[List[EventType]] = None

    def _get_all_event_types(self) -> List[EventType]:
        if self._all_event_types_cache is None:
            self._all_event_types_cache = self.db.query(EventType).all()
        return self._all_event_types_cache

    def _event_type_by_code(self, code: str) -> Optional[EventType]:
        if code in self._event_type_cache:
            return self._event_type_cache[code]
        event_type = self.db.query(EventType).filter(EventType.event_code == code).first()
        if event_type:
            self._event_type_cache[code] = event_type
        return event_type

    def _event_type_by_name_keywords(self, keywords: List[str]) -> Optional[EventType]:
        if not keywords:
            return None
        candidates: List[EventType] = []
        for et in self._get_all_event_types():
            name = (et.event_name or "").lower()
            desc = (et.description or "").lower()
            if any((kw in name) or (kw in desc) for kw in keywords):
                candidates.append(et)
        if not candidates:
            return None
        # Prefer top-level taxonomy code (e.g. 2.1 over 2.1.1), then by id
        candidates.sort(key=lambda x: (str(x.event_code).count("."), x.event_type_id))
        return candidates[0]

    def _resolve_event_type(self, item: Dict[str, Any]) -> Tuple[Optional[EventType], bool]:
        code = item["code"]
        event_type = self._event_type_by_code(code)
        if event_type:
            return event_type, False

        # Try known mapping from generated code to taxonomy code in DB
        preferred_code = PREFERRED_EVENT_CODE_BY_GENERATED.get(code)
        if preferred_code:
            event_type = self._event_type_by_code(preferred_code)
            if event_type:
                self._event_type_cache[code] = event_type
                return event_type, False

        # Try resolve by russian semantic keywords in event_name / description
        keywords = EVENT_NAME_KEYWORDS_BY_GENERATED.get(code, [])
        event_type = self._event_type_by_name_keywords(keywords)
        if event_type:
            self._event_type_cache[code] = event_type
            return event_type, False

        # Some DB deployments have stricter event_types schema than SQLAlchemy model.
        # To keep sync robust, fallback to predefined seeded types instead of insert.
        fallback_code = "overflow" if item.get("is_complication") else "normal"
        fallback = self._event_type_by_code(fallback_code)
        if fallback:
            self._event_type_cache[code] = fallback
            return fallback, False

        return None, False

    @staticmethod
    def _build_datetimes(report_date: date, time_from: Optional[time], time_to: Optional[time], duration_minutes: Optional[int]) -> Tuple[datetime, Optional[datetime]]:
        start_dt = datetime.combine(report_date, time_from or time(0, 0))
        if time_to:
            end_dt = datetime.combine(report_date, time_to)
            if end_dt < start_dt:
                end_dt += timedelta(days=1)
            return start_dt, end_dt
        if duration_minutes:
            return start_dt, start_dt + timedelta(minutes=duration_minutes)
        return start_dt, None

    @staticmethod
    def _extract_flag_codes(operation: SvDailyOperation) -> List[str]:
        flags = operation.anomaly_flags or {}
        selected = []
        if isinstance(flags, dict):
            for key, value in flags.items():
                if value and key in FLAG_TO_EVENT_TYPE:
                    selected.append(key)

        if not selected:
            if operation.is_complication:
                selected.append("drag")
            elif operation.is_npv:
                selected.append("npv")
            elif operation.anomaly_severity and operation.anomaly_severity > 0:
                selected.append("additional_work")
        return selected

    @staticmethod
    def _extract_flags_from_npv(npv_item: SvNpvBalance) -> List[str]:
        selected: List[str] = []
        category_key = (npv_item.category or "").strip().lower()
        mapped = NPV_CATEGORY_TO_FLAG.get(category_key)
        if mapped:
            selected.append(mapped)

        description = (npv_item.description or "").lower()
        if "прихват" in description:
            selected.append("stuck_pipe")
        if "поглощен" in description:
            selected.append("absorption")
        if "потер" in description and "циркуляц" in description:
            selected.append("circulation_loss")
        if "затяжк" in description:
            selected.append("drag")
        if "посадк" in description:
            selected.append("set_down")
        if "рост давлен" in description or "рост далвения" in description:
            selected.append("pressure_rise")
        if "геологическ" in description and "осложнен" in description:
            selected.append("geological_complication")
        if "забити" in description and "взд" in description:
            selected.append("mud_motor_plugged")
        if "отказ" in description:
            selected.append("equipment_failure")

        if not selected:
            selected.append("npv")
        # preserve order and uniqueness
        return list(dict.fromkeys(selected))

    def sync_events_from_supervisor(
        self,
        well_number: Optional[str],
        date_from: Optional[date],
        date_to: Optional[date],
        min_severity: int,
        dry_run: bool,
        max_operations: int,
        include_npv_balance: bool = True,
    ) -> Dict[str, Any]:
        query = (
            self.db.query(SvDailyOperation, SvDailyReport, Wellbore, Well)
            .join(SvDailyReport, SvDailyReport.report_id == SvDailyOperation.report_id)
            .join(Wellbore, Wellbore.wellbore_id == SvDailyReport.wellbore_id)
            .join(Well, Well.well_id == Wellbore.well_id)
            .filter(SvDailyOperation.anomaly_severity >= min_severity)
            .order_by(SvDailyReport.report_date.asc(), SvDailyOperation.sequence_number.asc())
        )

        if well_number:
            query = query.filter(Well.well_number == well_number)
        if date_from:
            query = query.filter(SvDailyReport.report_date >= date_from)
        if date_to:
            query = query.filter(SvDailyReport.report_date <= date_to)

        rows = query.limit(max_operations).all()
        created_events = 0
        skipped_existing = 0
        created_types = 0
        scanned_operations = len(rows)
        candidates = 0
        scanned_npv_items = 0
        candidate_npv_items = 0
        created_npv_events = 0
        skipped_existing_npv = 0
        preview: List[Dict[str, Any]] = []

        for op, report, wellbore, well in rows:
            flag_codes = self._extract_flag_codes(op)
            if not flag_codes:
                continue
            candidates += 1

            start_dt, end_dt = self._build_datetimes(
                report_date=report.report_date,
                time_from=op.time_from,
                time_to=op.time_to,
                duration_minutes=op.duration_minutes,
            )

            for flag_code in flag_codes:
                et_conf = FLAG_TO_EVENT_TYPE.get(flag_code)
                if not et_conf:
                    continue
                event_type, et_created = self._resolve_event_type(et_conf)
                if et_created:
                    created_types += 1
                if not event_type:
                    continue

                marker = f"sv_operation_id={op.operation_id};event_code={et_conf['code']};resolved_event_code={event_type.event_code}"
                existing = (
                    self.db.query(Event)
                    .filter(
                        Event.wellbore_id == wellbore.wellbore_id,
                        Event.event_type_id == event_type.event_type_id,
                        Event.start_time == start_dt,
                        Event.annotation_source == "supervisor_journal",
                    )
                    .first()
                )
                if existing:
                    skipped_existing += 1
                    continue

                note_text = f"[AUTO_FROM_SV] {marker}; report_id={report.report_id}; seq={op.sequence_number}; desc={op.description}"
                note_text = note_text[:4000]

                if not dry_run:
                    event = Event(
                        wellbore_id=wellbore.wellbore_id,
                        event_type_id=event_type.event_type_id,
                        start_time=start_dt,
                        end_time=end_dt,
                        start_md=op.depth_from_m,
                        end_md=op.depth_to_m,
                        annotation_source="supervisor_journal",
                        annotator_name="sv_events_sync",
                        confidence=min(1.0, 0.5 + 0.15 * int(op.anomaly_severity or 0)),
                        notes=note_text,
                    )
                    self.db.add(event)
                created_events += 1

                if len(preview) < 20:
                    preview.append(
                        {
                            "well_number": well.well_number,
                            "report_date": report.report_date.isoformat(),
                            "sv_operation_id": op.operation_id,
                            "sequence_number": op.sequence_number,
                            "event_code": event_type.event_code,
                            "event_name": event_type.event_name,
                            "severity": op.anomaly_severity,
                            "start_time": start_dt.isoformat(),
                            "end_time": end_dt.isoformat() if end_dt else None,
                            "description": op.description[:300],
                        }
                    )

        if include_npv_balance:
            npv_query = (
                self.db.query(SvNpvBalance, Wellbore, Well)
                .join(Wellbore, Wellbore.wellbore_id == SvNpvBalance.wellbore_id)
                .join(Well, Well.well_id == Wellbore.well_id)
                .order_by(SvNpvBalance.incident_date.asc(), SvNpvBalance.npv_id.asc())
            )
            if well_number:
                npv_query = npv_query.filter(Well.well_number == well_number)
            if date_from:
                npv_query = npv_query.filter(SvNpvBalance.incident_date >= date_from)
            if date_to:
                npv_query = npv_query.filter(SvNpvBalance.incident_date <= date_to)

            npv_rows = npv_query.limit(max_operations).all()
            scanned_npv_items = len(npv_rows)

            for npv_item, wellbore, well in npv_rows:
                npv_flags = self._extract_flags_from_npv(npv_item)
                if not npv_flags:
                    continue
                candidate_npv_items += 1

                start_dt = datetime.combine(npv_item.incident_date, time(0, 0))
                end_dt = None
                if npv_item.duration_hours and npv_item.duration_hours > 0:
                    end_dt = start_dt + timedelta(hours=float(npv_item.duration_hours))

                for flag_code in npv_flags:
                    et_conf = FLAG_TO_EVENT_TYPE.get(flag_code)
                    if not et_conf:
                        continue
                    event_type, et_created = self._resolve_event_type(et_conf)
                    if et_created:
                        created_types += 1
                    if not event_type:
                        continue

                    existing = (
                        self.db.query(Event)
                        .filter(
                            Event.wellbore_id == wellbore.wellbore_id,
                            Event.event_type_id == event_type.event_type_id,
                            Event.start_time == start_dt,
                            Event.annotation_source == "supervisor_journal_npv",
                        )
                        .first()
                    )
                    if existing:
                        skipped_existing_npv += 1
                        continue

                    note_text = (
                        f"[AUTO_FROM_SV_NPV] npv_id={npv_item.npv_id}; "
                        f"category={npv_item.category}; operation_type={npv_item.operation_type}; "
                        f"responsible_party={npv_item.responsible_party}; desc={npv_item.description}"
                    )
                    note_text = note_text[:4000]

                    if not dry_run:
                        event = Event(
                            wellbore_id=wellbore.wellbore_id,
                            event_type_id=event_type.event_type_id,
                            start_time=start_dt,
                            end_time=end_dt,
                            start_md=None,
                            end_md=None,
                            annotation_source="supervisor_journal_npv",
                            annotator_name="sv_events_sync",
                            confidence=0.85,
                            notes=note_text,
                        )
                        self.db.add(event)
                    created_events += 1
                    created_npv_events += 1

                    if len(preview) < 20:
                        preview.append(
                            {
                                "well_number": well.well_number,
                                "report_date": npv_item.incident_date.isoformat(),
                                "sv_npv_id": npv_item.npv_id,
                                "event_code": event_type.event_code,
                                "event_name": event_type.event_name,
                                "severity": et_conf.get("severity"),
                                "start_time": start_dt.isoformat(),
                                "end_time": end_dt.isoformat() if end_dt else None,
                                "description": (npv_item.description or "")[:300],
                            }
                        )

        if not dry_run:
            self.db.commit()

        return {
            "dry_run": dry_run,
            "well_number": well_number,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "min_severity": min_severity,
            "scanned_operations": scanned_operations,
            "candidate_operations": candidates,
            "created_event_types": created_types,
            "created_events": created_events,
            "skipped_existing": skipped_existing,
            "include_npv_balance": include_npv_balance,
            "scanned_npv_items": scanned_npv_items,
            "candidate_npv_items": candidate_npv_items,
            "created_npv_events": created_npv_events,
            "skipped_existing_npv": skipped_existing_npv,
            "preview": preview,
        }

    def diagnose_events_from_supervisor(
        self,
        well_number: str,
        date_from: Optional[date],
        date_to: Optional[date],
        min_severity: int,
        max_operations: int,
        include_npv_balance: bool = True,
    ) -> Dict[str, Any]:
        """
        Diagnose why candidates from sv_* are (or are not) inserted into events.
        No DB writes are performed.
        """
        diagnostic: Dict[str, Any] = {
            "well_number": well_number,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "min_severity": min_severity,
            "include_npv_balance": include_npv_balance,
            "operations": {
                "scanned": 0,
                "with_flags": 0,
                "with_event_type": 0,
                "missing_event_type": 0,
                "already_exists": 0,
                "ready_to_create": 0,
                "missing_event_type_by_code": {},
                "samples_missing_event_type": [],
                "samples_existing": [],
            },
            "npv": {
                "scanned": 0,
                "with_flags": 0,
                "with_event_type": 0,
                "missing_event_type": 0,
                "already_exists": 0,
                "ready_to_create": 0,
                "missing_event_type_by_code": {},
                "samples_missing_event_type": [],
                "samples_existing": [],
            },
        }

        op_query = (
            self.db.query(SvDailyOperation, SvDailyReport, Wellbore, Well)
            .join(SvDailyReport, SvDailyReport.report_id == SvDailyOperation.report_id)
            .join(Wellbore, Wellbore.wellbore_id == SvDailyReport.wellbore_id)
            .join(Well, Well.well_id == Wellbore.well_id)
            .filter(
                Well.well_number == well_number,
                SvDailyOperation.anomaly_severity >= min_severity,
            )
            .order_by(SvDailyReport.report_date.asc(), SvDailyOperation.sequence_number.asc())
            .limit(max_operations)
        )
        if date_from:
            op_query = op_query.filter(SvDailyReport.report_date >= date_from)
        if date_to:
            op_query = op_query.filter(SvDailyReport.report_date <= date_to)

        op_rows = op_query.all()
        diagnostic["operations"]["scanned"] = len(op_rows)

        for op, report, wellbore, _well in op_rows:
            flag_codes = self._extract_flag_codes(op)
            if not flag_codes:
                continue
            diagnostic["operations"]["with_flags"] += 1
            start_dt, _end_dt = self._build_datetimes(
                report_date=report.report_date,
                time_from=op.time_from,
                time_to=op.time_to,
                duration_minutes=op.duration_minutes,
            )

            for flag_code in flag_codes:
                et_conf = FLAG_TO_EVENT_TYPE.get(flag_code)
                if not et_conf:
                    continue

                event_type, _ = self._resolve_event_type(et_conf)
                if not event_type:
                    diagnostic["operations"]["missing_event_type"] += 1
                    missing = diagnostic["operations"]["missing_event_type_by_code"]
                    missing[et_conf["code"]] = missing.get(et_conf["code"], 0) + 1
                    samples = diagnostic["operations"]["samples_missing_event_type"]
                    if len(samples) < 20:
                        samples.append(
                            {
                                "sv_operation_id": op.operation_id,
                                "report_date": report.report_date.isoformat(),
                                "flag_code": flag_code,
                                "event_code": et_conf["code"],
                                "description": (op.description or "")[:240],
                            }
                        )
                    continue

                diagnostic["operations"]["with_event_type"] += 1
                existing = (
                    self.db.query(Event)
                    .filter(
                        Event.wellbore_id == wellbore.wellbore_id,
                        Event.event_type_id == event_type.event_type_id,
                        Event.start_time == start_dt,
                        Event.annotation_source == "supervisor_journal",
                    )
                    .first()
                )
                if existing:
                    diagnostic["operations"]["already_exists"] += 1
                    samples = diagnostic["operations"]["samples_existing"]
                    if len(samples) < 20:
                        samples.append(
                            {
                                "event_id": existing.event_id,
                                "sv_operation_id": op.operation_id,
                                "event_code": event_type.event_code,
                                "start_time": start_dt.isoformat(),
                            }
                        )
                    continue

                diagnostic["operations"]["ready_to_create"] += 1

        if include_npv_balance:
            npv_query = (
                self.db.query(SvNpvBalance, Wellbore, Well)
                .join(Wellbore, Wellbore.wellbore_id == SvNpvBalance.wellbore_id)
                .join(Well, Well.well_id == Wellbore.well_id)
                .filter(Well.well_number == well_number)
                .order_by(SvNpvBalance.incident_date.asc(), SvNpvBalance.npv_id.asc())
                .limit(max_operations)
            )
            if date_from:
                npv_query = npv_query.filter(SvNpvBalance.incident_date >= date_from)
            if date_to:
                npv_query = npv_query.filter(SvNpvBalance.incident_date <= date_to)

            npv_rows = npv_query.all()
            diagnostic["npv"]["scanned"] = len(npv_rows)

            for npv_item, wellbore, _well in npv_rows:
                npv_flags = self._extract_flags_from_npv(npv_item)
                if not npv_flags:
                    continue
                diagnostic["npv"]["with_flags"] += 1

                start_dt = datetime.combine(npv_item.incident_date, time(0, 0))
                for flag_code in npv_flags:
                    et_conf = FLAG_TO_EVENT_TYPE.get(flag_code)
                    if not et_conf:
                        continue

                    event_type, _ = self._resolve_event_type(et_conf)
                    if not event_type:
                        diagnostic["npv"]["missing_event_type"] += 1
                        missing = diagnostic["npv"]["missing_event_type_by_code"]
                        missing[et_conf["code"]] = missing.get(et_conf["code"], 0) + 1
                        samples = diagnostic["npv"]["samples_missing_event_type"]
                        if len(samples) < 20:
                            samples.append(
                                {
                                    "sv_npv_id": npv_item.npv_id,
                                    "incident_date": npv_item.incident_date.isoformat(),
                                    "flag_code": flag_code,
                                    "event_code": et_conf["code"],
                                    "description": (npv_item.description or "")[:240],
                                }
                            )
                        continue

                    diagnostic["npv"]["with_event_type"] += 1
                    existing = (
                        self.db.query(Event)
                        .filter(
                            Event.wellbore_id == wellbore.wellbore_id,
                            Event.event_type_id == event_type.event_type_id,
                            Event.start_time == start_dt,
                            Event.annotation_source == "supervisor_journal_npv",
                        )
                        .first()
                    )
                    if existing:
                        diagnostic["npv"]["already_exists"] += 1
                        samples = diagnostic["npv"]["samples_existing"]
                        if len(samples) < 20:
                            samples.append(
                                {
                                    "event_id": existing.event_id,
                                    "sv_npv_id": npv_item.npv_id,
                                    "event_code": event_type.event_code,
                                    "start_time": start_dt.isoformat(),
                                }
                            )
                        continue

                    diagnostic["npv"]["ready_to_create"] += 1

        totals = {
            "scanned_total": diagnostic["operations"]["scanned"] + diagnostic["npv"]["scanned"],
            "ready_to_create_total": diagnostic["operations"]["ready_to_create"] + diagnostic["npv"]["ready_to_create"],
            "already_exists_total": diagnostic["operations"]["already_exists"] + diagnostic["npv"]["already_exists"],
            "missing_event_type_total": diagnostic["operations"]["missing_event_type"] + diagnostic["npv"]["missing_event_type"],
        }
        diagnostic["totals"] = totals
        return diagnostic

    def cleanup_events_from_supervisor(
        self,
        well_number: str,
        date_from: Optional[date],
        date_to: Optional[date],
        dry_run: bool,
        include_npv_balance: bool = True,
    ) -> Dict[str, Any]:
        """
        Remove auto-created events for a well from supervisor sources.
        Sources:
          - supervisor_journal
          - supervisor_journal_npv (optional)
        """
        sources = ["supervisor_journal"]
        if include_npv_balance:
            sources.append("supervisor_journal_npv")

        base_query = (
            self.db.query(Event)
            .join(Wellbore, Wellbore.wellbore_id == Event.wellbore_id)
            .join(Well, Well.well_id == Wellbore.well_id)
            .filter(
                Well.well_number == well_number,
                Event.annotation_source.in_(sources),
            )
        )

        if date_from:
            start_dt = datetime.combine(date_from, time(0, 0))
            base_query = base_query.filter(Event.start_time >= start_dt)
        if date_to:
            end_dt = datetime.combine(date_to + timedelta(days=1), time(0, 0))
            base_query = base_query.filter(Event.start_time < end_dt)

        events = base_query.all()
        total_found = len(events)
        deleted_events = 0
        by_source: Dict[str, int] = {}
        preview: List[Dict[str, Any]] = []

        for event in events:
            src = event.annotation_source or "unknown"
            by_source[src] = by_source.get(src, 0) + 1
            if len(preview) < 20:
                preview.append(
                    {
                        "event_id": event.event_id,
                        "source": src,
                        "start_time": event.start_time.isoformat() if event.start_time else None,
                        "start_md": event.start_md,
                        "notes": (event.notes or "")[:200],
                    }
                )

            if not dry_run:
                self.db.delete(event)
            deleted_events += 1

        if not dry_run:
            self.db.commit()

        return {
            "dry_run": dry_run,
            "well_number": well_number,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "include_npv_balance": include_npv_balance,
            "sources": sources,
            "found_events": total_found,
            "deleted_events": deleted_events,
            "by_source": by_source,
            "preview": preview,
        }
