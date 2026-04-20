"""
LAS file parser service
"""
import lasio
import pandas as pd
import numpy as np
import re
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy.orm import Session
from ..models import Well, Wellbore, GtiLog, GtiSnapshot, File
from ..config import settings


class LASParserService:
    """Service for parsing LAS files"""
    
    # Default channel mapping (LAS mnemonic -> DB column - match SQL schema)
    DEFAULT_CHANNEL_MAPPING = {
        "Zab": "dbtm",
        "W": "woba",      # Changed: wob -> woba
        "Hkr": "bpos",
        "M": "tqa",       # Changed: trq -> tqa
        "W kr": "hkla",   # Changed: hkld -> hkla
        "P vkh": "sppa",  # Changed: spp -> sppa
        "N rot": "rpma",  # Changed: rpm -> rpma
        "V sum": "tvt",   # Changed: tvol -> tvt
        "Q vkh": "mfia",  # Changed: mfip -> mfia
        "Q vyikh": "mfoa",  # Changed: mfop -> mfoa
        "G vkh": "mdia",  # Changed: mwin -> mdia
        "G vyikh": "mdoa",  # Changed: mwop -> mdoa
        "G sum": "gasa",  # Changed: tgas -> gasa
        "Gl.dol": "dmea",
    }
    
    # Columns that go to params_extra
    EXTRA_COLUMNS = ["V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", 
                     "V10", "V11", "V12", "V13", "V14", "V15", "M kl",
                     "Ves instr.", "VES INSTR.", "Ves instr.."]
    
    def __init__(self, db: Session):
        self.db = db
    
    def parse_las_structure(self, file_path: str) -> Dict[str, Any]:
        """Parse LAS file structure without importing"""
        las = lasio.read(file_path)
        
        # Extract well info
        well_info = {}
        for item in las.well:
            well_info[item.mnemonic] = item.value
        
        # Parse curves
        curves = []
        auto_mapping = {}
        
        for curve in las.curves:
            suggested = self.DEFAULT_CHANNEL_MAPPING.get(curve.mnemonic)
            if suggested:
                auto_mapping[curve.mnemonic] = suggested
            
            # Get statistics
            data = las[curve.mnemonic]
            
            # Try to get statistics, handle non-numeric data gracefully
            try:
                # Convert to numeric, coerce errors to NaN
                data_numeric = pd.to_numeric(data, errors='coerce')
                valid_data = data_numeric[~pd.isna(data_numeric)]
                
                min_val = float(valid_data.min()) if len(valid_data) > 0 else None
                max_val = float(valid_data.max()) if len(valid_data) > 0 else None
                null_count = int(pd.isna(data_numeric).sum()) if hasattr(data_numeric, '__len__') else 0
            except:
                # If conversion fails, just use None for stats
                min_val = None
                max_val = None
                null_count = 0
            
            curves.append({
                "mnemonic": curve.mnemonic,
                "unit": curve.unit or "",
                "description": curve.descr or "",
                "suggested_mapping": suggested,
                "sample_values": list(data[:3]) if len(data) > 0 else [],
                "min": min_val,
                "max": max_val,
                "null_count": null_count
            })
        
        # Calculate statistics
        total_records = len(las.data) if las.data is not None else 0
        
        # Try to get time range
        time_range_hours = None
        sampling_rate = None
        if "STRT" in well_info and "STOP" in well_info:
            try:
                start = self._parse_las_datetime(well_info["STRT"])
                stop = self._parse_las_datetime(well_info["STOP"])
                if start and stop:
                    time_range_hours = (stop - start).total_seconds() / 3600
            except:
                pass
        
        if "STEP" in well_info:
            try:
                step_str = str(well_info["STEP"]).lower()
                if "sec" in step_str or step_str.isdigit():
                    sampling_rate = float(step_str.replace("sec", "").strip()) if "sec" in step_str else float(step_str)
                else:
                    sampling_rate = float(well_info["STEP"])
            except:
                sampling_rate = 1.0
        
        # Check for existing well
        well_name = well_info.get("WELL", "")
        existing_well = self._find_well(well_name)
        
        return {
            "las_version": las.version[0].value if las.version else "2.0",
            "well_info": well_info,
            "curves": curves,
            "statistics": {
                "total_records": total_records,
                "time_range_hours": time_range_hours,
                "sampling_rate_sec": sampling_rate,
                "null_percentage": {c["mnemonic"]: round(c["null_count"] / max(total_records, 1) * 100, 2) for c in curves}
            },
            "auto_mapping": auto_mapping,
            "existing_well": {
                "found": existing_well is not None,
                "well_id": existing_well.well_id if existing_well else None,
                "well_number": existing_well.well_number if existing_well else None,
                "wellbore_id": existing_well.wellbores[0].wellbore_id if existing_well and existing_well.wellbores else None
            }
        }
    
    def import_las(
        self,
        file_path: str,
        well_number: Optional[str] = None,
        create_well: bool = False,
        channel_mapping: Optional[Dict[str, str]] = None,
        extra_columns: Optional[List[str]] = None,
        unit_conversions: Optional[Dict[str, Dict[str, Any]]] = None,
        batch_size: int = 10000,
        progress_callback: Optional[callable] = None,
        existing_log_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Import LAS file into database with progress tracking"""
        
        # Progress: Reading file
        if progress_callback:
            progress_callback({"stage": "reading", "progress": 0, "message": "Reading LAS file..."})
        
        las = lasio.read(file_path, ignore_data=True)
        mapping = {**self.DEFAULT_CHANNEL_MAPPING, **(channel_mapping or {})}
        
        if progress_callback:
            progress_callback({"stage": "reading", "progress": 10, "message": "LAS file read successfully"})
        
        # Extract well info
        well_info = {item.mnemonic: item.value for item in las.well}
        
        # Determine well number and convert to string immediately
        if not well_number:
            well_number = well_info.get("WELL", "")
            if not well_number:
                # Try to get from folder name
                well_number = Path(file_path).parent.name

        # CRITICAL: Convert to Python string (handles numpy types, numbers, etc.)
        well_number = str(well_number).strip() if well_number else ""

        # Also convert well_info values to native Python types
        well_info = {k: str(v) if v is not None else "" for k, v in well_info.items()}

        # Parse time range
        start_time = self._parse_las_datetime(well_info.get("STRT"))
        end_time = self._parse_las_datetime(well_info.get("STOP"))
        if start_time and end_time and end_time <= start_time:
            end_time = start_time + timedelta(seconds=1)

        # Get sampling rate
        sampling_rate = 1.0
        if "STEP" in well_info:
            try:
                step_str = str(well_info["STEP"]).lower().replace("sec", "").strip()
                sampling_rate = float(step_str) if step_str else 1.0
            except:
                pass

        # Progress: Setting up well/log
        if progress_callback:
            progress_callback({"stage": "setup", "progress": 15, "message": f"Setting up well {well_number}..."})

        if existing_log_id is not None:
            gti_log = self.db.query(GtiLog).filter(GtiLog.log_id == existing_log_id).first()
            if not gti_log:
                raise ValueError(f"GTI log not found: {existing_log_id}")

            wellbore = self.db.query(Wellbore).filter(Wellbore.wellbore_id == gti_log.wellbore_id).first()
            if not wellbore:
                raise ValueError(f"Wellbore not found for log_id={existing_log_id}")

            well = self.db.query(Well).filter(Well.well_id == wellbore.well_id).first()
            if not well:
                raise ValueError(f"Well not found for log_id={existing_log_id}")

            gti_log.start_time = start_time or gti_log.start_time or datetime.utcnow()
            gti_log.end_time = end_time or gti_log.end_time or (gti_log.start_time + timedelta(seconds=1))
            if gti_log.end_time <= gti_log.start_time:
                gti_log.end_time = gti_log.start_time + timedelta(seconds=1)
            gti_log.sampling_rate_sec = sampling_rate
            # Total records will be finalized after dataframe extraction.
            gti_log.total_records = 0
            gti_log.source_file_path = file_path
            gti_log.file_format = "las"
            gti_log.quality_status = "processing"
            self.db.commit()
            self.db.refresh(gti_log)
        else:
            # Find or create well
            well = self._find_well(well_number)
            if not well:
                if create_well:
                    well = Well(
                        well_number=well_number,
                        field=well_info.get("FLD"),
                        project_code="imported",
                        company=well_info.get("COMP", "unknown"),
                        metadata_={
                            "kust": well_info.get("KUST"),
                            "srvc": well_info.get("SRVC")
                        }
                    )
                    self.db.add(well)
                    self.db.commit()
                    self.db.refresh(well)
                else:
                    raise ValueError(f"Well not found: {well_number}")

            # Get or create wellbore
            wellbore = self.db.query(Wellbore).filter(
                Wellbore.well_id == well.well_id,
                Wellbore.wellbore_number == "main"
            ).first()

            if not wellbore:
                wellbore = Wellbore(
                    well_id=well.well_id,
                    wellbore_number="main"
                )
                self.db.add(wellbore)
                self.db.commit()
                self.db.refresh(wellbore)

            # Progress: Creating log entry
            if progress_callback:
                progress_callback({"stage": "setup", "progress": 20, "message": "Creating GTI log entry..."})

            # Create GTI log
            gti_log = GtiLog(
                wellbore_id=wellbore.wellbore_id,
                start_time=start_time or datetime.utcnow(),
                end_time=end_time or (start_time + timedelta(seconds=1) if start_time else datetime.utcnow()),
                sampling_rate_sec=sampling_rate,
                total_records=0,
                quality_status="pending",
                source_file_path=file_path,
                file_format="las"
            )
            if gti_log.end_time <= gti_log.start_time:
                gti_log.end_time = gti_log.start_time + timedelta(seconds=1)
            self.db.add(gti_log)
            self.db.commit()
            self.db.refresh(gti_log)
        
        # Progress: Converting to DataFrame
        if progress_callback:
            progress_callback({"stage": "processing", "progress": 25, "message": "Converting LAS to DataFrame..."})
        
        # Convert LAS to DataFrame using raw ~A parsing.
        # This preserves exact channel order from ~C and handles decimal comma values.
        curve_mnemonics = [curve.mnemonic for curve in las.curves]
        df = self._build_dataframe_from_ascii_section(file_path, curve_mnemonics)
        if df.empty:
            raise ValueError("Failed to parse LAS ~A section into dataframe")
        
        # Convert all columns to numeric where possible, keep strings as-is
        for col in df.columns:
            if col not in ['DATE', 'TIME']:  # Skip date/time columns
                df[col] = pd.to_numeric(df[col], errors='ignore')
        
        # Finalize total records from parsed dataframe.
        gti_log.total_records = len(df)
        self.db.commit()
        self.db.refresh(gti_log)

        if progress_callback:
            progress_callback({"stage": "processing", "progress": 35, "message": f"Processing {len(df):,} records..."})
        
        # Parse datetime from DATE + TIME with strict priority.
        # If lasio dataframe alignment is unstable, parse directly from ~A rows.
        raw_datetimes = self._extract_datetime_from_ascii_section(file_path, len(df))
        if raw_datetimes:
            df["time_utc"] = raw_datetimes
        elif "DATE" in df.columns and "TIME" in df.columns:
            df["time_utc"] = df.apply(
                lambda row: self._combine_datetime(row.get("DATE"), row.get("TIME")),
                axis=1
            )
        else:
            df["time_utc"] = None

        # Fallback to generated timestamps only for rows where DATE+TIME couldn't be parsed.
        if "time_utc" not in df.columns or df["time_utc"].isna().all():
            if start_time:
                df["time_utc"] = pd.date_range(
                    start=start_time,
                    periods=len(df),
                    freq=f"{int(sampling_rate)}S"
                )
            else:
                df["time_utc"] = pd.date_range(
                    start=datetime.utcnow(),
                    periods=len(df),
                    freq=f"{int(sampling_rate)}S"
                )
        elif df["time_utc"].isna().any():
            fallback_start = start_time or datetime.utcnow()
            generated = pd.date_range(
                start=fallback_start,
                periods=len(df),
                freq=f"{int(sampling_rate)}S"
            )
            df["time_utc"] = df["time_utc"].where(df["time_utc"].notna(), generated)
        
        df["log_id"] = gti_log.log_id
        
        # Ensure time_utc exists and is not None
        if "time_utc" not in df.columns or df["time_utc"].isna().all():
            if start_time:
                df["time_utc"] = pd.date_range(
                    start=start_time,
                    periods=len(df),
                    freq=f"{int(sampling_rate)}S"
                )
            else:
                df["time_utc"] = pd.date_range(
                    start=datetime.utcnow(),
                    periods=len(df),
                    freq=f"{int(sampling_rate)}S"
                )
        
        # Apply channel mapping
        rename_map = {}
        for las_col, db_col in mapping.items():
            if las_col in df.columns:
                rename_map[las_col] = db_col
        
        df = df.rename(columns=rename_map)

        # Multiple LAS mnemonics can map to one DB column (e.g. "M" and "M KL." -> "tqa").
        # Collapse duplicated column names by taking first non-null value row-wise.
        duplicate_cols = [c for c in df.columns[df.columns.duplicated(keep=False)].unique()]
        if duplicate_cols:
            for col in duplicate_cols:
                same_name_cols = df.loc[:, df.columns == col]
                if same_name_cols.shape[1] > 1:
                    df[col] = same_name_cols.bfill(axis=1).iloc[:, 0]
            df = df.loc[:, ~df.columns.duplicated(keep="first")]
        
        # Apply unit conversions
        if unit_conversions:
            for col, conv in unit_conversions.items():
                if col in df.columns:
                    factor = conv.get("factor", 1.0)
                    df[col] = df[col] * factor
        
        # Collect extra columns into params_extra (case/punctuation-insensitive)
        def _normalize_extra_key(value: str) -> str:
            return str(value).strip().lower().replace(".", "").replace("_", " ")

        dynamic_extra = extra_columns or []
        normalized_extra = {
            _normalize_extra_key(c)
            for c in [*self.EXTRA_COLUMNS, *dynamic_extra]
            if c
        }
        extra_cols = [c for c in df.columns if _normalize_extra_key(c) in normalized_extra]
        if extra_cols:
            df["params_extra"] = df[extra_cols].apply(
                lambda row: {k: v for k, v in row.items() if pd.notna(v)},
                axis=1
            )
        
        # Select only valid columns for gti_snapshots (match SQL schema)
        valid_columns = [
            "log_id", "time_utc", "dbtm", "dmea",
            "woba", "ropa", "rpma", "tqa", "bpos", "sppa",
            "mfia", "mfoa", "mdia", "mdoa", "mtia", "mtoa",
            "tvt", "spm1", "spm2", "gasa", "c1c5", "hkla",
            "params_extra", "quality_flags"
        ]
        
        df_insert = df[[c for c in valid_columns if c in df.columns]].copy()

        # Ensure numeric snapshot channels are strictly numeric.
        # Some LAS files may contain textual fragments (e.g. time tokens) in channel columns.
        numeric_columns = [
            "dbtm", "dmea", "woba", "ropa", "rpma", "tqa", "bpos", "sppa",
            "mfia", "mfoa", "mdia", "mdoa", "mtia", "mtoa", "tvt", "spm1",
            "spm2", "gasa", "c1c5", "hkla"
        ]
        for col in numeric_columns:
            if col in df_insert.columns:
                df_insert[col] = pd.to_numeric(df_insert[col], errors="coerce")
        
        # CRITICAL: Ensure time_utc is present
        if "time_utc" not in df_insert.columns:
            raise ValueError("time_utc column is missing - cannot import without timestamps")
        
        # Check for NULL time_utc values
        null_count = df_insert["time_utc"].isna().sum()
        if null_count > 0:
            raise ValueError(f"Found {null_count} NULL time_utc values - all timestamps must be valid")
        
        # Replace NaN with None for database - use pandas built-in method
        df_insert = df_insert.where(pd.notnull(df_insert), None)
        
        # Batch insert with progress tracking
        total_records = len(df_insert)
        inserted = 0
        
        if progress_callback:
            progress_callback({"stage": "importing", "progress": 40, "message": f"Starting import of {total_records:,} records..."})
        
        for i in range(0, total_records, batch_size):
            batch = df_insert.iloc[i:i+batch_size]
            records = batch.to_dict(orient="records")
            
            self.db.bulk_insert_mappings(GtiSnapshot, records)
            self.db.commit()
            
            inserted += len(records)
            
            # Calculate progress (40% to 90% of total)
            if progress_callback:
                import_progress = 40 + int((inserted / total_records) * 50)
                progress_callback({
                    "stage": "importing",
                    "progress": import_progress,
                    "message": f"Imported {inserted:,} / {total_records:,} records ({inserted/total_records*100:.1f}%)",
                    "records_imported": inserted,
                    "total_records": total_records
                })
        
        # Progress: Finalizing
        if progress_callback:
            progress_callback({"stage": "finalizing", "progress": 95, "message": "Registering file..."})
        
        # Register file
        file_record = File(
            file_name=Path(file_path).name,
            file_path=file_path,
            file_type="las",
            category="gti_data",
            well_id=well.well_id,
            log_id=gti_log.log_id,
            file_size_bytes=Path(file_path).stat().st_size,
            processing_status="completed"
        )
        self.db.add(file_record)
        self.db.commit()

        gti_log.quality_status = "completed"
        self.db.commit()
        
        if progress_callback:
            progress_callback({"stage": "completed", "progress": 100, "message": f"Import completed! {inserted:,} records imported."})
        
        return {
            "success": True,
            "well_id": well.well_id,
            "wellbore_id": wellbore.wellbore_id,
            "log_id": gti_log.log_id,
            "well_number": well.well_number,
            "file_id": file_record.file_id,
            "records_imported": inserted
        }
    
    def _find_well(self, well_number: str) -> Optional[Well]:
        """Find well by number (with variations)"""
        if not well_number:
            return None
        
        # Convert to string and clean (handle numpy types)
        well_number = str(well_number).strip()
        
        # Try exact match
        well = self.db.query(Well).filter(Well.well_number == well_number).first()
        if well:
            return well
        
        # Try case-insensitive
        well = self.db.query(Well).filter(
            Well.well_number.ilike(well_number)
        ).first()
        if well:
            return well
        
        # Try stripping trailing letters (10767A -> 10767А)
        # Handle Latin/Cyrillic conversion
        latin_to_cyrillic = {"A": "А", "B": "Б", "D": "Д", "E": "Е"}
        converted = well_number
        for lat, cyr in latin_to_cyrillic.items():
            converted = converted.replace(lat, cyr)
        
        if converted != well_number:
            well = self.db.query(Well).filter(Well.well_number == converted).first()
            if well:
                return well
        
        return None
    
    def _parse_las_datetime(self, value: Any) -> Optional[datetime]:
        """Parse datetime from LAS file"""
        if value is None:
            return None
        
        value_str = str(value).strip()
        
        # Try common formats
        formats = [
            "%Y.%m.%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%Y.%m.%d",
            "%Y-%m-%d",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(value_str, fmt)
            except:
                continue
        
        return None
    
    def _combine_datetime(self, date_val: Any, time_val: Any) -> Optional[datetime]:
        """Combine DATE and TIME values into datetime"""
        if pd.isna(date_val) or pd.isna(time_val):
            return None
        
        try:
            date_str = str(date_val).strip()
            time_str = str(time_val).strip()
            
            # Try parsing combined
            combined = f"{date_str} {time_str}"
            return self._parse_las_datetime(combined)
        except:
            return None

    def _extract_datetime_from_ascii_section(self, file_path: str, expected_rows: int) -> Optional[List[datetime]]:
        """Extract DATE+TIME from raw ~A section, independent of lasio dataframe alignment."""
        result: List[datetime] = []
        in_ascii_section = False

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue

                    if not in_ascii_section:
                        if line.upper().startswith("~A"):
                            in_ascii_section = True
                        continue

                    if line.startswith("#"):
                        continue

                    # DATE and TIME are expected as first 2 tokens in this dataset.
                    parts = line.split()
                    if len(parts) < 2:
                        continue

                    dt = self._combine_datetime(parts[0], parts[1])
                    if dt is None:
                        # If parsing breaks, stop relying on this path.
                        return None
                    result.append(dt)

                    if len(result) >= expected_rows:
                        break
        except Exception:
            return None

        if len(result) == expected_rows:
            return result
        return None

    def _build_dataframe_from_ascii_section(self, file_path: str, curve_mnemonics: List[str]) -> pd.DataFrame:
        """Build DataFrame from raw ~A rows using exact curve order from ~C."""
        if not curve_mnemonics:
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        in_ascii_section = False

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue

                    if not in_ascii_section:
                        if line.upper().startswith("~A"):
                            in_ascii_section = True
                        continue

                    if line.startswith("#"):
                        continue

                    parts = line.split()
                    if len(parts) < len(curve_mnemonics):
                        # Skip malformed rows instead of shifting all subsequent values.
                        continue

                    # Some files can contain extra trailing tokens. Use only declared curve count.
                    parts = parts[:len(curve_mnemonics)]
                    row = {
                        mnemonic: self._parse_ascii_value(parts[idx])
                        for idx, mnemonic in enumerate(curve_mnemonics)
                    }
                    rows.append(row)
        except Exception:
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame(columns=curve_mnemonics)
        return pd.DataFrame(rows, columns=curve_mnemonics)

    def _parse_ascii_value(self, raw_value: str) -> Any:
        """Parse raw token from ~A section with decimal comma support."""
        if raw_value is None:
            return None
        token = str(raw_value).strip()
        if not token:
            return None

        upper_token = token.upper()
        if upper_token in {"NULL", "NAN", "NA"}:
            return None

        # Keep date/time as string, it is parsed later into time_utc.
        if re.fullmatch(r"\d{4}[./-]\d{2}[./-]\d{2}", token):
            return token
        if re.fullmatch(r"\d{2}:\d{2}:\d{2}", token):
            return token

        # Common LAS null marker and comma decimal support.
        numeric_token = token.replace(",", ".")
        if numeric_token in {"-999.25", "-9999", "-999.0"}:
            return None

        try:
            return float(numeric_token)
        except Exception:
            return token
