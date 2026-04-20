"""
Excel parser service for wells and events import
"""
import pandas as pd
import re
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from ..models import Well, Wellbore, Event, EventType
from ..schemas.import_schemas import ExcelColumnMapping, ComplicationRule


class ExcelParserService:
    """Service for parsing Excel files"""
    
    # Default column mappings
    DEFAULT_COLUMN_MAPPING = {
        "well_number": ["№ скв", "номер скважины", "скважина", "well_number", "well"],
        "bush_number": ["№ куста", "куст", "bush", "kust"],
        "field": ["площадь", "месторождение", "field"],
        "category": ["категория", "category"],
        "ngdu": ["нгду", "ngdu"],
        "construction": ["конструкция скважины", "конструкция", "construction"],
        "completion_date": ["дата завершения скважины", "дата завершения", "completion_date"],
        "circulation": ["характер циркуляции", "циркуляция", "circulation"],
        "gdi_data": ["данные гди", "гди", "gdi"],
        "injectivity": ["коэффициент приёмистости", "приёмистость", "injectivity"]
    }
    
    # Complication patterns
    DEFAULT_COMPLICATION_RULES = [
        {"pattern": r"пух", "event_type": "absorption", "confidence": 0.9},
        {"pattern": r"перелив", "event_type": "overflow", "confidence": 0.9},
        {"pattern": r"прихват", "event_type": "stuck_pipe", "confidence": 0.95},
        {"pattern": r"поглощен", "event_type": "absorption", "confidence": 0.9},
        {"pattern": r"Ц-(\d+)%", "event_type": "circulation_loss", "confidence": 0.8, "extract_value": True},
    ]
    
    def __init__(self, db: Session):
        self.db = db
    
    def parse_excel_structure(self, file_path: str) -> Dict[str, Any]:
        """Parse Excel file structure without importing"""
        df = pd.read_excel(file_path)
        
        columns_info = []
        auto_mapping = {}
        
        for idx, col in enumerate(df.columns):
            col_lower = str(col).lower().strip()
            
            # Detect column type
            suggested_mapping = None
            for mapping_key, patterns in self.DEFAULT_COLUMN_MAPPING.items():
                for pattern in patterns:
                    if pattern.lower() in col_lower:
                        suggested_mapping = f"wells.{mapping_key}" if mapping_key in ["well_number", "field"] else mapping_key
                        auto_mapping[mapping_key] = col
                        break
                if suggested_mapping:
                    break
            
            # Get sample values
            sample_values = df[col].dropna().head(3).tolist()
            
            columns_info.append({
                "name": col,
                "index": idx,
                "dtype": str(df[col].dtype),
                "sample_values": sample_values,
                "null_count": int(df[col].isna().sum()),
                "suggested_mapping": suggested_mapping
            })
        
        return {
            "file_info": {
                "filename": file_path.split("\\")[-1].split("/")[-1],
                "sheets": ["Sheet1"],  # TODO: support multiple sheets
                "rows": len(df),
                "columns": len(df.columns)
            },
            "columns": columns_info,
            "auto_detected_mapping": auto_mapping
        }
    
    def import_wells(
        self,
        file_path: str,
        project_code: str,
        company: str = "pao-tatneft",
        column_mapping: Optional[ExcelColumnMapping] = None,
        create_wellbores: bool = True,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Import wells from Excel file"""
        
        df = pd.read_excel(file_path)
        mapping = column_mapping or ExcelColumnMapping()
        
        results = {
            "total_rows": len(df),
            "wells_created": 0,
            "wells_updated": 0,
            "wells_skipped": 0,
            "wellbores_created": 0,
            "errors": [],
            "wells": []
        }
        
        for idx, row in df.iterrows():
            try:
                # Extract well number
                well_number = str(row.get(mapping.well_number, "")).strip()
                if not well_number or well_number == "nan":
                    results["wells_skipped"] += 1
                    continue
                
                # Build metadata
                metadata = {}
                if mapping.bush_number and mapping.bush_number in row:
                    metadata["bush_number"] = str(row[mapping.bush_number])
                if mapping.category and mapping.category in row:
                    metadata["category"] = str(row[mapping.category])
                if mapping.ngdu and mapping.ngdu in row:
                    metadata["ngdu"] = str(row[mapping.ngdu])
                if mapping.construction and mapping.construction in row:
                    metadata["construction"] = str(row[mapping.construction])
                if mapping.completion_date and mapping.completion_date in row:
                    completion = row[mapping.completion_date]
                    if pd.notna(completion):
                        if isinstance(completion, datetime):
                            metadata["completion_date"] = completion.isoformat()
                        else:
                            metadata["completion_date"] = str(completion)
                
                # Get field
                field = None
                if mapping.field and mapping.field in row:
                    field = str(row[mapping.field])
                    if field == "nan":
                        field = None
                
                if dry_run:
                    results["wells"].append({
                        "well_number": well_number,
                        "field": field,
                        "status": "would_create"
                    })
                    results["wells_created"] += 1
                    continue
                
                # Check if well exists
                existing_well = self.db.query(Well).filter(
                    Well.well_number == well_number
                ).first()
                
                if existing_well:
                    # Update existing well
                    existing_well.field = field or existing_well.field
                    existing_well.metadata_ = {**(existing_well.metadata_ or {}), **metadata}
                    self.db.commit()
                    well_id = existing_well.well_id
                    results["wells_updated"] += 1
                    status = "updated"
                else:
                    # Create new well
                    well = Well(
                        well_number=well_number,
                        field=field,
                        project_code=project_code,
                        company=company,
                        metadata_=metadata
                    )
                    self.db.add(well)
                    self.db.commit()
                    self.db.refresh(well)
                    well_id = well.well_id
                    results["wells_created"] += 1
                    status = "created"
                
                # Create wellbore if needed
                wellbore_id = None
                if create_wellbores:
                    existing_wellbore = self.db.query(Wellbore).filter(
                        Wellbore.well_id == well_id,
                        Wellbore.wellbore_number == "main"
                    ).first()
                    
                    if not existing_wellbore:
                        # Parse diameter from construction
                        diameter_mm = self._parse_diameter(metadata.get("construction", ""))
                        
                        # Build wellbore properties
                        wb_properties = {}
                        if mapping.gdi_data and mapping.gdi_data in row:
                            gdi = row[mapping.gdi_data]
                            if pd.notna(gdi):
                                wb_properties["gdi_data"] = str(gdi)
                        if mapping.injectivity and mapping.injectivity in row:
                            inj = row[mapping.injectivity]
                            if pd.notna(inj):
                                wb_properties["injectivity"] = float(inj) if isinstance(inj, (int, float)) else str(inj)
                        if mapping.circulation and mapping.circulation in row:
                            circ = row[mapping.circulation]
                            if pd.notna(circ):
                                wb_properties["circulation"] = str(circ)
                        
                        wellbore = Wellbore(
                            well_id=well_id,
                            wellbore_number="main",
                            diameter_mm=diameter_mm,
                            properties=wb_properties if wb_properties else None
                        )
                        self.db.add(wellbore)
                        self.db.commit()
                        self.db.refresh(wellbore)
                        wellbore_id = wellbore.wellbore_id
                        results["wellbores_created"] += 1
                    else:
                        wellbore_id = existing_wellbore.wellbore_id
                
                results["wells"].append({
                    "well_id": well_id,
                    "well_number": well_number,
                    "field": field,
                    "wellbore_id": wellbore_id,
                    "status": status
                })
                
            except Exception as e:
                results["errors"].append(f"Row {idx}: {str(e)}")
                results["wells_skipped"] += 1
        
        return results
    
    def import_events(
        self,
        file_path: str,
        annotation_source: str,
        column_mapping: Optional[Dict[str, str]] = None,
        complication_rules: Optional[List[ComplicationRule]] = None
    ) -> Dict[str, Any]:
        """Import events/complications from Excel"""
        
        df = pd.read_excel(file_path)
        mapping = column_mapping or {"well_number": "№ скв", "circulation": "Характер циркуляции, %"}
        rules = complication_rules or [ComplicationRule(**r) for r in self.DEFAULT_COMPLICATION_RULES]
        
        results = {
            "total_rows": len(df),
            "events_created": 0,
            "events_by_type": {},
            "errors": [],
            "events": []
        }
        
        for idx, row in df.iterrows():
            try:
                well_number = str(row.get(mapping.get("well_number", "№ скв"), "")).strip()
                if not well_number or well_number == "nan":
                    continue
                
                # Find well and wellbore
                well = self.db.query(Well).filter(Well.well_number == well_number).first()
                if not well:
                    continue
                
                wellbore = self.db.query(Wellbore).filter(
                    Wellbore.well_id == well.well_id,
                    Wellbore.wellbore_number == "main"
                ).first()
                if not wellbore:
                    continue
                
                # Check for complications
                circulation_col = mapping.get("circulation", "Характер циркуляции, %")
                circulation_value = row.get(circulation_col, "")
                
                if pd.isna(circulation_value):
                    continue
                
                circulation_str = str(circulation_value).lower()
                
                for rule in rules:
                    pattern = rule.pattern if isinstance(rule, ComplicationRule) else rule.get("pattern", "")
                    event_type_code = rule.event_type if isinstance(rule, ComplicationRule) else rule.get("event_type", "")
                    confidence = rule.confidence if isinstance(rule, ComplicationRule) else rule.get("confidence", 0.9)
                    
                    if re.search(pattern.lower(), circulation_str):
                        # Get event type
                        event_type = self.db.query(EventType).filter(
                            EventType.event_code == event_type_code
                        ).first()
                        
                        if not event_type:
                            continue
                        
                        # Create event
                        event = Event(
                            wellbore_id=wellbore.wellbore_id,
                            event_type_id=event_type.event_type_id,
                            annotation_source=annotation_source,
                            confidence=confidence,
                            notes=str(circulation_value)
                        )
                        self.db.add(event)
                        self.db.commit()
                        self.db.refresh(event)
                        
                        results["events_created"] += 1
                        results["events_by_type"][event_type_code] = results["events_by_type"].get(event_type_code, 0) + 1
                        results["events"].append({
                            "event_id": event.event_id,
                            "well_number": well_number,
                            "wellbore_id": wellbore.wellbore_id,
                            "event_type": event_type_code,
                            "notes": str(circulation_value),
                            "confidence": confidence
                        })
                        
            except Exception as e:
                results["errors"].append(f"Row {idx}: {str(e)}")
        
        return results
    
    def _parse_diameter(self, construction: str) -> Optional[float]:
        """Parse casing diameter from construction string like '178 э/к + 127 хв'"""
        if not construction:
            return None
        
        match = re.search(r"(\d+)\s*э/к", construction)
        if match:
            return float(match.group(1))
        
        # Try just first number
        match = re.search(r"(\d+)", construction)
        if match:
            return float(match.group(1))
        
        return None
