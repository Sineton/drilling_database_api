"""
Well service - business logic for wells
"""
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List
from ..models import Well, Wellbore, GtiLog
from ..schemas.well import WellCreate, WellUpdate


class WellService:
    """Service for well operations"""

    def __init__(self, db: Session):
        self.db = db

    def get_well(self, well_id: int) -> Optional[Well]:
        return self.db.query(Well).filter(Well.well_id == well_id).first()

    def get_well_by_number(self, well_number: str) -> Optional[Well]:
        return self.db.query(Well).filter(Well.well_number == well_number).first()

    def get_wells(
        self,
        project_code: Optional[str] = None,
        field: Optional[str] = None,
        pad_number: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> tuple[List[Well], int]:
        query = self.db.query(Well)

        if project_code:
            query = query.filter(Well.project_code == project_code)
        if field:
            query = query.filter(Well.field.ilike(f"%{field}%"))
        if pad_number:
            query = query.filter(Well.pad_number == pad_number)
        if search:
            query = query.filter(Well.well_number.ilike(f"%{search}%"))

        query = query.order_by(Well.well_id)
        total = query.count()
        wells = query.offset(offset).limit(limit).all()

        return wells, total

    def create_well(self, well_data: WellCreate) -> Well:
        well = Well(
            well_number=well_data.well_number,
            well_name=well_data.well_name,
            field=well_data.field,
            field_name=well_data.field_name,
            project_code=well_data.project_code,
            company=well_data.company,
            pad_number=well_data.pad_number,
            category=well_data.category,
            ngdu=well_data.ngdu,
            well_category=well_data.well_category,
            completion_date=well_data.completion_date,
            metadata_=well_data.metadata_,
        )
        self.db.add(well)
        self.db.commit()
        self.db.refresh(well)
        return well

    def update_well(self, well_id: int, well_data: WellUpdate) -> Optional[Well]:
        well = self.get_well(well_id)
        if not well:
            return None

        update_data = well_data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(well, key, value)

        self.db.commit()
        self.db.refresh(well)
        return well

    def delete_well(self, well_id: int) -> bool:
        well = self.get_well(well_id)
        if not well:
            return False

        self.db.delete(well)
        self.db.commit()
        return True

    def get_or_create_well(
        self,
        well_number: str,
        project_code: str,
        company: str = "ПАО Татнефть",
        field: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> tuple[Well, bool]:
        well = self.get_well_by_number(well_number)
        if well:
            return well, False

        well = Well(
            well_number=well_number,
            project_code=project_code,
            company=company,
            field=field,
            metadata_=metadata,
        )
        self.db.add(well)
        self.db.commit()
        self.db.refresh(well)
        return well, True

    def create_wellbore(
        self,
        well_id: int,
        wellbore_number: str = "main",
        diameter_mm: Optional[float] = None,
        construction: Optional[str] = None,
        casing_diameter_mm: Optional[float] = None,
        tubing_diameter_mm: Optional[float] = None,
        gdi_data: Optional[str] = None,
        injectivity_coefficient: Optional[float] = None,
        circulation_character: Optional[str] = None,
        circulation_percent: Optional[float] = None,
        properties: Optional[dict] = None
    ) -> Wellbore:
        wellbore = Wellbore(
            well_id=well_id,
            wellbore_number=wellbore_number,
            diameter_mm=diameter_mm,
            construction=construction,
            casing_diameter_mm=casing_diameter_mm,
            tubing_diameter_mm=tubing_diameter_mm,
            gdi_data=gdi_data,
            injectivity_coefficient=injectivity_coefficient,
            circulation_character=circulation_character,
            circulation_percent=circulation_percent,
            properties=properties,
        )
        self.db.add(wellbore)
        self.db.commit()
        self.db.refresh(wellbore)
        return wellbore

    def get_wellbore_by_well(self, well_id: int, wellbore_number: str = "main") -> Optional[Wellbore]:
        return self.db.query(Wellbore).filter(
            Wellbore.well_id == well_id,
            Wellbore.wellbore_number == wellbore_number
        ).first()

    def get_or_create_wellbore(
        self,
        well_id: int,
        wellbore_number: str = "main",
        **kwargs
    ) -> tuple[Wellbore, bool]:
        wellbore = self.get_wellbore_by_well(well_id, wellbore_number)
        if wellbore:
            return wellbore, False

        wellbore = self.create_wellbore(well_id, wellbore_number, **kwargs)
        return wellbore, True

    def count_wellbores(self, well_id: int) -> int:
        return self.db.query(Wellbore).filter(Wellbore.well_id == well_id).count()

    def count_logs(self, well_id: int) -> int:
        return self.db.query(GtiLog).join(Wellbore).filter(
            Wellbore.well_id == well_id
        ).count()
