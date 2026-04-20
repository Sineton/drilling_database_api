"""
Excel import API router
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import Optional
import tempfile
import os

from ..database import get_db
from ..services.excel_parser import ExcelParserService
from ..services.import_service import ImportService
from ..schemas.import_schemas import (
    ExcelImportRequest,
    ExcelImportResponse,
    ExcelImportSummary,
    ExcelParseResponse,
    EventsImportRequest,
    EventsImportResponse
)

router = APIRouter(prefix="/import/excel", tags=["Import Excel"])


@router.post("/parse", response_model=ExcelParseResponse)
async def parse_excel(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    """Parse Excel file structure without importing"""
    
    if file:
        # Save uploaded file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        try:
            service = ExcelParserService(db)
            result = service.parse_excel_structure(tmp_path)
            return ExcelParseResponse(success=True, **result)
        finally:
            os.unlink(tmp_path)
    
    elif file_path:
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        service = ExcelParserService(db)
        result = service.parse_excel_structure(file_path)
        return ExcelParseResponse(success=True, **result)
    
    else:
        raise HTTPException(status_code=400, detail="Either file or file_path is required")


@router.post("/wells", response_model=ExcelImportResponse)
async def import_wells(
    request: ExcelImportRequest = None,
    file: Optional[UploadFile] = File(None),
    project_code: Optional[str] = Form(None),
    company: Optional[str] = Form("pao-tatneft"),
    create_wellbores: Optional[bool] = Form(True),
    dry_run: Optional[bool] = Form(False),
    db: Session = Depends(get_db)
):
    """Import wells from Excel file"""
    
    # Determine file path
    if file:
        # Save uploaded file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            content = await file.read()
            tmp.write(content)
            file_path = tmp.name
        cleanup_file = True
    elif request and request.file_path:
        file_path = request.file_path
        cleanup_file = False
        project_code = request.project_code
        company = request.company
        create_wellbores = request.create_wellbores
        dry_run = request.dry_run
    else:
        raise HTTPException(status_code=400, detail="Either file or file_path is required")
    
    if not project_code:
        raise HTTPException(status_code=400, detail="project_code is required")
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
    
    try:
        # Create job
        job_id = ImportService.create_job("wells")
        ImportService.update_job(job_id, status="processing")
        
        # Import
        service = ExcelParserService(db)
        result = service.import_wells(
            file_path=file_path,
            project_code=project_code,
            company=company,
            column_mapping=request.column_mapping if request else None,
            create_wellbores=create_wellbores,
            dry_run=dry_run
        )
        
        # Update job
        ImportService.update_job(job_id, status="completed", result=result)
        
        return ExcelImportResponse(
            success=True,
            job_id=job_id,
            summary=ExcelImportSummary(
                total_rows=result["total_rows"],
                wells_created=result["wells_created"],
                wells_updated=result["wells_updated"],
                wells_skipped=result["wells_skipped"],
                wellbores_created=result["wellbores_created"],
                errors=result["errors"]
            ),
            wells=result["wells"]
        )
        
    except Exception as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cleanup_file and os.path.exists(file_path):
            os.unlink(file_path)


@router.post("/events", response_model=EventsImportResponse)
async def import_events(
    request: EventsImportRequest = None,
    file: Optional[UploadFile] = File(None),
    annotation_source: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    """Import events/complications from Excel file"""
    
    # Determine file path
    if file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            content = await file.read()
            tmp.write(content)
            file_path = tmp.name
        cleanup_file = True
        source = annotation_source
    elif request and request.file_path:
        file_path = request.file_path
        cleanup_file = False
        source = request.annotation_source
    else:
        raise HTTPException(status_code=400, detail="Either file or file_path is required")
    
    if not source:
        raise HTTPException(status_code=400, detail="annotation_source is required")
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
    
    try:
        job_id = ImportService.create_job("events")
        ImportService.update_job(job_id, status="processing")
        
        service = ExcelParserService(db)
        result = service.import_events(
            file_path=file_path,
            annotation_source=source,
            column_mapping=request.column_mapping if request else None,
            complication_rules=request.complication_rules if request else None
        )
        
        ImportService.update_job(job_id, status="completed", result=result)
        
        return EventsImportResponse(
            success=True,
            job_id=job_id,
            summary={
                "total_rows": result["total_rows"],
                "events_created": result["events_created"],
                "events_by_type": result["events_by_type"]
            },
            events=result["events"]
        )
        
    except Exception as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cleanup_file and os.path.exists(file_path):
            os.unlink(file_path)
