"""
LAS import API router
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional, List
import tempfile
import os
import httpx
import asyncio
import json

from ..database import get_db, SessionLocal
from ..services.las_parser import LASParserService
from ..services.import_service import ImportService
from ..schemas.import_schemas import (
    LASImportRequest,
    LASImportResponse,
    LASParseResponse,
    LASFileInfo,
    LASMappingResult,
    ImportProgress,
    LASBatchImportRequest,
    LASBatchImportResponse,
    BatchSummary,
    BatchFileStatus,
    ImportJobStatus
)

router = APIRouter(prefix="/import/las", tags=["Import LAS"])


@router.post("/parse", response_model=LASParseResponse)
async def parse_las(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    """Parse LAS file structure without importing"""
    
    if file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".las") as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        try:
            service = LASParserService(db)
            result = service.parse_las_structure(tmp_path)
            return LASParseResponse(success=True, **result)
        finally:
            os.unlink(tmp_path)
    
    elif file_path:
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        service = LASParserService(db)
        result = service.parse_las_structure(file_path)
        return LASParseResponse(success=True, **result)
    
    else:
        raise HTTPException(status_code=400, detail="Either file or file_path is required")


def process_url_import(job_id: str, url: str, temp_file: str, well_number: Optional[str], create_well: bool):
    """Background task for URL import with progress tracking"""
    db = SessionLocal()
    try:
        service = LASParserService(db)
        
        # Parse file structure
        parse_result = service.parse_las_structure(temp_file)
        
        # Update job with file info
        ImportService.update_job(job_id, progress={
            "stage": "parsed",
            "progress": 10,
            "message": f"File parsed: {parse_result['statistics']['total_records']:,} records found",
            "file_info": {
                "filename": url.split('/')[-1],
                "las_version": parse_result["las_version"],
                "well_name": parse_result["well_info"].get("WELL"),
                "records": parse_result["statistics"]["total_records"]
            }
        })
        
        # Import data with progress tracking
        def update_progress(progress_data):
            ImportService.update_job(job_id, progress=progress_data)
        
        result = service.import_las(
            file_path=temp_file,
            well_number=well_number,
            create_well=create_well,
            channel_mapping=None,
            unit_conversions=None,
            batch_size=50000,
            progress_callback=update_progress
        )
        
        ImportService.update_job(job_id, status="completed", result=result)
        
    except Exception as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
    finally:
        db.close()
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass


@router.post("/from-url")
async def import_las_from_url(
    url: str,
    well_number: Optional[str] = None,
    create_well: bool = False,
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Import LAS file from URL with real-time progress tracking"""
    
    # Download file from URL
    temp_file = None
    try:
        # Update: Downloading (avoid printing Cyrillic on Windows console)
        # print(f"Downloading from: {url}")
        
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(300.0, connect=60.0),
            follow_redirects=True
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            
            # Save to temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".las") as tmp:
                tmp.write(response.content)
                temp_file = tmp.name
        
        # Create job
        job_id = ImportService.create_job("las_url")
        ImportService.update_job(job_id, status="processing", progress={
            "stage": "downloading",
            "progress": 5,
            "message": "File downloaded, starting import..."
        })
        
        # Start background import
        background_tasks.add_task(
            process_url_import,
            job_id, url, temp_file, well_number, create_well
        )
        
        # Return immediately so client can stream progress
        return {
            "success": True,
            "job_id": job_id,
            "status": "processing",
            "message": "Import started. Use /stream/{job_id} or /status/{job_id} to monitor progress."
        }
        
    except httpx.HTTPError as e:
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)
        raise HTTPException(status_code=400, detail=f"Failed to download file: {str(e)}")
    except Exception as e:
        if temp_file and os.path.exists(temp_file):
            os.unlink(temp_file)
        raise HTTPException(status_code=500, detail=str(e))


def process_file_import(job_id: str, file_path: str, well_number: Optional[str], create_well: bool, 
                        channel_mapping: Optional[dict], unit_conversions: Optional[dict], 
                        batch_size: int, cleanup_file: bool):
    """Background task for file import with progress tracking"""
    db = SessionLocal()
    try:
        service = LASParserService(db)
        
        # Parse file structure
        parse_result = service.parse_las_structure(file_path)
        
        # Update job with file info
        ImportService.update_job(job_id, progress={
            "stage": "parsed",
            "progress": 10,
            "message": f"File parsed: {parse_result['statistics']['total_records']:,} records found",
            "file_info": {
                "filename": os.path.basename(file_path),
                "las_version": parse_result["las_version"],
                "well_name": parse_result["well_info"].get("WELL"),
                "records": parse_result["statistics"]["total_records"]
            }
        })
        
        # Import data with progress tracking
        def update_progress(progress_data):
            ImportService.update_job(job_id, progress=progress_data)
        
        result = service.import_las(
            file_path=file_path,
            well_number=well_number,
            create_well=create_well,
            channel_mapping=channel_mapping,
            unit_conversions=unit_conversions,
            batch_size=batch_size,
            progress_callback=update_progress
        )
        
        ImportService.update_job(job_id, status="completed", result=result)
        
    except Exception as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
    finally:
        db.close()
        if cleanup_file and file_path and os.path.exists(file_path):
            try:
                os.unlink(file_path)
            except:
                pass


@router.post("")
async def import_las(
    background_tasks: BackgroundTasks,
    request: LASImportRequest = None,
    db: Session = Depends(get_db)
):
    """Import LAS file with real-time progress tracking"""
    
    # Check if request body provided
    if not request or not request.file_path:
        raise HTTPException(status_code=400, detail="file_path is required in request body")
    
    file_path = request.file_path
    cleanup_file = False
    wn = request.well_number
    cw = request.create_well
    channel_mapping = request.channel_mapping
    unit_conversions = request.unit_conversions
    batch_size = request.batch_size or 50000
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
    
    try:
        job_id = ImportService.create_job("las")
        ImportService.update_job(job_id, status="processing", progress={
            "stage": "starting",
            "progress": 5,
            "message": f"Starting import of {os.path.basename(file_path)}..."
        })
        
        # Start background import
        background_tasks.add_task(
            process_file_import,
            job_id, file_path, wn, cw, channel_mapping, unit_conversions, batch_size, cleanup_file
        )
        
        # Return immediately so client can stream progress
        return {
            "success": True,
            "job_id": job_id,
            "status": "processing",
            "message": "Import started. Use /stream/{job_id} or /status/{job_id} to monitor progress."
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def process_batch_import(batch_id: str, request: LASBatchImportRequest):
    """Background task for batch import"""
    db = SessionLocal()
    try:
        service = LASParserService(db)
        
        # Get files
        files = ImportService.list_files_in_folder(
            request.folder_path,
            request.file_pattern,
            request.recursive
        )
        
        completed = 0
        failed = 0
        
        for file_path in files:
            try:
                well_number = None
                if request.well_number_from_folder:
                    well_number = ImportService.extract_well_number_from_path(file_path)
                
                service.import_las(
                    file_path=file_path,
                    well_number=well_number,
                    create_well=request.create_wells,
                    channel_mapping=request.channel_mapping
                )
                completed += 1
                
            except Exception as e:
                failed += 1
        
        ImportService.update_job(
            batch_id,
            status="completed",
            result={"completed": completed, "failed": failed}
        )
        
    except Exception as e:
        ImportService.update_job(batch_id, status="failed", error=str(e))
    finally:
        db.close()


@router.post("/batch", response_model=LASBatchImportResponse)
async def import_las_batch(
    request: LASBatchImportRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Batch import LAS files from folder"""
    
    if not os.path.exists(request.folder_path):
        raise HTTPException(status_code=404, detail=f"Folder not found: {request.folder_path}")
    
    # Get file list
    files = ImportService.list_files_in_folder(
        request.folder_path,
        request.file_pattern,
        request.recursive
    )
    
    if not files:
        raise HTTPException(status_code=400, detail="No files found matching pattern")
    
    # Create batch job
    batch_id = ImportService.create_job("las_batch")
    
    # Prepare file statuses
    file_statuses = []
    for file_path in files:
        well_number = None
        if request.well_number_from_folder:
            well_number = ImportService.extract_well_number_from_path(file_path)
        
        file_statuses.append(BatchFileStatus(
            file_path=file_path.replace(request.folder_path, "").lstrip("/\\"),
            well_number=well_number,
            status="queued"
        ))
    
    # Start background processing
    background_tasks.add_task(process_batch_import, batch_id, request)
    
    return LASBatchImportResponse(
        success=True,
        batch_id=batch_id,
        status="processing",
        summary=BatchSummary(
            total_files=len(files),
            queued=len(files),
            processing=0,
            completed=0,
            failed=0
        ),
        files=file_statuses,
        status_url=f"/api/v1/import/batch/{batch_id}"
    )


@router.get("/status/{job_id}")
def get_import_status(job_id: str):
    """Get import job status"""
    job = ImportService.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job


@router.get("/batch/{batch_id}")
def get_batch_status(batch_id: str):
    """Get batch import status"""
    job = ImportService.get_job(batch_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Batch job not found")
    
    return job


@router.get("/stream/{job_id}")
async def stream_import_progress(job_id: str):
    """Stream import progress using Server-Sent Events (SSE)"""
    
    async def event_generator():
        """Generate SSE events with progress updates"""
        try:
            last_progress = None
            
            while True:
                job = ImportService.get_job(job_id)
                
                if not job:
                    yield f"data: {json.dumps({'error': 'Job not found'})}\n\n"
                    break
                
                # Send progress update if changed
                current_progress = job.get("progress")
                if current_progress != last_progress:
                    yield f"data: {json.dumps(current_progress or {'message': 'Waiting...'})}\n\n"
                    last_progress = current_progress
                
                # Check if job completed or failed
                if job["status"] in ("completed", "failed"):
                    yield f"data: {json.dumps({'status': job['status'], 'result': job.get('result'), 'error': job.get('error')})}\n\n"
                    break
                
                await asyncio.sleep(0.5)  # Check every 500ms
                
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
