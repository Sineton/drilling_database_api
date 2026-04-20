"""
Markup xlsx import API router.
"""
import os
import tempfile
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from ..database import get_db
from ..schemas.markup_import import (
    MarkupImportResponse,
    MarkupImportSummary,
    MarkupParseResponse,
    MarkupParseSummary,
)
from ..services.import_service import ImportService
from ..services.markup_import_service import MarkupImportService

router = APIRouter(prefix="/import/markup", tags=["Import Markup"])


@router.post("/parse", response_model=MarkupParseResponse)
async def parse_markup_file(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    well_number: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """Parse markup xlsx without writing to the database."""
    actual_path, cleanup = await _resolve_file(file, file_path)

    try:
        service = MarkupImportService(db)
        result = service.parse_preview(
            file_path=actual_path,
            well_number_override=well_number,
        )
        return MarkupParseResponse(
            success=True,
            summary=MarkupParseSummary(**result),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ошибка разбора файла разметки: {exc}")
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


@router.post("", response_model=MarkupImportResponse)
async def import_markup_file(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    well_number: Optional[str] = Form(None),
    project_code: Optional[str] = Form(None),
    annotation_source: str = Form("markup_xlsx"),
    dry_run: bool = Form(False),
    db: Session = Depends(get_db),
):
    """Import markup xlsx into operations, events, geology_intervals and markup summary rows."""
    actual_path, cleanup = await _resolve_file(file, file_path)
    job_id = ImportService.create_job("markup")
    ImportService.update_job(job_id, status="processing")

    try:
        service = MarkupImportService(db)
        result = service.import_markup(
            file_path=actual_path,
            dry_run=dry_run,
            well_number_override=well_number,
            project_code=project_code,
            annotation_source=annotation_source,
        )
        ImportService.update_job(job_id, status="completed", result=result)
        return MarkupImportResponse(
            success=True,
            job_id=job_id,
            summary=MarkupImportSummary(**result),
        )
    except ValueError as exc:
        db.rollback()
        ImportService.update_job(job_id, status="failed", error=str(exc))
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        db.rollback()
        ImportService.update_job(job_id, status="failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Ошибка импорта файла разметки: {exc}")
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


async def _resolve_file(
    file: Optional[UploadFile],
    file_path: Optional[str],
) -> tuple[str, bool]:
    if file is not None:
        suffix = os.path.splitext(file.filename or "markup.xlsx")[1] or ".xlsx"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            return tmp.name, True

    if file_path:
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"Файл не найден: {file_path}")
        return file_path, False

    raise HTTPException(status_code=400, detail="Требуется передать file или file_path")
