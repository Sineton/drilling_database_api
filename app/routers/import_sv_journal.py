"""
Supervisor journal import API router.

Endpoints for parsing and importing supervisor daily drilling journals
from xlsx files into the sv_* tables.
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query
from sqlalchemy.orm import Session
from typing import Optional
import tempfile
import os

from ..database import get_db
from ..services.detail_actual_operations_import_service import (
    DetailActualOperationsImportService,
)
from ..services.sv_journal_parser import SvJournalParserService
from ..services.sv_final_journal_parser import SvFinalJournalParserService
from ..services.sv_otchet_sheet_parser import SvOtchetSheetParserService
from ..services.import_service import ImportService
from ..schemas.detail_actual_operations_import import (
    DetailActualOperationsImportResponse,
    DetailActualOperationsImportSummary,
)
from ..schemas.sv_journal import (
    SvJournalImportRequest,
    SvJournalParseResponse,
    SvJournalImportResponse,
    SvJournalImportSummary,
    DailyReportSummary,
    SvJournalOverview,
    SvDailyReportDetail,
    SvDailyOperationDetail,
    SvNpvBalanceDetail,
)
from ..models import (
    SvDailyReport, SvDailyOperation, SvNpvBalance, SvBhaRun,
    Well, Wellbore,
)

router = APIRouter(prefix="/import/sv-journal", tags=["Import Supervisor Journal"])


@router.post("/parse", response_model=SvJournalParseResponse)
async def parse_sv_journal(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """
    Предварительный разбор журнала супервайзера (без записи в БД).
    Возвращает информацию о скважине, количество ежедневных блоков,
    диапазон дат и количество записей НПВ.
    """
    actual_path, cleanup = await _resolve_file(file, file_path)

    try:
        service = SvJournalParserService(db)
        result = service.parse_preview(actual_path)
        return SvJournalParseResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


@router.post("/import", response_model=SvJournalImportResponse)
async def import_sv_journal(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    project_code: Optional[str] = Form("pao-tatneft"),
    well_number: Optional[str] = Form(None),
    dry_run: Optional[bool] = Form(False),
    db: Session = Depends(get_db),
):
    """
    Полный импорт журнала супервайзера в БД.

    Парсит xlsx-файл и заносит данные в таблицы:
    - wells / wellbores (обновление паспорта)
    - sv_daily_reports (ежедневные отчёты)
    - sv_daily_operations (технологические операции)
    - sv_bha_runs (КНБК)
    - sv_drilling_regime (режимы бурения)
    - sv_mud_accounting (учёт раствора)
    - sv_chemical_reagents (хим. реагенты)
    - sv_npv_balance (баланс НПВ)
    - sv_contractors (подрядчики)
    - sv_well_construction (конструкция скважины)
    - sv_rig_equipment (буровое оборудование)
    - sv_construction_timing (план/факт длительность)
    """
    actual_path, cleanup = await _resolve_file(file, file_path)

    job_id = ImportService.create_job("sv_journal")
    ImportService.update_job(job_id, status="processing")

    try:
        service = SvJournalParserService(db)
        result = service.import_journal(
            file_path=actual_path,
            project_code=project_code,
            well_number_override=well_number,
            dry_run=dry_run,
        )

        ImportService.update_job(job_id, status="completed", result=result)

        daily_summaries = [
            DailyReportSummary(**ds)
            for ds in result.get("daily_summaries", [])
        ]

        summary = SvJournalImportSummary(
            well_id=result.get("well_id", 0),
            well_number=result["well_number"],
            wellbore_id=result.get("wellbore_id", 0),
            daily_reports_created=result["daily_reports_created"],
            operations_created=result["operations_created"],
            bha_runs_created=result["bha_runs_created"],
            drilling_regimes_created=result["drilling_regimes_created"],
            mud_accounting_created=result["mud_accounting_created"],
            chemical_reagents_created=result["chemical_reagents_created"],
            npv_records_created=result["npv_records_created"],
            contractors_created=result["contractors_created"],
            construction_items_created=result["construction_items_created"],
            equipment_created=result["equipment_created"],
            timing_records_created=result["timing_records_created"],
            warnings=result.get("warnings", []),
            errors=result.get("errors", []),
        )

        return SvJournalImportResponse(
            success=True,
            job_id=job_id,
            summary=summary,
            daily_reports=daily_summaries,
        )

    except ValueError as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Ошибка импорта: {str(e)}")
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


@router.post("/import-by-path")
async def import_sv_journal_by_path(
    request: SvJournalImportRequest,
    db: Session = Depends(get_db),
):
    """
    Импорт журнала по JSON-запросу (путь на сервере).
    """
    if not request.file_path:
        raise HTTPException(status_code=400, detail="file_path is required")
    if not os.path.exists(request.file_path):
        raise HTTPException(status_code=404, detail=f"Файл не найден: {request.file_path}")

    job_id = ImportService.create_job("sv_journal")
    ImportService.update_job(job_id, status="processing")

    try:
        service = SvJournalParserService(db)
        result = service.import_journal(
            file_path=request.file_path,
            project_code=request.project_code,
            well_number_override=request.well_number,
            dry_run=request.dry_run,
        )

        ImportService.update_job(job_id, status="completed", result=result)

        return {
            "success": True,
            "job_id": job_id,
            "summary": result,
        }

    except ValueError as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Ошибка импорта: {str(e)}")


# ==================================================================
# READ endpoints — просмотр импортированных данных
# ==================================================================

@router.get("/overview/{well_number}", response_model=SvJournalOverview)
def get_journal_overview(
    well_number: str,
    db: Session = Depends(get_db),
):
    """Обзор загруженного журнала по номеру скважины."""
    well = db.query(Well).filter(Well.well_number == well_number).first()
    if not well:
        raise HTTPException(status_code=404, detail=f"Скважина {well_number} не найдена")

    wellbore = db.query(Wellbore).filter(
        Wellbore.well_id == well.well_id,
        Wellbore.wellbore_number == "main",
    ).first()
    if not wellbore:
        raise HTTPException(status_code=404, detail="Ствол не найден")

    reports = db.query(SvDailyReport).filter(
        SvDailyReport.wellbore_id == wellbore.wellbore_id,
    ).order_by(SvDailyReport.report_date).all()

    total_ops = db.query(SvDailyOperation).filter(
        SvDailyOperation.report_id.in_([r.report_id for r in reports])
    ).count() if reports else 0

    total_npv = db.query(SvNpvBalance).filter(
        SvNpvBalance.wellbore_id == wellbore.wellbore_id,
    ).count()

    total_bha = db.query(SvBhaRun).filter(
        SvBhaRun.report_id.in_([r.report_id for r in reports])
    ).count() if reports else 0

    date_range = None
    if reports:
        date_range = {
            "from": reports[0].report_date.isoformat(),
            "to": reports[-1].report_date.isoformat(),
        }

    stages = list(set(
        r.construction_stage for r in reports if r.construction_stage
    ))

    max_depth = max(
        (r.current_depth_m for r in reports if r.current_depth_m), default=None
    )

    return SvJournalOverview(
        well_id=well.well_id,
        well_number=well.well_number,
        wellbore_id=wellbore.wellbore_id,
        total_reports=len(reports),
        date_range=date_range,
        total_operations=total_ops,
        total_npv=total_npv,
        total_bha=total_bha,
        construction_stages=stages,
        max_depth_m=max_depth,
    )


@router.get("/reports/{well_number}", response_model=list[SvDailyReportDetail])
def get_daily_reports(
    well_number: str,
    date_from: Optional[str] = Query(None, description="Дата от (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Дата до (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
):
    """Список ежедневных отчётов по скважине."""
    well = db.query(Well).filter(Well.well_number == well_number).first()
    if not well:
        raise HTTPException(status_code=404, detail="Скважина не найдена")

    wellbore = db.query(Wellbore).filter(
        Wellbore.well_id == well.well_id,
        Wellbore.wellbore_number == "main",
    ).first()
    if not wellbore:
        raise HTTPException(status_code=404, detail="Ствол не найден")

    query = db.query(SvDailyReport).filter(
        SvDailyReport.wellbore_id == wellbore.wellbore_id,
    )
    if date_from:
        query = query.filter(SvDailyReport.report_date >= date_from)
    if date_to:
        query = query.filter(SvDailyReport.report_date <= date_to)

    reports = query.order_by(SvDailyReport.report_date).all()
    return [SvDailyReportDetail.model_validate(r) for r in reports]


@router.get("/operations/{report_id}", response_model=list[SvDailyOperationDetail])
def get_operations(
    report_id: int,
    db: Session = Depends(get_db),
):
    """Технологические операции за конкретный день (report_id)."""
    ops = db.query(SvDailyOperation).filter(
        SvDailyOperation.report_id == report_id,
    ).order_by(SvDailyOperation.sequence_number).all()
    return [SvDailyOperationDetail.model_validate(o) for o in ops]


@router.get("/npv/{well_number}", response_model=list[SvNpvBalanceDetail])
def get_npv_balance(
    well_number: str,
    db: Session = Depends(get_db),
):
    """Баланс НПВ по скважине."""
    well = db.query(Well).filter(Well.well_number == well_number).first()
    if not well:
        raise HTTPException(status_code=404, detail="Скважина не найдена")

    wellbore = db.query(Wellbore).filter(
        Wellbore.well_id == well.well_id,
        Wellbore.wellbore_number == "main",
    ).first()
    if not wellbore:
        raise HTTPException(status_code=404, detail="Ствол не найден")

    items = db.query(SvNpvBalance).filter(
        SvNpvBalance.wellbore_id == wellbore.wellbore_id,
    ).order_by(SvNpvBalance.incident_date).all()
    return [SvNpvBalanceDetail.model_validate(i) for i in items]


# ==================================================================
# Мультилистовый final.xlsx (Баланс, Детализация, …)
# ==================================================================


@router.post("/final/parse")
async def parse_final_journal(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """Предпросмотр разбора final.xlsx (листы Баланс, График, Детализация, Инциденты)."""
    actual_path, cleanup = await _resolve_file(file, file_path)
    try:
        svc = SvFinalJournalParserService(db)
        return svc.parse_preview(actual_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


@router.post("/final/import")
async def import_final_journal(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    project_code: Optional[str] = Form("pao-tatneft"),
    well_number: Optional[str] = Form(None),
    dry_run: Optional[bool] = Form(False),
    db: Session = Depends(get_db),
):
    """Импорт final.xlsx в wells, sv_daily_reports, sv_daily_operations, sv_npv_balance."""
    actual_path, cleanup = await _resolve_file(file, file_path)
    job_id = ImportService.create_job("sv_journal_final")
    ImportService.update_job(job_id, status="processing")
    try:
        svc = SvFinalJournalParserService(db)
        result = svc.import_journal(
            file_path=actual_path,
            project_code=project_code,
            well_number_override=well_number,
            dry_run=dry_run,
        )
        ImportService.update_job(job_id, status="completed", result=result)
        return {"success": True, "job_id": job_id, "summary": result}
    except ValueError as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


@router.post(
    "/detail/actual-operations/import",
    response_model=DetailActualOperationsImportResponse,
)
async def import_detail_sheet_to_actual_operations(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    project_code: Optional[str] = Form(None),
    well_number: Optional[str] = Form(None),
    replace_existing: bool = Form(True),
    dry_run: bool = Form(False),
    db: Session = Depends(get_db),
):
    """Импорт только листа 'Детализация' в таблицу actual_operations."""
    actual_path, cleanup = await _resolve_file(file, file_path)
    job_id = ImportService.create_job("detail_actual_operations")
    ImportService.update_job(job_id, status="processing")

    try:
        service = DetailActualOperationsImportService(db)
        result = service.import_sheet(
            file_path=actual_path,
            dry_run=dry_run,
            well_number_override=well_number,
            project_code=project_code,
            replace_existing=replace_existing,
        )
        ImportService.update_job(job_id, status="completed", result=result)
        return DetailActualOperationsImportResponse(
            success=True,
            job_id=job_id,
            summary=DetailActualOperationsImportSummary(**result),
        )
    except ValueError as e:
        db.rollback()
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        ImportService.update_job(job_id, status="failed", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка импорта листа 'Детализация' в actual_operations: {str(e)}",
        )
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


# ==================================================================
# Лист «Отчёт» (КНБК, режим, раствор, реагенты, подрядчики)
# ==================================================================


@router.post("/otchet/parse")
async def parse_otchet_sheet(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    sheet_name: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """Предпросмотр разбора листа «Отчёт» без записи в БД."""
    actual_path, cleanup = await _resolve_file(file, file_path)
    try:
        svc = SvOtchetSheetParserService(db)
        return svc.parse_preview(actual_path, sheet_name=sheet_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


@router.post("/otchet/import")
async def import_otchet_sheet(
    file: Optional[UploadFile] = File(None),
    file_path: Optional[str] = Form(None),
    well_number: str = Form(..., description="Номер скважины"),
    report_id: int = Form(..., description="ID sv_daily_reports, к которому привязать данные"),
    sheet_name: Optional[str] = Form(None),
    replace_existing: bool = Form(True),
    import_construction: bool = Form(False),
    db: Session = Depends(get_db),
):
    """
    Импорт листа «Отчёт» в sv_contractors, sv_rig_equipment, sv_bha_runs,
    sv_drilling_regime, sv_mud_accounting, sv_chemical_reagents;
    опционально sv_well_construction (import_construction=true).

    Сначала создайте суточный отчёт (например через /final/import) и укажите его report_id.
    """
    actual_path, cleanup = await _resolve_file(file, file_path)
    try:
        well = db.query(Well).filter(Well.well_number == well_number).first()
        if not well:
            raise HTTPException(status_code=404, detail=f"Скважина {well_number} не найдена")
        wellbore = db.query(Wellbore).filter(
            Wellbore.well_id == well.well_id,
            Wellbore.wellbore_number == "main",
        ).first()
        if not wellbore:
            raise HTTPException(status_code=404, detail="Ствол не найден")

        rep = db.query(SvDailyReport).filter(SvDailyReport.report_id == report_id).first()
        if not rep or rep.wellbore_id != wellbore.wellbore_id:
            raise HTTPException(
                status_code=400,
                detail="report_id не найден или не относится к этой скважине",
            )

        svc = SvOtchetSheetParserService(db)
        result = svc.import_sheet(
            actual_path,
            wellbore.wellbore_id,
            report_id,
            sheet_name=sheet_name,
            replace_existing=replace_existing,
            import_construction=import_construction,
        )
        return {"success": True, "summary": result}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cleanup and os.path.exists(actual_path):
            os.unlink(actual_path)


# ==================================================================
# Helper
# ==================================================================

async def _resolve_file(
    file: Optional[UploadFile],
    file_path: Optional[str],
) -> tuple[str, bool]:
    """Resolve uploaded file or server path. Returns (path, needs_cleanup)."""
    if file:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            content = await file.read()
            tmp.write(content)
            return tmp.name, True
    elif file_path:
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"Файл не найден: {file_path}")
        return file_path, False
    else:
        raise HTTPException(status_code=400, detail="Необходимо указать file или file_path")
