"""
API Routers
"""
from .wells import router as wells_router
from .import_excel import router as import_excel_router
from .import_las import router as import_las_router
from .gti_snapshot_las import router as gti_snapshot_las_router
from .events import router as events_router
from .import_sv_journal import router as import_sv_journal_router
from .analytics import router as analytics_router
from .datasets import router as datasets_router
from .sv_events import router as sv_events_router
from .import_markup import router as import_markup_router

__all__ = [
    "wells_router",
    "import_excel_router",
    "import_las_router",
    "gti_snapshot_las_router",
    "events_router",
    "import_sv_journal_router",
    "analytics_router",
    "datasets_router",
    "sv_events_router",
    "import_markup_router",
]
