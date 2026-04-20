"""
PAK PA PPR Import API - Main Application
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from .config import settings
from .database import init_db, engine, Base
from .routers import (
    wells_router,
    import_excel_router,
    import_las_router,
    gti_snapshot_las_router,
    events_router,
    import_sv_journal_router,
    analytics_router,
    datasets_router,
    sv_events_router,
    import_markup_router,
)
from .models import *  # Import all models to register them


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    print("Starting up...")
    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)
    print("Database tables created/verified")
    
    # Seed event types if needed
    from .database import SessionLocal
    from .models import EventType, Operation
    
    db = SessionLocal()
    try:
        # Seed event types
        if db.query(EventType).count() == 0:
            event_types = [
                EventType(event_code="normal", event_name="Нормальное бурение", is_complication=False, severity=0, target_label=0),
                EventType(event_code="stuck_pipe", event_name="Прихват", is_complication=True, severity=3, target_label=1),
                EventType(event_code="circulation_loss", event_name="Потеря циркуляции", is_complication=True, severity=2, target_label=2),
                EventType(event_code="overflow", event_name="Перелив", is_complication=True, severity=2, target_label=3),
                EventType(event_code="absorption", event_name="Поглощение", is_complication=True, severity=2, target_label=4),
                EventType(event_code="kick", event_name="Газонефтеводопроявление", is_complication=True, severity=3, target_label=5),
            ]
            db.add_all(event_types)
            db.commit()
            print("Event types seeded")
        
        # Seed operations
        if db.query(Operation).count() == 0:
            operations = [
                Operation(operation_code="drilling", operation_name="Бурение", is_drilling=True, risk_level=1),
                Operation(operation_code="tripping_in", operation_name="Спуск инструмента", is_drilling=False, risk_level=2),
                Operation(operation_code="tripping_out", operation_name="Подъем инструмента", is_drilling=False, risk_level=2),
                Operation(operation_code="connection", operation_name="Наращивание", is_drilling=False, risk_level=1),
                Operation(operation_code="circulation", operation_name="Промывка", is_drilling=False, risk_level=1),
                Operation(operation_code="reaming", operation_name="Проработка", is_drilling=True, risk_level=2),
                Operation(operation_code="casing", operation_name="Спуск обсадной колонны", is_drilling=False, risk_level=2),
            ]
            db.add_all(operations)
            db.commit()
            print("Operations seeded")
            
    finally:
        db.close()
    
    yield
    
    # Shutdown
    print("Shutting down...")


# Create FastAPI app
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description="""
## PAK PA PPR Import API

API для импорта данных ГТИ (геолого-технологических исследований) в базу данных ПАК ПА и ППР.

### Возможности:

* **Импорт скважин** из Excel файлов
* **Импорт событий/осложнений** из Excel файлов  
* **Импорт данных ГТИ** из LAS файлов
* **Пакетный импорт** LAS файлов из папки
* **Импорт журналов супервайзера** из Excel файлов
* **Управление скважинами и стволами**
* **Управление событиями**

### Документация:

* [Swagger UI](/docs)
* [ReDoc](/redoc)
    """,
    openapi_url=f"{settings.api_prefix}/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(wells_router, prefix=settings.api_prefix)
app.include_router(import_excel_router, prefix=settings.api_prefix)
app.include_router(import_las_router, prefix=settings.api_prefix)
app.include_router(gti_snapshot_las_router, prefix=settings.api_prefix)
app.include_router(events_router, prefix=settings.api_prefix)
app.include_router(import_sv_journal_router, prefix=settings.api_prefix)
app.include_router(analytics_router, prefix=settings.api_prefix)
app.include_router(datasets_router, prefix=settings.api_prefix)
app.include_router(sv_events_router, prefix=settings.api_prefix)
app.include_router(import_markup_router, prefix=settings.api_prefix)


@app.get("/")
def root():
    """Root endpoint"""
    return {
        "message": "PAK PA PPR Import API",
        "version": settings.api_version,
        "docs": "/docs"
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


# Run with: uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
