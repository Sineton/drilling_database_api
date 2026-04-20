# Drilling Database API

FastAPI service for importing and processing well data, LAS files, Excel registries, supervisor journals, and markup datasets.

## Features

- Manage wells and wellbores.
- Import LAS files from local paths, URLs, or folders in batch mode.
- Import wells and events from Excel files.
- Import supervisor journals and markup data from `.xlsx` files.
- Build datasets for analytics and ML workflows.

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and update the values for your environment:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=True
API_TITLE=PAK PA PPR Import API
API_VERSION=1.0.0
API_PREFIX=/api/v1
IMPORT_BATCH_SIZE=10000
IMPORT_MAX_PARALLEL_JOBS=4
DATA_DIR=d:\IPNG_NEW\Project\new\temp_extract
```

### 3. Run the API

```bash
python run.py
```

After startup:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- API base URL: `http://localhost:8000/api/v1`
- Health check: `http://localhost:8000/health`

## Main Endpoints

### Service

- `GET /` - basic service information
- `GET /health` - health check

### Wells

- `GET /api/v1/wells/overview` - overview of active wells
- `GET /api/v1/wells/{well_number}/parameters` - well parameters for a time window
- `GET /api/v1/wells` - list wells with filters
- `GET /api/v1/wells/{well_id}` - get well details
- `POST /api/v1/wells` - create a well
- `PUT /api/v1/wells/{well_id}` - update a well
- `DELETE /api/v1/wells/{well_id}` - delete a well
- `POST /api/v1/wells/{well_id}/wellbores` - create a wellbore

### Events

- `GET /api/v1/events/types` - list event types
- `POST /api/v1/events/types` - create an event type
- `GET /api/v1/events` - list events with filters
- `POST /api/v1/events` - create an event
- `GET /api/v1/events/{event_id}` - get event details
- `DELETE /api/v1/events/{event_id}` - delete an event
- `POST /api/v1/events/sync-from-supervisor` - create events from `sv_daily_operations`

### LAS Import

- `POST /api/v1/import/las/parse` - preview a LAS file
- `POST /api/v1/import/las/from-url` - import a LAS file from URL
- `POST /api/v1/import/las` - import a LAS file from local path
- `POST /api/v1/import/las/batch` - batch import LAS files from a folder
- `GET /api/v1/import/las/status/{job_id}` - get job status
- `GET /api/v1/import/las/batch/{batch_id}` - get batch import status
- `GET /api/v1/import/las/stream/{job_id}` - stream progress via SSE

### Excel Import

- `POST /api/v1/import/excel/parse` - preview an Excel file
- `POST /api/v1/import/excel/wells` - import wells from Excel
- `POST /api/v1/import/excel/events` - import events from Excel

### Supervisor Journal Import

- `POST /api/v1/import/sv-journal/parse` - preview a supervisor journal
- `POST /api/v1/import/sv-journal/import` - import a supervisor journal
- `POST /api/v1/import/sv-journal/import-by-path` - import by server-side file path
- `GET /api/v1/import/sv-journal/overview/{well_number}` - journal overview by well
- `GET /api/v1/import/sv-journal/reports/{well_number}` - reports by well
- `GET /api/v1/import/sv-journal/operations/{report_id}` - operations from a report
- `GET /api/v1/import/sv-journal/npv/{well_number}` - NPV balance by well
- `POST /api/v1/import/sv-journal/final/parse` - preview final journal file
- `POST /api/v1/import/sv-journal/final/import` - import final journal file
- `POST /api/v1/import/sv-journal/otchet/parse` - preview report sheet
- `POST /api/v1/import/sv-journal/otchet/import` - import report sheet

### Supervisor Events

- `POST /api/v1/sv-events/fill/{well_number}` - fill `events` from `sv_*` tables
- `POST /api/v1/sv-events/cleanup/{well_number}` - remove auto-generated events
- `POST /api/v1/sv-events/rebuild/{well_number}` - rebuild events for a well
- `POST /api/v1/sv-events/diagnose/{well_number}` - diagnose missing event creation

### Analytics and Datasets

- `GET /api/v1/analytics/anomalies` - get anomalies for a well
- `GET /api/v1/analytics/field-summary` - field summary
- `POST /api/v1/datasets/stuck-pipe-training` - build a stuck pipe training dataset

### Markup Import

- `POST /api/v1/import/markup/parse` - preview a markup file
- `POST /api/v1/import/markup` - import markup into the database

## Example Requests

### Health check

```bash
curl http://localhost:8000/health
```

### Import LAS from URL

```bash
curl -X POST "http://localhost:8000/api/v1/import/las/from-url?url=https://example.com/file.las&well_number=10767A&create_well=true"
```

### Import LAS from local path

```bash
curl -X POST "http://localhost:8000/api/v1/import/las" \
  -H "Content-Type: application/json" \
  -d "{\"file_path\":\"d:\\\\data\\\\file.las\",\"well_number\":\"10767A\",\"create_well\":true}"
```

## Notes

- Interactive API docs are available via FastAPI at `docs` and `redoc`.
- Do not store real `.env` files, production database credentials, or internal addresses in the repository.
- Use git commands from the `api` directory when maintaining this project as a standalone repository.
- Release history is tracked in `CHANGELOG.md`.
