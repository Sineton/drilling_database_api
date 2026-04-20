# API Documentation

FastAPI сервис для импорта и обработки данных по скважинам, LAS-файлам, Excel-реестрам, журналам супервайзера и разметке.

## Что умеет API

- управлять справочником скважин и стволов;
- импортировать LAS из локального пути, URL и пакетно из папки;
- импортировать Excel-файлы со скважинами и событиями;
- импортировать журналы супервайзера и разметку из `.xlsx`;
- строить выборки для аналитики и обучения.

## Быстрый старт

### 1. Установка

```bash
pip install -r requirements.txt
```

### 2. Настройка окружения

Скопируйте `.env.example` в `.env` и заполните свои значения:

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

### 3. Запуск

```bash
python run.py
```

После запуска:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- API base URL: `http://localhost:8000/api/v1`
- Health check: `http://localhost:8000/health`

## Основные эндпоинты

### Служебные

- `GET /` - краткая информация о сервисе
- `GET /health` - проверка доступности API

### Wells

- `GET /api/v1/wells/overview` - обзор активных скважин
- `GET /api/v1/wells/{well_number}/parameters` - параметры скважины по временному окну
- `GET /api/v1/wells` - список скважин с фильтрами
- `GET /api/v1/wells/{well_id}` - карточка скважины
- `POST /api/v1/wells` - создание скважины
- `PUT /api/v1/wells/{well_id}` - обновление скважины
- `DELETE /api/v1/wells/{well_id}` - удаление скважины
- `POST /api/v1/wells/{well_id}/wellbores` - создание ствола

### Events

- `GET /api/v1/events/types` - список типов событий
- `POST /api/v1/events/types` - создание типа события
- `GET /api/v1/events` - список событий с фильтрами
- `POST /api/v1/events` - создание события
- `GET /api/v1/events/{event_id}` - получение события
- `DELETE /api/v1/events/{event_id}` - удаление события
- `POST /api/v1/events/sync-from-supervisor` - заполнение событий из `sv_daily_operations`

### Import LAS

- `POST /api/v1/import/las/parse` - предварительный разбор LAS
- `POST /api/v1/import/las/from-url` - импорт LAS по URL
- `POST /api/v1/import/las` - импорт LAS по локальному пути
- `POST /api/v1/import/las/batch` - пакетный импорт из папки
- `GET /api/v1/import/las/status/{job_id}` - статус задания
- `GET /api/v1/import/las/batch/{batch_id}` - статус пакетного импорта
- `GET /api/v1/import/las/stream/{job_id}` - поток прогресса через SSE

### Import Excel

- `POST /api/v1/import/excel/parse` - предварительный разбор Excel
- `POST /api/v1/import/excel/wells` - импорт скважин
- `POST /api/v1/import/excel/events` - импорт событий

### Import Supervisor Journal

- `POST /api/v1/import/sv-journal/parse` - preview журнала
- `POST /api/v1/import/sv-journal/import` - полный импорт журнала
- `POST /api/v1/import/sv-journal/import-by-path` - импорт по пути из JSON
- `GET /api/v1/import/sv-journal/overview/{well_number}` - обзор по скважине
- `GET /api/v1/import/sv-journal/reports/{well_number}` - отчеты по скважине
- `GET /api/v1/import/sv-journal/operations/{report_id}` - операции из отчета
- `GET /api/v1/import/sv-journal/npv/{well_number}` - баланс НПВ
- `POST /api/v1/import/sv-journal/final/parse` - preview итогового файла
- `POST /api/v1/import/sv-journal/final/import` - импорт итогового файла
- `POST /api/v1/import/sv-journal/otchet/parse` - preview листа отчета
- `POST /api/v1/import/sv-journal/otchet/import` - импорт листа отчета

### Supervisor Events

- `POST /api/v1/sv-events/fill/{well_number}` - заполнить `events` по `sv_*`
- `POST /api/v1/sv-events/cleanup/{well_number}` - удалить автосгенерированные события
- `POST /api/v1/sv-events/rebuild/{well_number}` - пересобрать события по скважине
- `POST /api/v1/sv-events/diagnose/{well_number}` - диагностика причин пропуска событий

### Analytics and Datasets

- `GET /api/v1/analytics/anomalies` - аномалии по скважине
- `GET /api/v1/analytics/field-summary` - сводка по месторождению
- `POST /api/v1/datasets/stuck-pipe-training` - собрать датасет для обучения

### Import Markup

- `POST /api/v1/import/markup/parse` - preview файла разметки
- `POST /api/v1/import/markup` - импорт разметки в БД

## Примеры запросов

### Health check

```bash
curl http://localhost:8000/health
```

### Импорт LAS по URL

```bash
curl -X POST "http://localhost:8000/api/v1/import/las/from-url?url=https://example.com/file.las&well_number=10767A&create_well=true"
```

### Импорт LAS по локальному пути

```bash
curl -X POST "http://localhost:8000/api/v1/import/las" \
  -H "Content-Type: application/json" \
  -d "{\"file_path\":\"d:\\\\data\\\\file.las\",\"well_number\":\"10767A\",\"create_well\":true}"
```

## Важные замечания

- Интерактивная спецификация уже доступна через FastAPI в `docs` и `redoc`.
- Для публикации на GitHub не храните реальный `.env`, пароли БД и внутренние адреса в markdown или коде.
- Если нужно публиковать проект как отдельный репозиторий, достаточно выполнять git-команды из каталога `api`.
