# Aiven Document Extraction

Realtime logistics document extraction built with FastAPI, Gemini, and Aiven services.

This project is an open document-processing system for logistics PDFs such as bills of lading, invoices, purchase orders, delivery receipts, and similar operational documents. It combines AI extraction with an event-driven backend so users can watch job progress live instead of waiting on a black-box upload flow.

## Overview

The application is designed around three core Aiven services:

- `Aiven for Apache Kafka`: asynchronous job queue and progress event backbone
- `Aiven for PostgreSQL`: durable job state, extracted results, usage logs, and event history
- `Aiven for Valkey`: low-latency live job state for streaming UI updates

The extraction layer uses Vertex AI Gemini with a generic logistics prompt and schema so the system can handle multiple logistics document types with a single pipeline.

## Features

- PDF upload API for one or more documents
- Generic logistics-document extraction with a single prompt and schema
- Event-driven processing pipeline backed by Aiven Kafka
- Durable job and event history in Aiven PostgreSQL
- Live job state mirrored into Aiven Valkey
- Server-Sent Events endpoint for realtime frontend updates
- Polling endpoints for job state and event history
- Token and response-time tracking for LLM usage

## Architecture

```text
User Upload
   |
   v
FastAPI API
   |
   +--> PostgreSQL: create extraction_jobs row
   |
   +--> Kafka: publish upload job
            |
            v
      Document Worker
            |
            +--> Kafka: publish progress and result events
            +--> Valkey: cache latest job state for the UI
            +--> PostgreSQL: persist results and event history
            |
            v
      SSE / API Layer
            |
            +--> stream live updates to the frontend
            |
            v
        Live Processing UI
```


## Repository Structure

- [app.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/app.py): FastAPI app entrypoint and lifecycle management
- [src/api/endpoints.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/api/endpoints.py): upload, job status, event history, and SSE endpoints
- [src/api/routes.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/api/routes.py): API route registration
- [src/services/document_pipeline.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/services/document_pipeline.py): queue orchestration, event recording, and extraction worker flow
- [src/services/kafka_service.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/services/kafka_service.py): Aiven Kafka producer integration
- [src/services/valkey_service.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/services/valkey_service.py): live state caching and event mirroring
- [src/services/vertex_ai_service.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/services/vertex_ai_service.py): schema loading and extraction orchestration
- [src/core/extractors/pdf_extractor.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/core/extractors/pdf_extractor.py): grouped PDF extraction pipeline
- [src/core/processing/prompts.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/core/processing/prompts.py): shared extraction prompt
- [data/schemas/schema.json](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/data/schemas/schema.json): generic output schema
- [src/models/extraction_job.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/models/extraction_job.py): extraction job model
- [src/models/job_event.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/models/job_event.py): durable event history model
- [src/models/llm_usage.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/src/models/llm_usage.py): LLM usage log model
- [alembic/versions/0001_document_processing_baseline.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/alembic/versions/0001_document_processing_baseline.py): baseline schema
- [alembic/versions/0004_add_job_events.py](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/alembic/versions/0004_add_job_events.py): live event history migration

## API Endpoints

### `POST /api/upload`

Queues one or more PDF files for extraction.

Form fields:

- `files`: one or more PDF files
- `channel`: optional source channel, defaults to `api`

### `GET /api/jobs/{job_id}`

Returns the latest durable and cached state for a job.

### `GET /api/jobs/{job_id}/events`

Returns recent durable job events for timeline rendering or polling clients.

### `GET /api/jobs/{job_id}/stream`

Streams live job events using Server-Sent Events.

Streaming characteristics:

- sends an initial `snapshot` event
- streams ordered job events as they are persisted
- supports reconnection via `Last-Event-ID`
- emits keep-alive heartbeats for long-lived connections

## Event Model

Current event types include:

- `job.uploaded`
- `job.queued`
- `job.processing_started`
- `job.grouping_started`
- `job.extraction_started`
- `job.extraction_completed`
- `job.completed`
- `job.failed`

These events are:

- stored in PostgreSQL
- mirrored into Valkey
- published to Kafka event topics on a best-effort basis

## Requirements

- Python `3.12`
- PostgreSQL database
- Kafka cluster
- Valkey instance
- Vertex AI service account with Gemini access

## Environment Variables

### Required

- `SQLALCHEMY_DATABASE_URL`
- `KAFKA_BOOTSTRAP_SERVERS`
- `VALKEY_URL`
- `VERTEX_AI_SERVICE_ACCOUNT_FILE`

### Optional Kafka SSL

- `KAFKA_SSL_CA_CERT_PATH`
- `KAFKA_SSL_ACCESS_CERT_PATH`
- `KAFKA_SSL_ACCESS_KEY_PATH`

### Optional Runtime Settings

- `VERTEX_AI_MODEL`
- `VERTEX_AI_LOCATION`
- `KAFKA_UPLOAD_TOPIC`
- `KAFKA_UPLOAD_CONSUMER_GROUP`
- `KAFKA_JOBS_TOPIC`
- `KAFKA_PROGRESS_TOPIC`
- `KAFKA_RESULTS_TOPIC`
- `KAFKA_ERRORS_TOPIC`
- `UPLOAD_MAX_BYTES`
- `ENVIRONMENT`
- `LOG_LEVEL`
- `DEBUG`

## Local Development

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -e .
```

If you use `uv`:

```bash
uv sync
```

### 3. Configure environment variables

Start by copying [`.env.example`](C:/Users/Rohith/aiven-services/aiven-pdf-extraction/.env.example) to `.env`, then replace the placeholder values with your Aiven and Vertex settings.

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

macOS or Linux:

```bash
cp .env.example .env
```

Example:

```env
VERTEX_AI_SERVICE_ACCOUNT_FILE=secrets/your-service-account.json
VERTEX_AI_MODEL=gemini-2.5-flash-lite
VERTEX_AI_LOCATION=us-central1

SQLALCHEMY_DATABASE_URL=postgresql://<user>:<password>@<host>:<port>/<database>?sslmode=require

KAFKA_BOOTSTRAP_SERVERS=<host>:<port>
KAFKA_SSL_CA_CERT_PATH=secrets/aiven/ca.pem
KAFKA_SSL_ACCESS_CERT_PATH=secrets/aiven/service.cert
KAFKA_SSL_ACCESS_KEY_PATH=secrets/aiven/service.key

VALKEY_URL=rediss://default:<password>@<host>:<port>

KAFKA_UPLOAD_TOPIC=document-extraction.uploads
KAFKA_UPLOAD_CONSUMER_GROUP=document-extraction-workers
KAFKA_JOBS_TOPIC=document.jobs
KAFKA_PROGRESS_TOPIC=document.progress
KAFKA_RESULTS_TOPIC=document.results
KAFKA_ERRORS_TOPIC=document.errors
UPLOAD_MAX_BYTES=5242880
LOG_LEVEL=INFO
DEBUG=false
```

### 4. Apply database migrations

```bash
alembic upgrade head
```

### 5. Create Kafka topics

Create these topics in Aiven before running the full live pipeline:

- `document-extraction.uploads`
- `document.jobs`
- `document.progress`
- `document.results`
- `document.errors`

You can change the names through environment variables if needed.

### 6. Start the API

```bash
python app.py
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

## Example Usage

### Upload a document

```bash
curl -X POST "http://127.0.0.1:8000/api/upload" \
  -F "files=@sample.pdf" \
  -F "channel=api"
```

### Fetch job status

```bash
curl "http://127.0.0.1:8000/api/jobs/<job_id>"
```

### Fetch job events

```bash
curl "http://127.0.0.1:8000/api/jobs/<job_id>/events"
```

### Consume the live SSE stream

Browser example:

```js
const source = new EventSource(`/api/jobs/${jobId}/stream`);

source.addEventListener("snapshot", (event) => {
  console.log("snapshot", JSON.parse(event.data));
});

source.addEventListener("job.completed", (event) => {
  console.log("completed", JSON.parse(event.data));
  source.close();
});
```

## Frontend Demo

A companion frontend demo is available here:

- `https://github.com/rohitchebolu/aiven-doc-processing-ui`

The frontend visualizes:

- upload workflow for logistics PDFs
- live pipeline timeline with stage-by-stage progress
- durable PostgreSQL-backed event history
- Aiven service cards showing Kafka, PostgreSQL, and Valkey roles
- current job state and system-performance summary
- SSE-driven live updates from the backend

That UI is designed for contest demos and connects directly to the SSE endpoint in this backend.

### Current UI Experience

The current frontend is designed as a realtime operations dashboard rather than a generic file uploader.

It includes:

- a top-level Aiven contest demo header and active job indicator
- service cards for `Apache Kafka`, `PostgreSQL`, and `Valkey`
- an upload panel to start extraction runs
- a live pipeline panel that moves through stages such as `Uploaded`, `Queued`, `Processing`, `Grouping`, `Extracting`, and `Completed`
- an event history panel showing durable backend events in execution order
- supporting state panels for job status, performance, and stack overview

This layout helps viewers immediately understand the architecture:

- `Kafka` is responsible for moving work through the pipeline
- `PostgreSQL` keeps the job history and durable event trail
- `Valkey` powers the live feel of the dashboard

That is an important part of the project story: the UI is intentionally built to make the value of the Aiven services visible, not hidden behind infrastructure.

## Operational Notes

- If the upload topic does not exist, uploads will fail with a clear Kafka topic error.
- If the worker event topics do not exist, the app still persists events to PostgreSQL and mirrors them to Valkey.
- The queue consumer starts at app startup and times out gracefully if the upload topic is missing.
- Large files are rejected using `UPLOAD_MAX_BYTES`.

## Contributing

Contributions are welcome. If you plan to open a pull request:

1. keep the extraction flow generic
2. preserve Aiven-first architecture choices
3. prefer migration-driven schema changes
4. include verification steps for backend behavior

## License

MIT
