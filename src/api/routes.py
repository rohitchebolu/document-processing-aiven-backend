from fastapi import APIRouter

from . import endpoints

router = APIRouter(prefix="/api", tags=["document-extraction"])

router.add_api_route(
    "/upload",
    endpoints.process_pdf_endpoint,
    methods=["POST"],
)

router.add_api_route(
    "/jobs/{job_id}",
    endpoints.get_job_status_endpoint,
    methods=["GET"],
)

router.add_api_route(
    "/jobs/{job_id}/events",
    endpoints.get_job_events_endpoint,
    methods=["GET"],
)

router.add_api_route(
    "/jobs/{job_id}/stream",
    endpoints.stream_job_events_endpoint,
    methods=["GET"],
)
