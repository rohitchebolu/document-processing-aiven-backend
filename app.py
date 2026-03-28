from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI
from dotenv import load_dotenv

from src.api import router
import src.models  # noqa: F401
from src.services.document_pipeline import document_pipeline
from src.services.kafka_service import get_kafka_service
from src.services.valkey_service import get_valkey_service

load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    kafka_service = await get_kafka_service()
    valkey_service = await get_valkey_service()
    await document_pipeline.start()
    try:
        yield
    finally:
        await document_pipeline.stop()
        await kafka_service.stop()
        await valkey_service.close()


app = FastAPI(
    title="Aiven Document Extraction",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=False)
