from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Integer, String, Text

from src.database import Base


class ExtractionJob(Base):
    __tablename__ = "extraction_jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(64), unique=True, nullable=False, index=True)
    original_filename = Column(Text, nullable=False)
    content_type = Column(String(255), nullable=True)
    channel = Column(String(50), nullable=False, default="api")
    status = Column(String(32), nullable=False, default="queued")
    prompt_name = Column(String(128), nullable=True)
    prompt_template = Column(Text, nullable=True)
    extraction_result = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )
    processed_at = Column(DateTime, nullable=True)
