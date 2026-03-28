from sqlalchemy import Column, Integer, DateTime, ForeignKey, Index, Text
from datetime import datetime
from src.database import Base


class LLMUsageLog(Base):
    __tablename__ = "llm_usage_logs"

    id = Column(Integer, primary_key=True, index=True)
    provider = Column(Text, nullable=False)
    model = Column(Text, nullable=False)
    call_type = Column(Text, nullable=False)  # "classification" | "extraction" | "page_extraction"
    input_tokens = Column(Integer, nullable=False, default=0)
    output_tokens = Column(Integer, nullable=False, default=0)
    response_time_ms = Column(Integer, nullable=False, default=0)
    extraction_job_id = Column(Integer, ForeignKey("extraction_jobs.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_llm_usage_logs_job_id", "extraction_job_id"),
    )
