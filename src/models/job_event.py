from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Integer, String

from src.database import Base


class JobEvent(Base):
    __tablename__ = "job_events"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(128), nullable=False)
    stage = Column(String(64), nullable=False)
    progress = Column(Integer, nullable=False, default=0)
    payload = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
