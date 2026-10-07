from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON
from sqlalchemy.sql import func
from database import Base

class JobDefinition(Base):
    __tablename__ = "job_definitions"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    
    payload = Column(JSON, nullable=True) 
    priority = Column(Integer, default=0)
    max_retries = Column(Integer, default=3, nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Execution(Base):
    __tablename__ = "executions"
    
    id = Column(Integer, primary_key=True, index=True)
    job_definition_id = Column(Integer, ForeignKey("job_definitions.id"))
    
    # QUEUED, CLAIMED, RUNNING, SUCCEEDED, FAILED, RETRY_WAIT, DEAD_LETTERED
    status = Column(String, default="QUEUED", index=True) 
    attempt = Column(Integer, default=0)
    max_retries = Column(Integer, default=3, nullable=False)
    
    worker_id = Column(String, nullable=True)
    lease_until = Column(DateTime(timezone=True), nullable=True)
    available_at = Column(DateTime(timezone=True), nullable=True)
    
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    error_summary = Column(String, nullable=True)


class Worker(Base):
    __tablename__ = "workers"

    worker_id = Column(String, primary_key=True)
    status = Column(String, nullable=False, default="ACTIVE")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_heartbeat_at = Column(DateTime(timezone=True), nullable=False)
