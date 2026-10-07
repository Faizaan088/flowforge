from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import synonym
from sqlalchemy.sql import expression, func
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


class ScheduleDefinition(Base):
    __tablename__ = "schedule_definitions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    schedule_type = Column(String, nullable=False)
    enabled = Column(Boolean, default=True, server_default=expression.true(), nullable=False)
    configuration = Column(JSON, nullable=True)
    payload = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    is_enabled = synonym("enabled")

    def __init__(self, **kwargs):
        if "enabled" not in kwargs and "is_enabled" in kwargs:
            kwargs["enabled"] = kwargs.pop("is_enabled")
        elif "enabled" not in kwargs:
            kwargs["enabled"] = True

        config_val = kwargs.get("configuration")
        if config_val is None:
            config_val = kwargs.get("config")
        if config_val is None:
            config_val = kwargs.get("schedule_payload")

        payload_val = kwargs.get("payload")

        if config_val is not None and payload_val is None:
            kwargs["configuration"] = config_val
            kwargs["payload"] = config_val
        elif payload_val is not None and config_val is None:
            kwargs["configuration"] = payload_val
            kwargs["payload"] = payload_val

        kwargs.pop("config", None)
        kwargs.pop("schedule_payload", None)

        super().__init__(**kwargs)

    @property
    def config(self):
        return self.configuration

    @config.setter
    def config(self, val):
        self.configuration = val
        if self.payload is None:
            self.payload = val

