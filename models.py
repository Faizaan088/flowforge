from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import relationship, synonym
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
    schedule_occurrence_id = Column(Integer, ForeignKey("schedule_occurrences.id"), nullable=True, index=True)
    
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

    occurrence_id = synonym("schedule_occurrence_id")
    schedule_occurrence = relationship("ScheduleOccurrence", back_populates="execution", lazy="selectin")

    __table_args__ = (
        UniqueConstraint(
            "schedule_occurrence_id",
            name="uq_executions_schedule_occurrence_id",
        ),
    )



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

    occurrences = relationship(
        "ScheduleOccurrence",
        back_populates="schedule_definition",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

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


class ScheduleOccurrence(Base):
    __tablename__ = "schedule_occurrences"

    id = Column(Integer, primary_key=True, index=True)
    schedule_definition_id = Column(
        Integer, ForeignKey("schedule_definitions.id"), nullable=False, index=True
    )
    scheduled_for = Column(DateTime(timezone=True), nullable=False, index=True)
    status = Column(String, default="SCHEDULED", nullable=False, index=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    schedule_definition = relationship(
        "ScheduleDefinition",
        back_populates="occurrences",
        lazy="selectin",
    )

    execution = relationship(
        "Execution",
        back_populates="schedule_occurrence",
        uselist=False,
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    definition_id = synonym("schedule_definition_id")
    definition = synonym("schedule_definition")

    __table_args__ = (
        UniqueConstraint(
            "schedule_definition_id",
            "scheduled_for",
            name="uq_schedule_occurrences_def_scheduled_for",
        ),
    )

    def __init__(self, **kwargs):
        if "definition_id" in kwargs and "schedule_definition_id" not in kwargs:
            kwargs["schedule_definition_id"] = kwargs.pop("definition_id")
        if "definition" in kwargs and "schedule_definition" not in kwargs:
            kwargs["schedule_definition"] = kwargs.pop("definition")
        if "status" not in kwargs:
            kwargs["status"] = "SCHEDULED"
        super().__init__(**kwargs)

    @property
    def executions(self):
        return [self.execution] if self.execution is not None else []




