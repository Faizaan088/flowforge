from sqlalchemy import Boolean, CheckConstraint, Column, DateTime, ForeignKey, Integer, JSON, String, UniqueConstraint
from sqlalchemy.orm import relationship, synonym
from sqlalchemy.sql import expression, func
from database import Base

class JobDefinition(Base):
    __tablename__ = "job_definitions"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    
    payload = Column(JSON, nullable=True) 
    category = Column(String, nullable=True, index=True)
    priority = Column(Integer, default=0)
    max_retries = Column(Integer, default=3, nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Execution(Base):
    __tablename__ = "executions"
    
    id = Column(Integer, primary_key=True, index=True)
    job_definition_id = Column(Integer, ForeignKey("job_definitions.id"))
    schedule_occurrence_id = Column(Integer, ForeignKey("schedule_occurrences.id"), nullable=True, index=True)
    category = Column(String, nullable=True, index=True)
    priority = Column(Integer, default=0, nullable=False, index=True)
    
    # QUEUED, CLAIMED, RUNNING, SUCCEEDED, FAILED, RETRY_WAIT, DEAD_LETTERED, CANCELLED
    status = Column(String, default="QUEUED", index=True) 
    attempt = Column(Integer, default=0)
    max_retries = Column(Integer, default=3, nullable=False)
    
    worker_id = Column(String, nullable=True)
    lease_until = Column(DateTime(timezone=True), nullable=True)
    available_at = Column(DateTime(timezone=True), nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    error_summary = Column(String, nullable=True)

    occurrence_id = synonym("schedule_occurrence_id")
    schedule_occurrence = relationship("ScheduleOccurrence", back_populates="execution", lazy="selectin")
    workflow_task_execution = relationship(
        "WorkflowTaskExecution",
        back_populates="execution",
        uselist=False,
        lazy="selectin",
    )

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


class WorkflowDefinition(Base):
    __tablename__ = "workflow_definitions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    description = Column(String, nullable=True)
    version = Column(Integer, default=1, nullable=False)
    status = Column(String, default="ACTIVE", nullable=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    tasks = relationship(
        "WorkflowTask",
        back_populates="workflow",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    edges = relationship(
        "WorkflowEdge",
        back_populates="workflow",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    runs = relationship(
        "WorkflowRun",
        back_populates="workflow",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    nodes = synonym("tasks")


class WorkflowTask(Base):
    __tablename__ = "workflow_tasks"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(
        Integer,
        ForeignKey("workflow_definitions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name = Column(String, nullable=False, index=True)
    task_type = Column(String, default="STANDARD", nullable=False)
    config = Column(JSON, nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    workflow = relationship(
        "WorkflowDefinition",
        back_populates="tasks",
        lazy="selectin",
    )
    outgoing_edges = relationship(
        "WorkflowEdge",
        primaryjoin="WorkflowTask.id == WorkflowEdge.upstream_task_id",
        back_populates="upstream_task",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    incoming_edges = relationship(
        "WorkflowEdge",
        primaryjoin="WorkflowTask.id == WorkflowEdge.downstream_task_id",
        back_populates="downstream_task",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    executions = relationship(
        "WorkflowTaskExecution",
        back_populates="workflow_task",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    __table_args__ = (
        UniqueConstraint("workflow_id", "name", name="uq_workflow_tasks_workflow_name"),
    )


WorkflowNode = WorkflowTask


class WorkflowEdge(Base):
    __tablename__ = "workflow_edges"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(
        Integer,
        ForeignKey("workflow_definitions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    upstream_task_id = Column(
        Integer,
        ForeignKey("workflow_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    downstream_task_id = Column(
        Integer,
        ForeignKey("workflow_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    workflow = relationship(
        "WorkflowDefinition",
        back_populates="edges",
        lazy="selectin",
    )
    upstream_task = relationship(
        "WorkflowTask",
        foreign_keys=[upstream_task_id],
        back_populates="outgoing_edges",
        lazy="selectin",
    )
    downstream_task = relationship(
        "WorkflowTask",
        foreign_keys=[downstream_task_id],
        back_populates="incoming_edges",
        lazy="selectin",
    )

    upstream_node_id = synonym("upstream_task_id")
    downstream_node_id = synonym("downstream_task_id")
    upstream_node = synonym("upstream_task")
    downstream_node = synonym("downstream_task")

    __table_args__ = (
        CheckConstraint(
            "upstream_task_id != downstream_task_id",
            name="ck_workflow_edges_no_self_dependency",
        ),
        UniqueConstraint(
            "workflow_id",
            "upstream_task_id",
            "downstream_task_id",
            name="uq_workflow_edges_workflow_upstream_downstream",
        ),
    )

    def __init__(self, **kwargs):
        if "upstream_node_id" in kwargs and "upstream_task_id" not in kwargs:
            kwargs["upstream_task_id"] = kwargs.pop("upstream_node_id")
        if "downstream_node_id" in kwargs and "downstream_task_id" not in kwargs:
            kwargs["downstream_task_id"] = kwargs.pop("downstream_node_id")
        if "upstream_node" in kwargs and "upstream_task" not in kwargs:
            kwargs["upstream_task"] = kwargs.pop("upstream_node")
        if "downstream_node" in kwargs and "downstream_task" not in kwargs:
            kwargs["downstream_task"] = kwargs.pop("downstream_node")
        super().__init__(**kwargs)


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(
        Integer,
        ForeignKey("workflow_definitions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status = Column(String, default="PENDING", nullable=False, index=True)
    triggered_by = Column(String, default="MANUAL", nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    error_summary = Column(String, nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    workflow = relationship(
        "WorkflowDefinition",
        back_populates="runs",
        lazy="selectin",
    )
    task_executions = relationship(
        "WorkflowTaskExecution",
        back_populates="workflow_run",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    definition_id = synonym("workflow_id")
    workflow_definition_id = synonym("workflow_id")
    workflow_definition = synonym("workflow")
    tasks = synonym("task_executions")
    task_runs = synonym("task_executions")

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="ck_workflow_runs_status",
        ),
    )

    def __init__(self, **kwargs):
        if "workflow_definition_id" in kwargs and "workflow_id" not in kwargs:
            kwargs["workflow_id"] = kwargs.pop("workflow_definition_id")
        if "definition_id" in kwargs and "workflow_id" not in kwargs:
            kwargs["workflow_id"] = kwargs.pop("definition_id")
        if "workflow_definition" in kwargs and "workflow" not in kwargs:
            kwargs["workflow"] = kwargs.pop("workflow_definition")
        if "status" not in kwargs:
            kwargs["status"] = "PENDING"
        super().__init__(**kwargs)


class WorkflowTaskExecution(Base):
    __tablename__ = "workflow_task_executions"

    id = Column(Integer, primary_key=True, index=True)
    workflow_run_id = Column(
        Integer,
        ForeignKey("workflow_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    workflow_task_id = Column(
        Integer,
        ForeignKey("workflow_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    execution_id = Column(
        Integer,
        ForeignKey("executions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    status = Column(String, default="PENDING", nullable=False, index=True)
    attempt = Column(Integer, default=0, nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    error_summary = Column(String, nullable=True)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    workflow_run = relationship(
        "WorkflowRun",
        back_populates="task_executions",
        lazy="selectin",
    )
    workflow_task = relationship(
        "WorkflowTask",
        back_populates="executions",
        lazy="selectin",
    )
    execution = relationship(
        "Execution",
        back_populates="workflow_task_execution",
        lazy="selectin",
    )

    run_id = synonym("workflow_run_id")
    task_id = synonym("workflow_task_id")
    node_id = synonym("workflow_task_id")
    run = synonym("workflow_run")
    task = synonym("workflow_task")
    node = synonym("workflow_task")

    __table_args__ = (
        UniqueConstraint(
            "workflow_run_id",
            "workflow_task_id",
            name="uq_workflow_task_executions_run_task",
        ),
        UniqueConstraint(
            "execution_id",
            name="uq_workflow_task_executions_execution_id",
        ),
        CheckConstraint(
            "status IN ('PENDING', 'READY', 'RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED', 'CANCELLED')",
            name="ck_workflow_task_executions_status",
        ),
    )

    def __init__(self, **kwargs):
        if "run_id" in kwargs and "workflow_run_id" not in kwargs:
            kwargs["workflow_run_id"] = kwargs.pop("run_id")
        if "task_id" in kwargs and "workflow_task_id" not in kwargs:
            kwargs["workflow_task_id"] = kwargs.pop("task_id")
        if "node_id" in kwargs and "workflow_task_id" not in kwargs:
            kwargs["workflow_task_id"] = kwargs.pop("node_id")
        if "run" in kwargs and "workflow_run" not in kwargs:
            kwargs["workflow_run"] = kwargs.pop("run")
        if "task" in kwargs and "workflow_task" not in kwargs:
            kwargs["workflow_task"] = kwargs.pop("task")
        if "node" in kwargs and "workflow_task" not in kwargs:
            kwargs["workflow_task"] = kwargs.pop("node")
        if "status" not in kwargs:
            kwargs["status"] = "PENDING"
        super().__init__(**kwargs)


WorkflowTaskRun = WorkflowTaskExecution
WorkflowNodeExecution = WorkflowTaskExecution


class ConcurrencyLimitPolicy(Base):
    __tablename__ = "concurrency_limit_policies"

    id = Column(Integer, primary_key=True, index=True)
    target_type = Column(String, nullable=False, index=True)  # "WORKFLOW", "TASK_TYPE"
    target_id = Column(String, nullable=False, index=True)
    max_concurrency = Column(Integer, nullable=False, default=1)
    is_enabled = Column(
        Boolean, default=True, server_default=expression.true(), nullable=False
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "target_type IN ('WORKFLOW', 'TASK_TYPE')",
            name="ck_concurrency_policies_target_type",
        ),
        CheckConstraint(
            "max_concurrency >= 1",
            name="ck_concurrency_policies_max_concurrency",
        ),
        UniqueConstraint(
            "target_type",
            "target_id",
            name="uq_concurrency_policies_target",
        ),
    )


class RateLimitPolicy(Base):
    __tablename__ = "rate_limit_policies"

    id = Column(Integer, primary_key=True, index=True)
    target_type = Column(
        String, default="CATEGORY", nullable=False, index=True
    )  # "CATEGORY", "JOB_CATEGORY", "TASK_TYPE", "WORKFLOW"
    target_id = Column(String, nullable=False, index=True)
    max_requests = Column(Integer, nullable=False, default=10)
    window_seconds = Column(Integer, nullable=False, default=60)
    is_enabled = Column(
        Boolean, default=True, server_default=expression.true(), nullable=False
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    records = relationship(
        "RateLimitRecord",
        back_populates="policy",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    __table_args__ = (
        CheckConstraint(
            "target_type IN ('CATEGORY', 'JOB_CATEGORY', 'TASK_TYPE', 'WORKFLOW')",
            name="ck_rate_limit_policies_target_type",
        ),
        CheckConstraint(
            "max_requests >= 1",
            name="ck_rate_limit_policies_max_requests",
        ),
        CheckConstraint(
            "window_seconds >= 1",
            name="ck_rate_limit_policies_window_seconds",
        ),
        UniqueConstraint(
            "target_type",
            "target_id",
            name="uq_rate_limit_policies_target",
        ),
    )


class RateLimitRecord(Base):
    __tablename__ = "rate_limit_records"

    id = Column(Integer, primary_key=True, index=True)
    policy_id = Column(
        Integer,
        ForeignKey("rate_limit_policies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    execution_id = Column(
        Integer,
        ForeignKey("executions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    recorded_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    policy = relationship(
        "RateLimitPolicy", back_populates="records", lazy="selectin"
    )


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=True, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(50), nullable=False, default="observer")
    is_active = Column(
        Boolean, default=True, server_default=expression.true(), nullable=False
    )
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "role IN ('admin', 'operator', 'observer')",
            name="ck_users_role",
        ),
    )

