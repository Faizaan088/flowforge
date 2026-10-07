"""PostgreSQL integration tests for execution retry and dead-letter handling."""

import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base  # noqa: E402
    from execution_claim import (  # noqa: E402
        ExecutionClaim,
        claim_execution,
        complete_execution,
        fail_execution,
        start_execution,
    )
    from execution_retry import (  # noqa: E402
        compute_exponential_backoff,
        fail_and_evaluate_retry,
        process_failed_executions,
        requeue_eligible_retries,
        requeue_retry_execution,
        transition_failed_execution,
    )
    from models import Execution  # noqa: E402


@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def clear_executions(session_factory):
    async with session_factory() as session:
        await session.execute(Execution.__table__.delete())
        await session.commit()


async def create_execution(
    session_factory,
    status="QUEUED",
    attempt=0,
    max_retries=3,
    worker_id=None,
    lease_until=None,
    available_at=None,
):
    async with session_factory() as session:
        execution = Execution(
            status=status,
            attempt=attempt,
            max_retries=max_retries,
            worker_id=worker_id,
            lease_until=lease_until,
            available_at=available_at,
        )
        session.add(execution)
        await session.commit()
        await session.refresh(execution)
        return execution.id


@pytest.mark.asyncio
async def test_success_lifecycle(session_factory):
    execution_id = await create_execution(session_factory, status="QUEUED")

    async with session_factory() as session:
        claim = await claim_execution(
            session, execution_id, "worker-1", timedelta(seconds=30)
        )
        assert claim is not None
        assert await start_execution(session, claim) is True
        assert await complete_execution(session, claim) is True

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "SUCCEEDED"
        assert row.finished_at is not None
        assert row.lease_until is None
        assert row.worker_id == "worker-1"


@pytest.mark.asyncio
async def test_failed_execution_transitions_to_retry_wait_when_retryable(session_factory):
    execution_id = await create_execution(
        session_factory, status="QUEUED", attempt=0, max_retries=3
    )

    async with session_factory() as session:
        claim = await claim_execution(
            session, execution_id, "worker-1", timedelta(seconds=30)
        )
        assert claim is not None
        assert await start_execution(session, claim) is True

        # Worker fails the execution
        failed = await fail_execution(session, claim, error_summary="database timeout")
        assert failed is True

    # Intermediate state is FAILED
    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "FAILED"
        assert row.error_summary == "database timeout"
        assert row.lease_until is None

    # Transition to RETRY_WAIT since attempt (0) < max_retries (3)
    async with session_factory() as session:
        result = await transition_failed_execution(
            session, execution_id, retry_delay=timedelta(seconds=15)
        )
        assert result == "RETRY_WAIT"

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "RETRY_WAIT"
        assert row.available_at is not None


@pytest.mark.asyncio
async def test_retry_wait_to_queued_when_eligible_preserves_attempt_semantics(
    session_factory,
):
    now = datetime.now(timezone.utc)
    execution_id = await create_execution(
        session_factory,
        status="RETRY_WAIT",
        attempt=0,
        max_retries=3,
        available_at=now - timedelta(seconds=1),
    )

    async with session_factory() as session:
        requeued = await requeue_retry_execution(session, execution_id)
        assert requeued is True

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "QUEUED"
        assert row.worker_id is None
        assert row.lease_until is None
        assert row.available_at is None
        # Attempt semantics must match lease recovery: increment by 1
        assert row.attempt == 1

    # Eligible queued execution can now be claimed by a new worker
    async with session_factory() as session:
        new_claim = await claim_execution(
            session, execution_id, "worker-2", timedelta(seconds=30)
        )
        assert new_claim is not None
        assert new_claim.worker_id == "worker-2"


@pytest.mark.asyncio
async def test_retry_wait_not_requeued_before_available_at(session_factory):
    now = datetime.now(timezone.utc)
    future_time = now + timedelta(minutes=5)

    execution_id = await create_execution(
        session_factory,
        status="RETRY_WAIT",
        attempt=0,
        available_at=future_time,
    )

    # Requeue attempt at current time should be rejected as not yet eligible
    async with session_factory() as session:
        assert await requeue_retry_execution(session, execution_id, now=now) is False

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "RETRY_WAIT"
        assert row.attempt == 0

    # At future_time, it becomes eligible
    async with session_factory() as session:
        assert (
            await requeue_retry_execution(
                session, execution_id, now=future_time + timedelta(seconds=1)
            )
            is True
        )

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "QUEUED"
        assert row.attempt == 1


@pytest.mark.asyncio
async def test_terminal_dead_lettered_when_retries_exhausted(session_factory):
    # Set attempt equal to max_retries: retries are exhausted
    execution_id = await create_execution(
        session_factory, status="QUEUED", attempt=2, max_retries=2
    )

    async with session_factory() as session:
        claim = await claim_execution(
            session, execution_id, "worker-1", timedelta(seconds=30)
        )
        assert claim is not None
        assert await start_execution(session, claim) is True
        assert await fail_execution(session, claim, error_summary="fatal error") is True

        result = await transition_failed_execution(session, execution_id)
        assert result == "DEAD_LETTERED"

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "DEAD_LETTERED"
        assert row.finished_at is not None
        assert row.available_at is None
        assert row.attempt == 2


@pytest.mark.asyncio
async def test_dead_lettered_is_terminal_and_cannot_be_claimed_or_requeued(
    session_factory,
):
    execution_id = await create_execution(
        session_factory, status="DEAD_LETTERED", attempt=3, max_retries=3
    )

    async with session_factory() as session:
        # Cannot be claimed
        claim = await claim_execution(
            session, execution_id, "worker-1", timedelta(seconds=30)
        )
        assert claim is None

        # Cannot be requeued
        assert await requeue_retry_execution(session, execution_id) is False

        # Cannot be transitioned as failed
        assert await transition_failed_execution(session, execution_id) is None


@pytest.mark.asyncio
async def test_invalid_and_duplicate_transitions_are_rejected(session_factory):
    execution_id = await create_execution(session_factory, status="QUEUED")

    async with session_factory() as session:
        claim = await claim_execution(
            session, execution_id, "worker-1", timedelta(seconds=30)
        )
        assert claim is not None
        assert await start_execution(session, claim) is True

    # Duplicate fail / complete: first succeeds, second rejected
    async with session_factory() as session:
        assert await fail_execution(session, claim) is True
        assert await fail_execution(session, claim) is False
        assert await complete_execution(session, claim) is False

    # Cannot transition to retry if not in FAILED status
    async with session_factory() as session:
        # First transition to RETRY_WAIT succeeds
        assert await transition_failed_execution(session, execution_id) == "RETRY_WAIT"
        # Second transition rejected because status is now RETRY_WAIT, not FAILED
        assert await transition_failed_execution(session, execution_id) is None

    # Cannot requeue from non-RETRY_WAIT status
    succeeded_id = await create_execution(session_factory, status="SUCCEEDED")
    async with session_factory() as session:
        assert await requeue_retry_execution(session, succeeded_id) is False

    # Stale claim with wrong lease cannot fail
    stale_claim = ExecutionClaim(
        execution_id=execution_id,
        worker_id="worker-wrong",
        lease_until=datetime.now(timezone.utc),
    )
    async with session_factory() as session:
        assert await fail_execution(session, stale_claim) is False


@pytest.mark.asyncio
async def test_fail_and_evaluate_retry_convenience_helper(session_factory):
    # Test retryable transition
    exec_1 = await create_execution(
        session_factory, status="QUEUED", attempt=0, max_retries=2
    )
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_1, "worker-1", timedelta(seconds=30))
        assert await start_execution(session, claim_1) is True
        result_1 = await fail_and_evaluate_retry(
            session, claim_1, error_summary="transient network issue"
        )
        assert result_1 == "RETRY_WAIT"

    # Test exhausted transition
    exec_2 = await create_execution(
        session_factory, status="QUEUED", attempt=2, max_retries=2
    )
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_2, "worker-1", timedelta(seconds=30))
        assert await start_execution(session, claim_2) is True
        result_2 = await fail_and_evaluate_retry(
            session, claim_2, error_summary="permanent schema error"
        )
        assert result_2 == "DEAD_LETTERED"


@pytest.mark.asyncio
async def test_batch_process_failed_executions(session_factory):
    # Two retryable, two exhausted
    id_retry_1 = await create_execution(
        session_factory, status="FAILED", attempt=0, max_retries=3
    )
    id_retry_2 = await create_execution(
        session_factory, status="FAILED", attempt=1, max_retries=3
    )
    id_dead_1 = await create_execution(
        session_factory, status="FAILED", attempt=3, max_retries=3
    )
    id_dead_2 = await create_execution(
        session_factory, status="FAILED", attempt=4, max_retries=3
    )

    async with session_factory() as session:
        summary = await process_failed_executions(session)

    assert set(summary["retry_wait"]) == {id_retry_1, id_retry_2}
    assert set(summary["dead_lettered"]) == {id_dead_1, id_dead_2}


@pytest.mark.asyncio
async def test_batch_requeue_eligible_retries(session_factory):
    now = datetime.now(timezone.utc)
    id_eligible_1 = await create_execution(
        session_factory,
        status="RETRY_WAIT",
        attempt=0,
        available_at=now - timedelta(seconds=5),
    )
    id_eligible_2 = await create_execution(
        session_factory,
        status="RETRY_WAIT",
        attempt=1,
        available_at=None,
    )
    id_delayed = await create_execution(
        session_factory,
        status="RETRY_WAIT",
        attempt=0,
        available_at=now + timedelta(minutes=10),
    )

    async with session_factory() as session:
        requeued_ids = await requeue_eligible_retries(session, now=now)

    assert set(requeued_ids) == {id_eligible_1, id_eligible_2}

    async with session_factory() as session:
        row_1 = await session.get(Execution, id_eligible_1)
        row_2 = await session.get(Execution, id_eligible_2)
        row_3 = await session.get(Execution, id_delayed)

        assert row_1.status == "QUEUED"
        assert row_1.attempt == 1

        assert row_2.status == "QUEUED"
        assert row_2.attempt == 2

        assert row_3.status == "RETRY_WAIT"
        assert row_3.attempt == 0


def test_compute_exponential_backoff_calculation():
    base_delay = timedelta(seconds=5)

    # 3 distinct exponential retry delays
    delay_0 = compute_exponential_backoff(0, base_delay=base_delay)
    delay_1 = compute_exponential_backoff(1, base_delay=base_delay)
    delay_2 = compute_exponential_backoff(2, base_delay=base_delay)
    delay_3 = compute_exponential_backoff(3, base_delay=base_delay)

    assert delay_0 == timedelta(seconds=5)   # 5 * (2 ** 0) = 5
    assert delay_1 == timedelta(seconds=10)  # 5 * (2 ** 1) = 10
    assert delay_2 == timedelta(seconds=20)  # 5 * (2 ** 2) = 20
    assert delay_3 == timedelta(seconds=40)  # 5 * (2 ** 3) = 40

    # Max delay cap
    capped = compute_exponential_backoff(
        3, base_delay=base_delay, max_delay=timedelta(seconds=25)
    )
    assert capped == timedelta(seconds=25)


@pytest.mark.asyncio
async def test_exponential_backoff_multi_attempt_progression_and_exhaustion(
    session_factory,
):
    """Prove 3 distinct exponential delays followed by terminal retry exhaustion in PostgreSQL."""
    base_delay = timedelta(seconds=10)
    max_retries = 3

    # Seed execution with max_retries = 3
    execution_id = await create_execution(
        session_factory, status="QUEUED", attempt=0, max_retries=max_retries
    )

    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # --- ATTEMPT 0: First run fails -> Delay 1: 10s * (2 ** 0) = 10s ---
    async with session_factory() as session:
        claim_0 = await claim_execution(session, execution_id, "worker-1", timedelta(seconds=30))
        assert claim_0 is not None
        assert await start_execution(session, claim_0) is True
        assert await fail_execution(session, claim_0, error_summary="failure 1") is True

        res_0 = await transition_failed_execution(
            session, execution_id, base_delay=base_delay, now=t0
        )
        assert res_0 == "RETRY_WAIT"

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "RETRY_WAIT"
        assert row.available_at == t0 + timedelta(seconds=10)
        assert row.attempt == 0

    # Requeue when eligible (t0 + 10s) -> attempt increments to 1
    t0_eligible = t0 + timedelta(seconds=10)
    async with session_factory() as session:
        assert await requeue_retry_execution(session, execution_id, now=t0_eligible) is True
        row = await session.get(Execution, execution_id)
        assert row.status == "QUEUED"
        assert row.attempt == 1

    # --- ATTEMPT 1: Second run fails -> Delay 2: 10s * (2 ** 1) = 20s ---
    t1 = t0_eligible + timedelta(seconds=5)
    async with session_factory() as session:
        claim_1 = await claim_execution(session, execution_id, "worker-2", timedelta(seconds=30))
        assert claim_1 is not None
        assert await start_execution(session, claim_1) is True
        assert await fail_execution(session, claim_1, error_summary="failure 2") is True

        res_1 = await transition_failed_execution(
            session, execution_id, base_delay=base_delay, now=t1
        )
        assert res_1 == "RETRY_WAIT"

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "RETRY_WAIT"
        assert row.available_at == t1 + timedelta(seconds=20)
        assert row.attempt == 1

    # Requeue when eligible (t1 + 20s) -> attempt increments to 2
    t1_eligible = t1 + timedelta(seconds=20)
    async with session_factory() as session:
        assert await requeue_retry_execution(session, execution_id, now=t1_eligible) is True
        row = await session.get(Execution, execution_id)
        assert row.status == "QUEUED"
        assert row.attempt == 2

    # --- ATTEMPT 2: Third run fails -> Delay 3: 10s * (2 ** 2) = 40s ---
    t2 = t1_eligible + timedelta(seconds=5)
    async with session_factory() as session:
        claim_2 = await claim_execution(session, execution_id, "worker-3", timedelta(seconds=30))
        assert claim_2 is not None
        assert await start_execution(session, claim_2) is True
        assert await fail_execution(session, claim_2, error_summary="failure 3") is True

        res_2 = await transition_failed_execution(
            session, execution_id, base_delay=base_delay, now=t2
        )
        assert res_2 == "RETRY_WAIT"

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "RETRY_WAIT"
        assert row.available_at == t2 + timedelta(seconds=40)
        assert row.attempt == 2

    # Requeue when eligible (t2 + 40s) -> attempt increments to 3
    t2_eligible = t2 + timedelta(seconds=40)
    async with session_factory() as session:
        assert await requeue_retry_execution(session, execution_id, now=t2_eligible) is True
        row = await session.get(Execution, execution_id)
        assert row.status == "QUEUED"
        assert row.attempt == 3

    # --- ATTEMPT 3: Fourth run fails -> Retries exhausted (attempt 3 >= max_retries 3) ---
    t3 = t2_eligible + timedelta(seconds=5)
    async with session_factory() as session:
        claim_3 = await claim_execution(session, execution_id, "worker-4", timedelta(seconds=30))
        assert claim_3 is not None
        assert await start_execution(session, claim_3) is True
        assert await fail_execution(session, claim_3, error_summary="failure 4 (terminal)") is True

        res_3 = await transition_failed_execution(
            session, execution_id, base_delay=base_delay, now=t3
        )
        assert res_3 == "DEAD_LETTERED"

    async with session_factory() as session:
        # Monotonic terminality: cannot be claimed or requeued
        assert await claim_execution(session, execution_id, "worker-5", timedelta(seconds=30)) is None
        assert await requeue_retry_execution(session, execution_id) is False

        row = await session.get(Execution, execution_id)
        assert row.status == "DEAD_LETTERED"
        assert row.available_at is None
        assert row.finished_at == t3
        assert row.attempt == 3


@pytest.mark.asyncio
async def test_batch_process_failed_executions_applies_individual_exponential_delays(
    session_factory,
):
    base_delay = timedelta(seconds=10)
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    id_0 = await create_execution(session_factory, status="FAILED", attempt=0, max_retries=3)
    id_1 = await create_execution(session_factory, status="FAILED", attempt=1, max_retries=3)
    id_2 = await create_execution(session_factory, status="FAILED", attempt=2, max_retries=3)
    id_exhausted = await create_execution(session_factory, status="FAILED", attempt=3, max_retries=3)

    async with session_factory() as session:
        summary = await process_failed_executions(session, base_delay=base_delay, now=now)

    assert set(summary["retry_wait"]) == {id_0, id_1, id_2}
    assert summary["dead_lettered"] == [id_exhausted]

    async with session_factory() as session:
        row_0 = await session.get(Execution, id_0)
        row_1 = await session.get(Execution, id_1)
        row_2 = await session.get(Execution, id_2)
        row_ex = await session.get(Execution, id_exhausted)

        # Delays: 10s * (2 ** 0) = 10s, 10s * (2 ** 1) = 20s, 10s * (2 ** 2) = 40s
        assert row_0.available_at == now + timedelta(seconds=10)
        assert row_1.available_at == now + timedelta(seconds=20)
        assert row_2.available_at == now + timedelta(seconds=40)
        assert row_ex.status == "DEAD_LETTERED"
        assert row_ex.available_at is None

