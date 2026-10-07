import os
import sys
import socket
import redis
import asyncio
from datetime import timedelta

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from execution_claim import claim_execution, complete_execution, start_execution

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("Worker can't find DATABASE_URL!")
    sys.exit(1)

engine = create_async_engine(DB_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
WORKER_ID = os.getenv("WORKER_ID", f"{socket.gethostname()}-{os.getpid()}")
LEASE_DURATION = timedelta(seconds=15)

async def claim_and_run(execution_id, worker_id=WORKER_ID):
    async with AsyncSessionLocal() as session:
        claim = await claim_execution(
            session,
            execution_id,
            worker_id,
            LEASE_DURATION,
        )

        if claim is None:
            print(f"Execution {execution_id} was not queued; skipping it.")
            return

        print(
            f"Claimed execution {execution_id} as {worker_id} "
            f"until {claim.lease_until.isoformat()}..."
        )

        if not await start_execution(session, claim):
            print(f"Execution {execution_id} is no longer owned by {worker_id}; skipping it.")
            return

        print(f">>> RUNNING JOB {execution_id}... simulating a 30-second heavy computation...")
        await asyncio.sleep(30) 
        
        print(f"<<< Finished execution {execution_id}!")
        if not await complete_execution(session, claim):
            print(f"Execution {execution_id} is no longer owned by {worker_id}; not completing it.")

def main():
    print("Worker process starting up...")
    
    r_url = os.getenv('REDIS_URL')
    if not r_url:
        print("Worker can't find REDIS_URL!")
        sys.exit(1)
        
    redis_conn = redis.from_url(r_url, decode_responses=True)
    print("Worker is ready! Listening to 'flowforge:queue'...")
    
    while True:
        job = redis_conn.brpop("flowforge:queue", timeout=5)
        
        if job:
            queue_name, execution_id = job
            print(f"\n--- INCOMING WORK ---")
            print(f"Got a job from Redis! Execution ID: {execution_id}")
            asyncio.run(claim_and_run(execution_id))

if __name__ == "__main__":
    main()
