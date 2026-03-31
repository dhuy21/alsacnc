"""
Classifiers worker: polls pipeline_jobs for predict_cookies and predict_purposes
tasks, then executes them as subprocesses.
Runs as a background thread alongside the Flask IETC server.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ClassifiersWorker] %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("classifiers_worker")

POLL_INTERVAL = 5
LEASE_TIMEOUT_SECONDS = 7200
LOG_FLUSH_INTERVAL = 10
WORKER_ID = f"classifiers-{os.getenv('HOSTNAME', os.getpid())}"
JOB_TYPES = ("predict_cookies", "predict_purposes")


def get_engine():
    db_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(
        db_url,
        poolclass=NullPool,
        connect_args={
            "connect_timeout": 10,
            "options": "-c statement_timeout=120000",
        },
    )


def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS pipeline_jobs (
                id SERIAL PRIMARY KEY,
                job_type VARCHAR NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'pending',
                config JSON,
                experiment_id VARCHAR,
                pipeline_id VARCHAR,
                depends_on_id INTEGER,
                progress JSON,
                result JSON,
                logs TEXT,
                error_message TEXT,
                worker_id VARCHAR,
                created_at TIMESTAMP DEFAULT TIMEZONE('utc', CURRENT_TIMESTAMP),
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
            """
            )
        )
        conn.execute(
            text(
                """
            CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_pending
            ON pipeline_jobs (status, job_type) WHERE status = 'pending'
            """
            )
        )


def claim_job(engine):
    type_filter = ", ".join(f"'{t}'" for t in JOB_TYPES)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
            UPDATE pipeline_jobs
            SET status = 'running',
                started_at = TIMEZONE('utc', CURRENT_TIMESTAMP),
                worker_id = :worker_id
            WHERE id = (
                SELECT pj.id FROM pipeline_jobs pj
                WHERE pj.status = 'pending'
                  AND pj.job_type IN ({type_filter})
                  AND (
                    pj.depends_on_id IS NULL
                    OR EXISTS (
                        SELECT 1 FROM pipeline_jobs dep
                        WHERE dep.id = pj.depends_on_id AND dep.status = 'completed'
                    )
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM pipeline_jobs sibling
                    WHERE sibling.pipeline_id = pj.pipeline_id
                      AND sibling.job_type = 'crawl'
                      AND sibling.status IN ('pending', 'running')
                  )
                ORDER BY pj.created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, job_type, config, experiment_id, pipeline_id
            """
            ),
            {"worker_id": WORKER_ID},
        ).fetchone()
        return row


def recover_stuck_jobs(engine):
    """Mark jobs stuck in 'running' beyond lease timeout as failed, then cascade."""
    type_filter = ", ".join(f"'{t}'" for t in JOB_TYPES)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
            UPDATE pipeline_jobs
            SET status = 'failed',
                error_message = 'Worker lease expired (timeout)',
                completed_at = TIMEZONE('utc', CURRENT_TIMESTAMP)
            WHERE status = 'running'
              AND job_type IN ({type_filter})
              AND started_at < TIMEZONE('utc', CURRENT_TIMESTAMP) - INTERVAL '{LEASE_TIMEOUT_SECONDS} seconds'
            RETURNING id, pipeline_id
            """
            ),
        ).fetchall()
    for job_id, pipeline_id in rows:
        logger.warning(f"Recovered stuck classifier job {job_id}")
        cascade_fail_dependents(engine, job_id, pipeline_id)


def update_job_progress(engine, job_id, progress_data):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE pipeline_jobs SET progress = :progress WHERE id = :id"),
            {"progress": json.dumps(progress_data), "id": job_id},
        )


def complete_job(engine, job_id, result_data=None):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            UPDATE pipeline_jobs
            SET status = 'completed',
                completed_at = TIMEZONE('utc', CURRENT_TIMESTAMP),
                result = :result
            WHERE id = :id
            """
            ),
            {"result": json.dumps(result_data) if result_data is not None else None, "id": job_id},
        )


def fail_job(engine, job_id, error_msg, logs_text=None):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
            UPDATE pipeline_jobs
            SET status = 'failed',
                completed_at = TIMEZONE('utc', CURRENT_TIMESTAMP),
                error_message = :error,
                logs = :logs
            WHERE id = :id
            """
            ),
            {"error": error_msg, "logs": logs_text, "id": job_id},
        )


def cascade_fail_dependents(engine, job_id, pipeline_id):
    """When a job fails, cascade-fail all downstream jobs in the same pipeline."""
    if not pipeline_id:
        return
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
            UPDATE pipeline_jobs
            SET status = 'failed',
                error_message = 'Upstream job failed',
                completed_at = TIMEZONE('utc', CURRENT_TIMESTAMP)
            WHERE pipeline_id = :pid
              AND status IN ('pending', 'paused')
              AND id != :job_id
            """
            ),
            {"pid": pipeline_id, "job_id": job_id},
        )
        if result.rowcount > 0:
            logger.info(f"Cascade-failed {result.rowcount} downstream jobs in pipeline {pipeline_id}")


def execute_predict_cookies(job_id, config, experiment_id, engine):
    cmd = ["python", "-m", "classifiers.predict_cookies"]
    if experiment_id:
        cmd.extend(["--experiment_id", experiment_id])

    logger.info(f"Job {job_id}: Running predict_cookies")
    update_job_progress(engine, job_id, {"status": "running", "message": "Running cookie predictions..."})

    return _run_subprocess(cmd, job_id, engine, cwd="/opt/repo")


def execute_predict_purposes(job_id, config, experiment_id, engine):
    cmd = ["python", "-m", "classifiers.predict_purposes"]
    if experiment_id:
        cmd.extend(["--experiment_id", experiment_id])

    logger.info(f"Job {job_id}: Running predict_purposes")
    update_job_progress(engine, job_id, {"status": "running", "message": "Running purpose predictions..."})

    return _run_subprocess(cmd, job_id, engine, cwd="/opt/repo")


def _flush_logs(engine, job_id, log_lines):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE pipeline_jobs SET logs = :logs WHERE id = :id"),
                {"logs": "\n".join(log_lines[-100:]), "id": job_id},
            )
    except Exception:
        pass


def _run_subprocess(cmd, job_id, engine, cwd=None):
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=cwd,
            env={**os.environ},
        )

        log_lines = []
        last_flush = time.time()
        for line in proc.stdout:
            line = line.rstrip()
            log_lines.append(line)
            if len(log_lines) > 200:
                log_lines = log_lines[-200:]
            logger.info(f"Job {job_id}: {line}")

            if time.time() - last_flush >= LOG_FLUSH_INTERVAL:
                _flush_logs(engine, job_id, log_lines)
                last_flush = time.time()

        proc.wait()
        logs_text = "\n".join(log_lines[-100:])
        _flush_logs(engine, job_id, log_lines)

        if proc.returncode == 0:
            logger.info(f"Job {job_id}: Completed successfully")
            return True, logs_text
        else:
            logger.error(f"Job {job_id}: Failed with code {proc.returncode}")
            return False, logs_text

    except Exception as e:
        logger.error(f"Job {job_id}: Exception: {e}")
        return False, str(e)
    finally:
        if proc and proc.poll() is None:
            proc.kill()
            proc.wait()


EXECUTORS = {
    "predict_cookies": execute_predict_cookies,
    "predict_purposes": execute_predict_purposes,
}


def run_worker(shutdown_event=None):
    """Main worker loop. If shutdown_event is provided (threading.Event), use it for stopping."""
    if shutdown_event is None:
        shutdown_event = threading.Event()

        def _handle_signal(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            shutdown_event.set()

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

    logger.info(f"Classifiers worker started (id={WORKER_ID})")

    engine = get_engine()
    ensure_tables(engine)
    logger.info("Connected to PostgreSQL, pipeline_jobs table ready")

    def _sleep():
        shutdown_event.wait(POLL_INTERVAL)

    recovery_counter = 0
    while not shutdown_event.is_set():
        try:
            recovery_counter += 1
            if recovery_counter % 60 == 0:
                recover_stuck_jobs(engine)

            job = claim_job(engine)
            if job is None:
                _sleep()
                continue

            job_id, job_type, config, experiment_id, pipeline_id = job
            logger.info(f"Claimed job {job_id} (type={job_type}, experiment={experiment_id})")

            executor = EXECUTORS.get(job_type)
            if executor is None:
                fail_job(engine, job_id, f"Unknown job type: {job_type}")
                continue

            success, logs_text = executor(job_id, config, experiment_id, engine)

            if success:
                complete_job(engine, job_id, {"status": "completed"})
                logger.info(f"Job {job_id}: Pipeline {pipeline_id} - next step now eligible")
            else:
                fail_job(engine, job_id, f"{job_type} failed", logs_text)
                cascade_fail_dependents(engine, job_id, pipeline_id)

        except Exception as e:
            logger.error(f"Worker loop error: {e}", exc_info=True)
            _sleep()

    logger.info("Classifiers worker stopped")


if __name__ == "__main__":
    run_worker()
