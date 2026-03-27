"""
Classifiers worker: polls pipeline_jobs for predict_cookies, predict_purposes,
and summary tasks, then executes them as subprocesses.
Runs as a background thread alongside the Flask IETC server.
"""

import json
import logging
import os
import signal
import subprocess
import sys
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
WORKER_ID = f"classifiers-{os.getpid()}"
JOB_TYPES = ("predict_cookies", "predict_purposes", "summary")


def get_engine():
    db_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url, poolclass=NullPool)


def ensure_tables(engine):
    with engine.connect() as conn:
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
        conn.commit()


def claim_job(engine):
    type_filter = ", ".join(f"'{t}'" for t in JOB_TYPES)
    with engine.connect() as conn:
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
                ORDER BY pj.created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, job_type, config, experiment_id, pipeline_id
            """
            ),
            {"worker_id": WORKER_ID},
        ).fetchone()
        conn.commit()
        return row


def update_job_progress(engine, job_id, progress_data):
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE pipeline_jobs SET progress = :progress WHERE id = :id"),
            {"progress": json.dumps(progress_data), "id": job_id},
        )
        conn.commit()


def complete_job(engine, job_id, result_data=None):
    with engine.connect() as conn:
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
            {"result": json.dumps(result_data) if result_data else None, "id": job_id},
        )
        conn.commit()


def fail_job(engine, job_id, error_msg, logs_text=None):
    with engine.connect() as conn:
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
        conn.commit()


def chain_next_job(engine, job_id, pipeline_id):
    if not pipeline_id:
        return
    with engine.connect() as conn:
        next_job = conn.execute(
            text(
                """
            SELECT id, job_type FROM pipeline_jobs
            WHERE depends_on_id = :job_id AND pipeline_id = :pipeline_id
            LIMIT 1
            """
            ),
            {"job_id": job_id, "pipeline_id": pipeline_id},
        ).fetchone()
        conn.commit()
    if next_job:
        logger.info(f"Next pipeline job {next_job[0]} ({next_job[1]}) is now eligible")


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


def execute_summary(job_id, config, experiment_id, engine):
    cmd = ["python", "-m", "cookie_crawler.crawl_summary"]
    if experiment_id:
        cmd.extend(["--experiment_id", experiment_id])

    logger.info(f"Job {job_id}: Running crawl_summary")
    update_job_progress(engine, job_id, {"status": "running", "message": "Generating summary..."})

    return _run_subprocess(cmd, job_id, engine, cwd="/opt/repo")


def _run_subprocess(cmd, job_id, engine, cwd=None):
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
        for line in proc.stdout:
            line = line.rstrip()
            log_lines.append(line)
            if len(log_lines) > 200:
                log_lines = log_lines[-200:]
            logger.info(f"Job {job_id}: {line}")

        proc.wait()
        logs_text = "\n".join(log_lines[-100:])

        if proc.returncode == 0:
            logger.info(f"Job {job_id}: Completed successfully")
            return True, logs_text
        else:
            logger.error(f"Job {job_id}: Failed with code {proc.returncode}")
            return False, logs_text

    except Exception as e:
        logger.error(f"Job {job_id}: Exception: {e}")
        return False, str(e)


EXECUTORS = {
    "predict_cookies": execute_predict_cookies,
    "predict_purposes": execute_predict_purposes,
    "summary": execute_summary,
}


def run_worker(shutdown_event=None):
    """Main worker loop. If shutdown_event is provided (threading.Event), use it for stopping."""
    logger.info(f"Classifiers worker started (id={WORKER_ID})")

    engine = get_engine()
    ensure_tables(engine)
    logger.info("Connected to PostgreSQL, pipeline_jobs table ready")

    def should_stop():
        if shutdown_event:
            return shutdown_event.is_set()
        return False

    while not should_stop():
        try:
            job = claim_job(engine)
            if job is None:
                time.sleep(POLL_INTERVAL)
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
                chain_next_job(engine, job_id, pipeline_id)
            else:
                fail_job(engine, job_id, f"{job_type} failed", logs_text)

        except Exception as e:
            logger.error(f"Worker loop error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL)

    logger.info("Classifiers worker stopped")


if __name__ == "__main__":
    run_worker()
