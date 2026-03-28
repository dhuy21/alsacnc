"""
Summary worker: polls pipeline_jobs for 'summary' tasks and runs them as subprocesses.
Runs as a background thread inside the dashboard service, avoiding queue congestion
with long-running prediction jobs in the classifiers service.
"""

import json
import logging
import os
import subprocess
import sys
import threading

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SummaryWorker] %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("summary_worker")

POLL_INTERVAL = 5
LEASE_TIMEOUT_SECONDS = 3600
WORKER_ID = f"summary-{os.getenv('HOSTNAME', os.getpid())}"


def _get_engine():
    db_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url, poolclass=NullPool)


def _claim_job(engine):
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
            UPDATE pipeline_jobs
            SET status = 'running',
                started_at = TIMEZONE('utc', CURRENT_TIMESTAMP),
                worker_id = :worker_id
            WHERE id = (
                SELECT pj.id FROM pipeline_jobs pj
                WHERE pj.status = 'pending'
                  AND pj.job_type = 'summary'
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
        return row


def _recover_stuck_jobs(engine):
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
            UPDATE pipeline_jobs
            SET status = 'failed',
                error_message = 'Worker lease expired (timeout)',
                completed_at = TIMEZONE('utc', CURRENT_TIMESTAMP)
            WHERE status = 'running'
              AND job_type = 'summary'
              AND started_at < TIMEZONE('utc', CURRENT_TIMESTAMP) - INTERVAL '{LEASE_TIMEOUT_SECONDS} seconds'
            RETURNING id, pipeline_id
            """
            ),
        ).fetchall()
    for job_id, pipeline_id in rows:
        logger.warning(f"Recovered stuck summary job {job_id}")
        _cascade_fail_dependents(engine, job_id, pipeline_id)


def _complete_job(engine, job_id, result_data=None):
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


def _fail_job(engine, job_id, error_msg, logs_text=None):
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


def _cascade_fail_dependents(engine, job_id, pipeline_id):
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


def _run_subprocess(cmd, job_id):
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
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
            logger.info(f"Job {job_id}: Summary completed successfully")
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


def run_worker(shutdown_event=None):
    """Main worker loop. If shutdown_event is provided (threading.Event), use it for stopping."""
    if shutdown_event is None:
        shutdown_event = threading.Event()

    logger.info(f"Summary worker started (id={WORKER_ID})")
    engine = _get_engine()
    logger.info("Connected to PostgreSQL")

    recovery_counter = 0
    while not shutdown_event.is_set():
        try:
            recovery_counter += 1
            if recovery_counter % 60 == 0:
                _recover_stuck_jobs(engine)

            job = _claim_job(engine)
            if job is None:
                shutdown_event.wait(POLL_INTERVAL)
                continue

            job_id, job_type, config, experiment_id, pipeline_id = job
            logger.info(f"Claimed summary job {job_id} (experiment={experiment_id})")

            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE pipeline_jobs SET progress = :progress WHERE id = :id"),
                    {"progress": json.dumps({"status": "running", "message": "Generating summary..."}), "id": job_id},
                )

            cmd = ["python", "-m", "cookie_crawler.crawl_summary"]
            if experiment_id:
                cmd.extend(["--experiment_id", experiment_id])

            success, logs_text = _run_subprocess(cmd, job_id)

            if success:
                _complete_job(engine, job_id, {"status": "completed"})
                logger.info(f"Job {job_id}: Pipeline {pipeline_id} - summary done")
            else:
                _fail_job(engine, job_id, "Summary failed", logs_text)
                _cascade_fail_dependents(engine, job_id, pipeline_id)

        except Exception as e:
            logger.error(f"Summary worker error: {e}", exc_info=True)
            shutdown_event.wait(POLL_INTERVAL)

    logger.info("Summary worker stopped")
