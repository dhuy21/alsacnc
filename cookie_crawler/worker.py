"""
Crawler worker: polls pipeline_jobs for crawl tasks and executes them.
Replaces 'sleep infinity' CMD in Dockerfile.railway.
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
    format="%(asctime)s [CrawlerWorker] %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("crawler_worker")

POLL_INTERVAL = 5
LEASE_TIMEOUT_SECONDS = 7200  # 2 hours: mark stuck jobs as failed
LOG_FLUSH_INTERVAL = 10
WORKER_ID = f"crawler-{os.getenv('HOSTNAME', os.getpid())}"


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
    """Create pipeline_jobs table if it doesn't exist."""
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
            CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_type
            ON pipeline_jobs (status, job_type)
            WHERE status = 'pending'
            """
            )
        )


def claim_job(engine):
    """Claim next available crawl job using SKIP LOCKED."""
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
                  AND pj.job_type = 'crawl'
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


def update_job_progress(engine, job_id, progress_data):
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE pipeline_jobs SET progress = :progress WHERE id = :id"
            ),
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


def recover_stuck_jobs(engine):
    """Mark jobs stuck in 'running' beyond lease timeout as failed, then cascade."""
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
            UPDATE pipeline_jobs
            SET status = 'failed',
                error_message = 'Worker lease expired (timeout)',
                completed_at = TIMEZONE('utc', CURRENT_TIMESTAMP)
            WHERE status = 'running'
              AND job_type = 'crawl'
              AND started_at < TIMEZONE('utc', CURRENT_TIMESTAMP) - INTERVAL ':timeout seconds'
            RETURNING id, pipeline_id
            """
                .replace(":timeout", str(LEASE_TIMEOUT_SECONDS))
            ),
        ).fetchall()
    for job_id, pipeline_id in rows:
        logger.warning(f"Recovered stuck crawl job {job_id}")
        cascade_fail_dependents(engine, job_id, pipeline_id)


def _resolve_experiment_id_from_db(engine):
    """Get the most recently created experiment_id from the database."""
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id FROM experiments ORDER BY timestamp DESC LIMIT 1")
        ).fetchone()
    return row[0] if row else None


def _propagate_experiment_id(engine, pipeline_id, experiment_id):
    """Set experiment_id on all jobs in this pipeline so downstream steps know which experiment to target."""
    if not pipeline_id or not experiment_id:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE pipeline_jobs SET experiment_id = :eid WHERE pipeline_id = :pid AND experiment_id IS NULL"
            ),
            {"eid": experiment_id, "pid": pipeline_id},
        )
    logger.info(f"Propagated experiment_id={experiment_id} to pipeline {pipeline_id}")


def _flush_logs(engine, job_id, log_lines):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE pipeline_jobs SET logs = :logs WHERE id = :id"),
                {"logs": "\n".join(log_lines[-100:]), "id": job_id},
            )
    except Exception:
        pass


def execute_crawl(job_id, config, experiment_id, engine, pipeline_id=None):
    """Execute crawl as subprocess and monitor progress."""
    config = config or {}
    config_path = config.get("config_path", "config/experiment_config_railway.yaml")

    cmd = [
        "python",
        "-m",
        "cookie_crawler.run_crawler",
        "--config_path",
        config_path,
    ]

    if config.get("num_browsers"):
        cmd.extend(["--num_browsers", str(config["num_browsers"])])
    if config.get("num_websites"):
        cmd.extend(["--num_domains", str(config["num_websites"])])
    if config.get("domains_path"):
        cmd.extend(["--domains_path", config["domains_path"]])
    if experiment_id:
        cmd.extend(["--experiment_id", str(experiment_id)])

    logger.info(f"Job {job_id}: Starting crawl with cmd: {' '.join(cmd)}")
    update_job_progress(engine, job_id, {"status": "starting", "message": "Launching crawler..."})

    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd="/opt/crawler",
            env={**os.environ},
            start_new_session=True,
        )

        log_lines = []
        last_flush = time.time()
        for line in proc.stdout:
            line = line.rstrip()
            log_lines.append(line)
            if len(log_lines) > 200:
                log_lines = log_lines[-200:]
            logger.info(f"Job {job_id}: {line}")

            if "Crawling" in line and "websites" in line:
                update_job_progress(engine, job_id, {
                    "status": "crawling",
                    "message": line,
                })
            elif "Total running time:" in line:
                update_job_progress(engine, job_id, {
                    "status": "finishing",
                    "message": line,
                })

            if time.time() - last_flush >= LOG_FLUSH_INTERVAL:
                _flush_logs(engine, job_id, log_lines)
                last_flush = time.time()

        proc.wait()

        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except (ProcessLookupError, OSError):
            pass

        logs_text = "\n".join(log_lines[-100:])
        _flush_logs(engine, job_id, log_lines)

        if proc.returncode == 0:
            logger.info(f"Job {job_id}: Crawl completed successfully")
            try:
                resolved_eid = _resolve_experiment_id_from_db(engine)
                if resolved_eid:
                    _propagate_experiment_id(engine, pipeline_id, resolved_eid)
            except Exception as prop_err:
                logger.warning(f"Job {job_id}: experiment_id propagation failed (crawl still OK): {prop_err}")
            return True, logs_text
        else:
            logger.error(f"Job {job_id}: Crawl failed with code {proc.returncode}")
            return False, logs_text

    except Exception as e:
        logger.error(f"Job {job_id}: Crawl exception: {e}")
        return False, str(e)
    finally:
        if proc:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            if proc.poll() is None:
                proc.kill()
                proc.wait()


def run_worker():
    """Main worker loop."""
    logger.info(f"Crawler worker started (id={WORKER_ID})")

    engine = get_engine()
    ensure_tables(engine)
    logger.info("Connected to PostgreSQL, pipeline_jobs table ready")

    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    recovery_counter = 0
    while not shutdown:
        try:
            recovery_counter += 1
            if recovery_counter % 60 == 0:
                recover_stuck_jobs(engine)

            job = claim_job(engine)
            if job is None:
                time.sleep(POLL_INTERVAL)
                continue

            job_id, job_type, config, experiment_id, pipeline_id = job
            logger.info(f"Claimed job {job_id} (type={job_type}, experiment={experiment_id})")

            success, logs_text = execute_crawl(job_id, config, experiment_id, engine, pipeline_id)

            if success:
                complete_job(engine, job_id, {"status": "completed"})
                logger.info(f"Job {job_id}: Pipeline {pipeline_id} - next step now eligible")
            else:
                fail_job(engine, job_id, "Crawl process failed", logs_text)
                cascade_fail_dependents(engine, job_id, pipeline_id)

        except Exception as e:
            logger.error(f"Worker loop error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL)

    logger.info("Crawler worker stopped")


if __name__ == "__main__":
    run_worker()
