"""
ALSACNC Dashboard — FastAPI web UI for managing crawl experiments.

Provides:
- Experiment list and details
- Pipeline management (full auto / step-by-step)
- Real-time progress monitoring
- Results visualization (violations, dark patterns)
"""

import json
import logging
import os
import threading
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

logger = logging.getLogger("dashboard")

BASE_DIR = Path(__file__).resolve().parent

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        db_url = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        )
        _engine = create_engine(db_url, poolclass=NullPool)
    return _engine


def ensure_tables():
    engine = get_engine()
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


_summary_thread: Optional[threading.Thread] = None
_summary_shutdown = threading.Event()


def _start_summary_worker():
    global _summary_thread
    if not os.environ.get("DB_HOST"):
        logger.info("DB_HOST not set, summary worker not started")
        return
    try:
        from dashboard.summary_worker import run_worker

        _summary_thread = threading.Thread(
            target=run_worker,
            args=(_summary_shutdown,),
            daemon=True,
            name="summary-worker",
        )
        _summary_thread.start()
    except Exception as e:
        logger.warning(f"Could not start summary worker thread: {e}")


def _stop_summary_worker():
    _summary_shutdown.set()
    if _summary_thread and _summary_thread.is_alive():
        logger.info("Waiting for summary worker thread to stop...")
        _summary_thread.join(timeout=10)


@asynccontextmanager
async def lifespan(a):
    ensure_tables()
    _start_summary_worker()
    yield
    _stop_summary_worker()


app = FastAPI(title="ALSACNC Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


# ---------------------------------------------------------------------------
# Helper queries
# ---------------------------------------------------------------------------

def query_all(sql, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params or {}).fetchall()
    return rows


def query_one(sql, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).fetchone()
    return row


def execute(sql, params=None):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    experiments = query_all(
        """
        SELECT e.id, e.timestamp, e.country,
               COUNT(w.id) AS num_websites,
               SUM(CASE WHEN w.success = 1 THEN 1 ELSE 0 END) AS num_success,
               e.config::text AS config
        FROM experiments e
        LEFT JOIN websites w ON w.experiment_id = e.id
        GROUP BY e.id, e.timestamp, e.country, e.config::text
        ORDER BY e.timestamp DESC
        """
    )

    active_jobs = query_all(
        "SELECT * FROM pipeline_jobs WHERE status IN ('pending', 'running') ORDER BY created_at"
    )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"experiments": experiments, "active_jobs": active_jobs},
    )


@app.get("/experiment/{experiment_id}", response_class=HTMLResponse)
async def experiment_detail(request: Request, experiment_id: str):
    experiment = query_one(
        "SELECT * FROM experiments WHERE id = :id", {"id": experiment_id}
    )
    if not experiment:
        return HTMLResponse("<h1>Experiment not found</h1>", status_code=404)

    websites = query_all(
        """
        SELECT w.id, w.name, w.url, w.success, w.language, w.crux_rank,
               cr.cookie_notice_detected, cr.tracking_detected,
               cr.forced_action_detected, cr.interface_interference_detected,
               cr.accept_button_detected, cr.reject_button_detected
        FROM websites w
        LEFT JOIN crawl_results cr ON cr.website_id = w.id
        WHERE w.experiment_id = :eid
        ORDER BY w.id
        """,
        {"eid": experiment_id},
    )

    errors = query_all(
        """
        SELECT e.text, e.timestamp, w.name AS website_name
        FROM errors e
        JOIN websites w ON w.id = e.website_id
        WHERE w.experiment_id = :eid
        ORDER BY e.timestamp DESC
        """,
        {"eid": experiment_id},
    )

    jobs = query_all(
        """
        SELECT * FROM pipeline_jobs
        WHERE experiment_id = :eid
        ORDER BY created_at
        """,
        {"eid": experiment_id},
    )

    error_names = {e.website_name for e in errors}
    stats = _compute_stats(websites, error_names)
    is_full_pipeline = any(getattr(j, "depends_on_id", None) is not None for j in jobs)

    return templates.TemplateResponse(
        request=request,
        name="experiment.html",
        context={
            "experiment": experiment,
            "websites": websites,
            "errors": errors,
            "error_names": error_names,
            "jobs": jobs,
            "stats": stats,
            "is_full_pipeline": is_full_pipeline,
        },
    )


def _compute_stats(websites, error_names=None):
    error_names = error_names or set()
    total = len(websites)
    if total == 0:
        return {}
    crawled = sum(1 for w in websites if w.success == 1)
    analyzed = [w for w in websites if w.success == 1 and w.name not in error_names]
    analyzed_count = len(analyzed)
    banners = sum(1 for w in analyzed if w.cookie_notice_detected)
    tracking = sum(1 for w in analyzed if w.tracking_detected and w.tracking_detected >= 2)
    forced = sum(1 for w in analyzed if w.forced_action_detected)
    interference = sum(1 for w in analyzed if w.interface_interference_detected)

    return {
        "total": total,
        "crawled": crawled,
        "crawled_pct": round(100 * crawled / total, 1) if total else 0,
        "analyzed": analyzed_count,
        "analyzed_pct": round(100 * analyzed_count / total, 1) if total else 0,
        "banners": banners,
        "tracking": tracking,
        "forced_action": forced,
        "interface_interference": interference,
    }


@app.get("/new", response_class=HTMLResponse)
async def new_experiment_form(request: Request):
    return templates.TemplateResponse(
        request=request, name="new_experiment.html",
    )


VALID_JOB_TYPES = {"crawl", "predict_cookies", "predict_purposes", "summary"}
VALID_NUM_WEBSITES = {3, 10, 50, 100, 500, 1000, 10000, 50000}
STEP_DEPS = {"predict_cookies": "crawl", "predict_purposes": "predict_cookies", "summary": "predict_purposes"}


@app.post("/new")
async def create_experiment(
    request: Request,
    num_websites: int = Form(10),
    num_browsers: int = Form(1),
    mode: str = Form("full"),
):
    if num_websites not in VALID_NUM_WEBSITES:
        return HTMLResponse("Invalid number of websites", status_code=400)
    if num_browsers < 1 or num_browsers > 3:
        return HTMLResponse("Invalid number of browsers (max 3 on Railway)", status_code=400)
    if mode not in ("full", "step"):
        return HTMLResponse("Invalid mode", status_code=400)

    pipeline_id = str(uuid.uuid4())[:8]
    config = {
        "config_path": "config/experiment_config_railway.yaml",
        "num_browsers": num_browsers,
        "num_websites": num_websites,
    }

    if mode == "full":
        crawl_id = _insert_job("crawl", config, pipeline_id)
        pc_id = _insert_job("predict_cookies", {}, pipeline_id, depends_on=crawl_id)
        pp_id = _insert_job("predict_purposes", {}, pipeline_id, depends_on=pc_id)
        _insert_job("summary", {}, pipeline_id, depends_on=pp_id)
    else:
        _insert_job("crawl", config, pipeline_id)

    return RedirectResponse(url=f"/pipeline/{pipeline_id}", status_code=303)


def _insert_job(job_type, config, pipeline_id, depends_on=None, experiment_id=None):
    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
            INSERT INTO pipeline_jobs (job_type, status, config, pipeline_id, depends_on_id, experiment_id)
            VALUES (:type, 'pending', :config, :pid, :dep, :eid)
            RETURNING id
            """
            ),
            {
                "type": job_type,
                "config": json.dumps(config),
                "pid": pipeline_id,
                "dep": depends_on,
                "eid": experiment_id,
            },
        ).fetchone()
    return row[0]


@app.get("/pipeline/{pipeline_id}", response_class=HTMLResponse)
async def pipeline_status(request: Request, pipeline_id: str):
    jobs = query_all(
        "SELECT * FROM pipeline_jobs WHERE pipeline_id = :pid ORDER BY created_at",
        {"pid": pipeline_id},
    )
    if not jobs:
        return HTMLResponse("<h1>Pipeline not found</h1>", status_code=404)

    experiment_id = None
    for j in jobs:
        if j.experiment_id:
            experiment_id = j.experiment_id
            break

    return templates.TemplateResponse(
        request=request,
        name="pipeline.html",
        context={"jobs": jobs, "pipeline_id": pipeline_id, "experiment_id": experiment_id},
    )


@app.post("/pipeline/{pipeline_id}/pause")
async def pause_pipeline(pipeline_id: str):
    execute(
        """
        UPDATE pipeline_jobs SET status = 'paused'
        WHERE pipeline_id = :pid AND status = 'pending'
        """,
        {"pid": pipeline_id},
    )
    return RedirectResponse(url=f"/pipeline/{pipeline_id}", status_code=303)


@app.post("/pipeline/{pipeline_id}/resume")
async def resume_pipeline(pipeline_id: str):
    execute(
        """
        UPDATE pipeline_jobs SET status = 'pending'
        WHERE pipeline_id = :pid AND status = 'paused'
        """,
        {"pid": pipeline_id},
    )
    return RedirectResponse(url=f"/pipeline/{pipeline_id}", status_code=303)


@app.post("/pipeline/{pipeline_id}/cancel")
async def cancel_pipeline(pipeline_id: str):
    execute(
        """
        UPDATE pipeline_jobs SET status = 'cancelled'
        WHERE pipeline_id = :pid AND status IN ('pending', 'paused')
        """,
        {"pid": pipeline_id},
    )
    return RedirectResponse(url=f"/pipeline/{pipeline_id}", status_code=303)


@app.post("/step/run")
async def run_single_step(
    job_type: str = Form(...),
    experiment_id: Optional[str] = Form(None),
    num_websites: int = Form(10),
    num_browsers: int = Form(1),
):
    if job_type not in VALID_JOB_TYPES:
        return HTMLResponse("Invalid job type", status_code=400)
    if num_browsers < 1 or num_browsers > 3:
        return HTMLResponse("Invalid number of browsers (max 3 on Railway)", status_code=400)

    if experiment_id and job_type != "crawl":
        exists = query_one("SELECT id FROM experiments WHERE id = :id", {"id": experiment_id})
        if not exists:
            return HTMLResponse("Experiment not found", status_code=404)

    if experiment_id:
        is_full = query_one(
            "SELECT 1 FROM pipeline_jobs WHERE experiment_id = :eid AND depends_on_id IS NOT NULL LIMIT 1",
            {"eid": experiment_id},
        )
        if is_full:
            return HTMLResponse("Full pipeline mode: steps are managed automatically", status_code=409)

        already = query_one(
            "SELECT id FROM pipeline_jobs WHERE experiment_id = :eid AND job_type = :jtype AND status IN ('pending', 'running', 'completed', 'paused') LIMIT 1",
            {"eid": experiment_id, "jtype": job_type},
        )
        if already:
            return HTMLResponse(f"Step '{job_type}' has already been run for this experiment", status_code=409)

        dep = STEP_DEPS.get(job_type)
        if dep:
            dep_done = query_one(
                "SELECT 1 FROM pipeline_jobs WHERE experiment_id = :eid AND job_type = :jtype AND status = 'completed' LIMIT 1",
                {"eid": experiment_id, "jtype": dep},
            )
            if not dep_done:
                return HTMLResponse(f"Cannot run '{job_type}': prerequisite step '{dep}' must complete first", status_code=409)

    pipeline_id = str(uuid.uuid4())[:8]
    config = {}
    if job_type == "crawl":
        config["config_path"] = "config/experiment_config_railway.yaml"
        config["num_browsers"] = num_browsers
        config["num_websites"] = num_websites

    _insert_job(job_type, config, pipeline_id, experiment_id=experiment_id)

    return RedirectResponse(url=f"/pipeline/{pipeline_id}", status_code=303)


# ---------------------------------------------------------------------------
# API endpoints (JSON) for HTMX / polling
# ---------------------------------------------------------------------------

@app.get("/api/pipeline/{pipeline_id}/status")
async def api_pipeline_status(pipeline_id: str):
    jobs = query_all(
        "SELECT id, job_type, status, progress, error_message, experiment_id, started_at, completed_at FROM pipeline_jobs WHERE pipeline_id = :pid ORDER BY created_at",
        {"pid": pipeline_id},
    )
    return [
        {
            "id": j.id,
            "job_type": j.job_type,
            "status": j.status,
            "progress": j.progress,
            "error_message": j.error_message,
            "experiment_id": j.experiment_id,
            "started_at": str(j.started_at) if j.started_at else None,
            "completed_at": str(j.completed_at) if j.completed_at else None,
        }
        for j in jobs
    ]


@app.get("/api/experiments")
async def api_experiments():
    rows = query_all(
        """
        SELECT e.id, e.timestamp, COUNT(w.id) AS num_websites
        FROM experiments e
        LEFT JOIN websites w ON w.experiment_id = e.id
        GROUP BY e.id, e.timestamp
        ORDER BY e.timestamp DESC
        LIMIT 50
        """
    )
    return [{"id": r.id, "timestamp": str(r.timestamp), "num_websites": r.num_websites} for r in rows]


# ---------------------------------------------------------------------------
# Research Results
# ---------------------------------------------------------------------------

@app.get("/experiment/{experiment_id}/results", response_class=HTMLResponse)
async def experiment_results(request: Request, experiment_id: str):
    from dashboard.results import COOKIEBLOCK_THRESHOLD, generate_results

    res = generate_results(get_engine(), experiment_id)
    return templates.TemplateResponse(
        request=request,
        name="results.html",
        context={
            "experiment_id": experiment_id,
            "threshold": COOKIEBLOCK_THRESHOLD,
            **res,
        },
    )


@app.get("/experiment/{experiment_id}/results/csv")
async def experiment_results_csv(experiment_id: str):
    from dashboard.results import results_to_csv

    csv_str = results_to_csv(get_engine(), experiment_id)
    if not csv_str:
        return HTMLResponse("No data available for this experiment", status_code=404)
    return PlainTextResponse(
        content=csv_str,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=results_{experiment_id}.csv"},
    )


@app.get("/api/jobs/{job_id}/logs")
async def api_job_logs(job_id: int):
    row = query_one(
        "SELECT logs, status, progress FROM pipeline_jobs WHERE id = :id",
        {"id": job_id},
    )
    if not row:
        return {"logs": "", "status": "unknown"}
    return {
        "logs": row.logs or "",
        "status": row.status,
        "progress": row.progress,
    }


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "summary_worker": _summary_thread is not None and _summary_thread.is_alive(),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
