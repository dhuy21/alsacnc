"""
PostgresStorageProvider: writes OpenWPM structured data directly to PostgreSQL.

This replaces SQLiteStorageProvider on platforms where a shared filesystem
is unavailable (e.g. Railway).  All table names are prefixed with ``openwpm_``
so they coexist with the project's own schema in the same database.

Enable by setting the environment variable ``OPENWPM_STORAGE=postgres``.
"""

import json
import logging
from asyncio import Task
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection as PgConnection

from openwpm.storage.storage_providers import (
    INCOMPLETE_VISITS,
    StructuredStorageProvider,
    TableName,
)
from openwpm.types import VisitId

TABLE_PREFIX = "openwpm_"

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS openwpm_task (
    task_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    manager_params TEXT NOT NULL,
    openwpm_version TEXT NOT NULL,
    browser_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS openwpm_crawl (
    browser_id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL,
    browser_params TEXT NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(task_id) REFERENCES openwpm_task(task_id)
);

CREATE TABLE IF NOT EXISTS openwpm_site_visits (
    visit_id BIGINT PRIMARY KEY,
    browser_id INTEGER NOT NULL,
    site_url VARCHAR(500) NOT NULL,
    site_rank INTEGER,
    FOREIGN KEY(browser_id) REFERENCES openwpm_crawl(browser_id)
);

CREATE TABLE IF NOT EXISTS openwpm_crawl_history (
    id SERIAL PRIMARY KEY,
    browser_id INTEGER,
    visit_id BIGINT,
    command TEXT,
    arguments TEXT,
    retry_number INTEGER,
    command_status TEXT,
    error TEXT,
    traceback TEXT,
    duration INTEGER,
    dtg TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS openwpm_http_requests (
    id SERIAL PRIMARY KEY,
    incognito INTEGER,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    extension_session_uuid TEXT,
    event_ordinal INTEGER,
    window_id INTEGER,
    tab_id INTEGER,
    frame_id INTEGER,
    url TEXT NOT NULL,
    top_level_url TEXT,
    parent_frame_id INTEGER,
    frame_ancestors TEXT,
    method TEXT NOT NULL,
    referrer TEXT NOT NULL,
    headers TEXT NOT NULL,
    request_id INTEGER NOT NULL,
    is_XHR INTEGER,
    is_third_party_channel INTEGER,
    is_third_party_to_top_window INTEGER,
    triggering_origin TEXT,
    loading_origin TEXT,
    loading_href TEXT,
    req_call_stack TEXT,
    resource_type TEXT NOT NULL,
    post_body TEXT,
    post_body_raw TEXT,
    time_stamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS openwpm_http_responses (
    id SERIAL PRIMARY KEY,
    incognito INTEGER,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    extension_session_uuid TEXT,
    event_ordinal INTEGER,
    window_id INTEGER,
    tab_id INTEGER,
    frame_id INTEGER,
    url TEXT NOT NULL,
    method TEXT NOT NULL,
    response_status INTEGER,
    response_status_text TEXT NOT NULL,
    is_cached INTEGER NOT NULL,
    headers TEXT NOT NULL,
    request_id INTEGER NOT NULL,
    location TEXT NOT NULL,
    time_stamp TIMESTAMP NOT NULL,
    content_hash TEXT
);

CREATE TABLE IF NOT EXISTS openwpm_http_redirects (
    id SERIAL PRIMARY KEY,
    incognito INTEGER,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    old_request_url TEXT,
    old_request_id TEXT,
    new_request_url TEXT,
    new_request_id TEXT,
    extension_session_uuid TEXT,
    event_ordinal INTEGER,
    window_id INTEGER,
    tab_id INTEGER,
    frame_id INTEGER,
    response_status INTEGER NOT NULL,
    response_status_text TEXT NOT NULL,
    headers TEXT NOT NULL,
    time_stamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS openwpm_javascript (
    id SERIAL PRIMARY KEY,
    incognito INTEGER,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    extension_session_uuid TEXT,
    event_ordinal INTEGER,
    page_scoped_event_ordinal INTEGER,
    window_id INTEGER,
    tab_id INTEGER,
    frame_id INTEGER,
    script_url TEXT,
    script_line TEXT,
    script_col TEXT,
    func_name TEXT,
    script_loc_eval TEXT,
    document_url TEXT,
    top_level_url TEXT,
    call_stack TEXT,
    symbol TEXT,
    operation TEXT,
    value TEXT,
    arguments TEXT,
    time_stamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS openwpm_javascript_cookies (
    id SERIAL PRIMARY KEY,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    extension_session_uuid TEXT,
    event_ordinal INTEGER,
    record_type TEXT,
    change_cause TEXT,
    expiry TEXT,
    is_http_only INTEGER,
    is_host_only INTEGER,
    is_session INTEGER,
    host TEXT,
    is_secure INTEGER,
    name TEXT,
    path TEXT,
    value TEXT,
    same_site TEXT,
    first_party_domain TEXT,
    store_id TEXT,
    time_stamp TEXT
);

CREATE TABLE IF NOT EXISTS openwpm_navigations (
    id SERIAL PRIMARY KEY,
    incognito INTEGER,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    extension_session_uuid TEXT,
    process_id INTEGER,
    window_id INTEGER,
    tab_id INTEGER,
    tab_opener_tab_id INTEGER,
    frame_id INTEGER,
    parent_frame_id INTEGER,
    window_width INTEGER,
    window_height INTEGER,
    window_type TEXT,
    tab_width INTEGER,
    tab_height INTEGER,
    tab_cookie_store_id TEXT,
    uuid TEXT,
    url TEXT,
    transition_qualifiers TEXT,
    transition_type TEXT,
    before_navigate_event_ordinal INTEGER,
    before_navigate_time_stamp TIMESTAMP,
    committed_event_ordinal INTEGER,
    committed_time_stamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS openwpm_callstacks (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    call_stack TEXT
);

CREATE TABLE IF NOT EXISTS openwpm_incomplete_visits (
    visit_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS openwpm_dns_responses (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL,
    browser_id INTEGER NOT NULL,
    visit_id BIGINT NOT NULL,
    hostname TEXT,
    addresses TEXT,
    used_address TEXT,
    canonical_name TEXT,
    is_TRR INTEGER,
    time_stamp TIMESTAMP NOT NULL
);
"""

logger = logging.getLogger("openwpm")


class PostgresStorageProvider(StructuredStorageProvider):
    """Stores OpenWPM structured data in PostgreSQL with ``openwpm_`` prefix."""

    def __init__(self, db_url: str) -> None:
        super().__init__()
        self.db_url = db_url
        self.conn: Optional[PgConnection] = None

    async def init(self) -> None:
        self.conn = psycopg2.connect(self.db_url)
        self.conn.autocommit = False
        with self.conn.cursor() as cur:
            cur.execute(POSTGRES_SCHEMA)
        self.conn.commit()
        logger.info("PostgresStorageProvider: tables created")

    async def store_record(
        self, table: TableName, visit_id: VisitId, record: Dict[str, Any]
    ) -> None:
        assert self.conn is not None
        pg_table = TABLE_PREFIX + table
        statement, args = self._generate_insert(pg_table, record)
        try:
            with self.conn.cursor() as cur:
                cur.execute(statement, args)
        except Exception as e:
            self.conn.rollback()
            logger.error(
                "PostgresStorageProvider unsupported record:\n%s\n%s\n%s",
                e, statement, repr(args),
            )

    @staticmethod
    def _generate_insert(
        table: str, data: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        columns = []
        placeholders = []
        values: List[Any] = []
        for field, value in data.items():
            columns.append(field)
            placeholders.append("%s")
            if isinstance(value, bytes):
                value = str(value, errors="ignore")
            elif callable(value):
                value = str(value)
            elif isinstance(value, dict):
                value = json.dumps(value)
            values.append(value)
        stmt = "INSERT INTO {} ({}) VALUES ({})".format(
            table, ", ".join(columns), ", ".join(placeholders)
        )
        return stmt, values

    async def finalize_visit_id(
        self, visit_id: VisitId, interrupted: bool = False
    ) -> Optional[Task[None]]:
        if interrupted:
            logger.warning("Visit with visit_id %d got interrupted", visit_id)
            assert self.conn is not None
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO openwpm_incomplete_visits (visit_id) VALUES (%s)",
                    (visit_id,),
                )
        await self.flush_cache()
        return None

    async def flush_cache(self) -> None:
        if self.conn is not None:
            self.conn.commit()

    async def shutdown(self) -> None:
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            self.conn = None
        logger.info("PostgresStorageProvider: shutdown complete")
