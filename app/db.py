"""SQLite schema and connection helpers.

The database is the source of truth for consent state and batch state. Generated
CSV files and their digests live on the data volume next to it; the database
records each digest and size so a missing or altered file is never served.
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subjects (
    subject_id      TEXT PRIMARY KEY,
    active          INTEGER NOT NULL,              -- 1 = currently consented
    active_since    TEXT NOT NULL,                 -- "" while revoked
    last_changed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consent_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id  TEXT NOT NULL,
    active      INTEGER NOT NULL,
    changed_at  TEXT NOT NULL,
    epoch       INTEGER NOT NULL                   -- bumped on every revocation
);

CREATE TABLE IF NOT EXISTS codes (
    subject_id TEXT PRIMARY KEY,
    code       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id             TEXT PRIMARY KEY,
    created_at     TEXT NOT NULL,
    status         TEXT NOT NULL,  -- running | completed | failed | invalidated
    failure_reason TEXT,
    finished_at    TEXT,
    boot_id        TEXT NOT NULL,
    row_count      INTEGER,
    file_path      TEXT,
    file_sha256    TEXT,
    file_size      INTEGER,
    source_version INTEGER NOT NULL DEFAULT 1   -- source extract pinned at admission
);

-- Every LIMS extract version ever activated.  A row exists only after the
-- full file was written and fsynced inside the data volume and the row was
-- committed, so a crash mid-activation leaves no usable row behind.
CREATE TABLE IF NOT EXISTS source_versions (
    version        INTEGER PRIMARY KEY,          -- monotonic, 1 = seeded config CSV
    file_name      TEXT NOT NULL,                -- original upload file name
    file_path      TEXT NOT NULL,                -- absolute path inside the data volume
    sha256         TEXT NOT NULL,
    byte_size      INTEGER NOT NULL,
    row_count      INTEGER NOT NULL,
    subject_count  INTEGER NOT NULL,
    base_version   INTEGER,                      -- version the upload was based on (NULL for v1)
    activated_at   TEXT NOT NULL,
    boot_id        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batch_subjects (
    batch_id      TEXT NOT NULL,
    subject_id    TEXT NOT NULL,
    consent_epoch INTEGER NOT NULL,               -- revocation count when admitted
    PRIMARY KEY (batch_id, subject_id)
);

CREATE TABLE IF NOT EXISTS batch_events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    at       TEXT NOT NULL,
    kind     TEXT NOT NULL,
    detail   TEXT
);

CREATE INDEX IF NOT EXISTS idx_batch_subjects_subject ON batch_subjects(subject_id);
CREATE INDEX IF NOT EXISTS idx_batch_events_batch ON batch_events(batch_id);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
        except BaseException:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            # Migration for data volumes created before source versioning:
            # old batch rows get column added; they are backfilled to v1 by
            # the service once the config CSV is registered as the initial version.
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(batches)").fetchall()}
            if "source_version" not in cols:
                conn.execute(
                    "ALTER TABLE batches ADD COLUMN source_version INTEGER NOT NULL DEFAULT 1"
                )
