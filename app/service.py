"""Domain logic: LIMS input, source versioning, consent, asynchronous batches.

Correctness invariants:

* A batch admits a subject only if that subject was already consented at the
  instant the batch was created.  Admission and consent changes are serialized
  by one process-wide lock plus SQLite ``BEGIN IMMEDIATE`` transactions.
* A revocation permanently invalidates every batch that admitted the subject --
  still generating or already completed -- by flipping the batch to
  ``invalidated`` and bumping the subject's revocation epoch.  Admission records
  the epoch, so a stale batch can never be quietly completed or revived; later
  re-consent preserves the revoked state of old batches.
* Generation runs in a background thread.  Rows stream to a temp file and the
  file is published (atomic rename, digest + size recorded in the same
  transaction that marks the batch completed) only after the whole extract
  was processed and validity re-checked.  Any failure leaves the batch
  ``failed`` and no servable file.
* On start-up every batch left ``running`` by an earlier process is marked
  ``failed``; completed batches whose file is missing, resized or altered are
  also marked failed.  Intact completed batches remain downloadable.

Source versioning invariants:

* Every activated LIMS extract is an immutable, numbered ``source_versions``
  row whose bytes live on the data volume (``sources/v<N>.csv``) with size and
  SHA-256 recorded in the same committed row.  Activated versions are never
  overwritten or deleted.
* A replacement upload passes the version it is based on.  The whole file is
  validated first, then written to a temp name and fsynced; activation commits
  the new row only if the current version still equals the base version --
  inside one ``BEGIN IMMEDIATE`` transaction under the process lock -- so two
  concurrent replacements of the same base version can activate at most once.
  Any validation or crash leaves the current source untouched.
* Each batch pins the source version current at admission and generates from
  that exact, frozen file even if the current source changes mid-generation;
  data from two versions is never mixed.  Batch details carry the pinned
  version, and every historical version stays downloadable for traceability.
* Version replacement never changes consent, study codes or existing batch
  files.  On first start against an old data volume the configured input CSV is
  registered as version 1 and legacy batches are backfilled to it.  After a
  crash only fully committed versions exist, so an already-activated correction
  cannot be lost.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Optional

from .codes import CodeRegistry
from .config import Settings
from .db import Database

SUBJECT_COLUMN = "subject_id"
DATE_COLUMN = "test_date"
ITEM_COLUMN = "test_item"
VALUE_COLUMN = "value"

STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_INVALIDATED = "invalidated"

SEED_VERSION = 1
SOURCE_TMP_SUFFIX = ".upload.tmp"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class _BatchInvalidated(Exception):
    pass


class _WorkerStopped(Exception):
    """The process is shutting down; leave the batch for restart recovery."""


def _normalize_header(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


class LimsData:
    """One immutable LIMS extract version, parsed and validated as a whole.

    The same validation rules apply to the seeded configuration CSV (start-up
    failures are 500-level ``input_invalid``) and to uploaded replacements
    (client mistakes are 400-level ``source_invalid``); ``error_status`` picks
    which status an ``ApiError`` carries.
    """

    def __init__(self, raw: bytes, error_status: int = 500) -> None:
        self.raw = raw
        self.sha256 = hashlib.sha256(raw).hexdigest()
        self.byte_size = len(raw)

        def fail(code: str, message: str) -> None:
            raise ApiError(error_status, code, message)

        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            fail("source_invalid" if error_status == 400 else "input_invalid",
                 f"LIMS CSV is not UTF-8: {exc}")
        reader = csv.reader(io.StringIO(text), strict=True)
        try:
            header = next(reader)
        except StopIteration:
            fail("source_invalid" if error_status == 400 else "input_invalid",
                 "LIMS CSV is empty")
        self.header = header
        norm = [_normalize_header(h) for h in self.header]
        required = (SUBJECT_COLUMN, DATE_COLUMN, ITEM_COLUMN, VALUE_COLUMN)
        missing = [c for c in required if c not in norm]
        if missing:
            fail("source_invalid" if error_status == 400 else "input_invalid",
                 f"LIMS CSV header missing columns: {', '.join(missing)}")
        self.subject_idx = norm.index(SUBJECT_COLUMN)
        self.rows: list[list[str]] = []
        invalid_code = "source_invalid" if error_status == 400 else "input_invalid"
        for line_no, row in enumerate(reader, start=2):
            if not row or all(cell.strip() == "" for cell in row):
                continue  # blank separator lines are tolerated; order is otherwise kept
            if len(row) != len(self.header):
                fail(invalid_code,
                     f"LIMS CSV line {line_no} has {len(row)} fields, expected {len(self.header)}")
            if row[self.subject_idx].strip() == "":
                fail(invalid_code, f"LIMS CSV line {line_no} has no subject id")
            self.rows.append(row)
        if not self.rows:
            fail(invalid_code, "LIMS CSV contains a header but no data rows")
        self.subject_ids = {row[self.subject_idx].strip() for row in self.rows}
        self.row_count = len(self.rows)
        self.subject_count = len(self.subject_ids)
        # The header is emitted verbatim: the de-identified output differs from
        # the extract only in the identifier column's values.
        self.output_header = list(self.header)
        # Mirror the extract's own record terminator so line endings are kept.
        self.line_terminator = "\r\n" if "\r\n" in text else "\n"

    @classmethod
    def from_path(cls, path: Path, error_status: int = 500) -> "LimsData":
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise ApiError(
                error_status,
                "source_unreadable" if error_status == 400 else "input_unreadable",
                f"Cannot read LIMS CSV {path}: {exc}",
            )
        return cls(raw, error_status=error_status)


def load_initial_consent(path: Path) -> list[str]:
    """Read the initial roster: one subject id per line, or a one-column CSV with header."""
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ApiError(
            500, "consent_roster_unreadable", f"Cannot read initial consent roster: {exc}"
        )
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ApiError(500, "consent_roster_unreadable", f"Roster is not UTF-8: {exc}")
    rows = [
        r
        for r in csv.reader(io.StringIO(text))
        if r and not all(c.strip() == "" for c in r)
    ]
    if not rows:
        return []
    if len(rows[0]) == 1 and _normalize_header(rows[0][0]) in {"subject_id", "subject", "id"}:
        body = rows[1:]
    else:
        body = rows
    ids = []
    for row in body:
        if len(row) != 1:
            raise ApiError(
                500,
                "consent_roster_invalid",
                "Initial consent roster must contain one subject id per line",
            )
        if row[0].strip():
            ids.append(row[0].strip())
    return ids


class Service:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.settings.batch_dir.mkdir(parents=True, exist_ok=True)
        self.settings.source_dir.mkdir(parents=True, exist_ok=True)
        self.db = Database(settings.db_path)
        self.db.init()
        self.codes = CodeRegistry(self.db)
        self.boot_id = uuid.uuid4().hex
        self._lock = threading.Lock()  # serializes consent changes vs. batch admission vs. activation
        self._workers: dict[str, threading.Thread] = {}
        self._stopping = threading.Event()
        # Frozen extract per activated version; lazily filled for old versions
        # and replaced atomically when a replacement is activated.
        self._sources: dict[int, LimsData] = {}
        self._bootstrap_sources()
        self.lims = self._sources[self.current_version]
        self._bootstrap_consent()
        self._recover_after_restart()

    # ------------------------------------------------------------- source setup

    def _bootstrap_sources(self) -> None:
        """Register the configured CSV as v1 on old volumes; load the registry."""
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT MAX(version) AS v FROM source_versions"
            ).fetchone()["v"]
            if current is None:
                # First start, or an old data volume predating versioning:
                # the configuration CSV becomes immutable version 1.
                data = LimsData.from_path(self.settings.input_csv)
                final_path = self.settings.source_dir / f"v{SEED_VERSION}.csv"
                self._atomic_write_bytes(final_path, data.raw)
                now = utc_now()
                conn.execute(
                    "INSERT INTO source_versions(version, file_name, file_path, sha256, "
                    "byte_size, row_count, subject_count, base_version, activated_at, boot_id) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        SEED_VERSION,
                        self.settings.input_csv.name,
                        str(final_path),
                        data.sha256,
                        data.byte_size,
                        data.row_count,
                        data.subject_count,
                        None,
                        now,
                        self.boot_id,
                    ),
                )
                # Legacy batch rows (created by a pre-versioning build) pin v1.
                conn.execute(
                    "UPDATE batches SET source_version=? WHERE source_version IS NULL",
                    (SEED_VERSION,),
                )
            conn.execute("COMMIT")
        self._load_source_registry()
        self._cleanup_orphan_source_files()

    def _load_source_registry(self) -> None:
        """Load every activated version into memory, verifying the bytes on disk.

        A damaged non-current version stays in the registry (it is historical
        fact) but is not cached; an attempt to use it yields a clear conflict.
        A damaged *current* version is a fatal start-up error, because new
        batches could not be generated from a trustworthy source.
        """
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM source_versions ORDER BY version"
            ).fetchall()
        loaded: dict[int, LimsData] = {}
        current_version = max(r["version"] for r in rows)
        for row in rows:
            version = row["version"]
            try:
                raw = Path(row["file_path"]).read_bytes()
            except OSError as exc:
                if version == current_version:
                    raise ApiError(
                        500, "current_source_unreadable",
                        f"Current source version {version} is unreadable: {exc}",
                    )
                continue
            if (
                len(raw) != row["byte_size"]
                or hashlib.sha256(raw).hexdigest() != row["sha256"]
            ):
                if version == current_version:
                    raise ApiError(
                        500, "current_source_corrupt",
                        f"Current source version {version} failed size/digest verification",
                    )
                continue
            data = LimsData(raw)  # re-parse; registry rows were valid when activated
            loaded[version] = data
        self._sources = loaded
        self.current_version = current_version

    def _cleanup_orphan_source_files(self) -> None:
        """Remove temp uploads and files no registered version points at."""
        with self.db.connect() as conn:
            known = {
                Path(r["file_path"]).name
                for r in conn.execute("SELECT file_path FROM source_versions").fetchall()
            }
        for path in self.settings.source_dir.glob("*"):
            if not path.is_file():
                continue
            if path.name.endswith(SOURCE_TMP_SUFFIX) or path.name not in known:
                try:
                    path.unlink()
                except OSError:
                    pass

    @staticmethod
    def _atomic_write_bytes(final_path: Path, raw: bytes) -> None:
        """Write, fsync and atomically publish bytes inside the data volume."""
        tmp_path = final_path.with_name(f".{final_path.name}{SOURCE_TMP_SUFFIX}")
        with open(tmp_path, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, final_path)
        # Make the directory entry durable as well, so a crash cannot lose the
        # rename of an about-to-be-committed (or already committed) version.
        directory_fd = os.open(final_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _source_for_version(self, version: int) -> LimsData:
        """Return the frozen extract for a version, loading it lazily if needed."""
        cached = self._sources.get(version)
        if cached is not None:
            return cached
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM source_versions WHERE version=?", (version,)
            ).fetchone()
        if row is None:
            raise ApiError(
                409, "source_version_missing",
                f"Source version {version} is not registered",
            )
        try:
            raw = Path(row["file_path"]).read_bytes()
        except OSError as exc:
            raise ApiError(
                409, "source_version_unreadable",
                f"Source version {version} cannot be read: {exc}",
            )
        if len(raw) != row["byte_size"] or hashlib.sha256(raw).hexdigest() != row["sha256"]:
            raise ApiError(
                409, "source_version_corrupt",
                f"Source version {version} failed size/digest verification",
            )
        data = LimsData(raw)
        self._sources[version] = data
        return data

    # ----------------------------------------------------------- source queries

    def get_current_source(self) -> dict:
        return self.get_source(self.current_version)

    def list_sources(self) -> dict:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM source_versions ORDER BY version"
            ).fetchall()
        return {
            "current_version": self.current_version,
            "versions": [self._source_dict(r, r["version"] == self.current_version) for r in rows],
        }

    def get_source(self, version: int) -> dict:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT * FROM source_versions WHERE version=?", (version,)
            ).fetchone()
        if row is None:
            raise ApiError(404, "source_not_found", f"Unknown source version {version}")
        return self._source_dict(row, row["version"] == self.current_version)

    @staticmethod
    def _source_dict(row, is_current: bool) -> dict:
        return {
            "version": row["version"],
            "file_name": row["file_name"],
            "sha256": row["sha256"],
            "byte_size": row["byte_size"],
            "row_count": row["row_count"],
            "subject_count": row["subject_count"],
            "base_version": row["base_version"],
            "activated_at": row["activated_at"],
            "is_current": bool(is_current),
        }

    def open_source_download(self, version: int) -> tuple[str, int, BinaryIO]:
        with self._lock, self.db.connect() as conn:
            row = conn.execute(
                "SELECT file_name, file_path, sha256, byte_size FROM source_versions "
                "WHERE version=?",
                (version,),
            ).fetchone()
            if row is None:
                raise ApiError(404, "source_not_found", f"Unknown source version {version}")
            try:
                handle = open(row["file_path"], "rb")
            except OSError as exc:
                raise ApiError(
                    409, "source_version_unreadable",
                    f"Source version {version} cannot be read: {exc}",
                )
        problem = self._verify_stream(handle, row["sha256"], int(row["byte_size"]))
        if problem:
            handle.close()
            raise ApiError(
                409, "source_version_corrupt",
                f"Source version {version} is not servable: {problem}",
            )
        handle.seek(0)
        return row["file_name"], int(row["byte_size"]), handle

    # ----------------------------------------------------------- source uploads

    def upload_source(self, raw: bytes, filename: str, base_version: int) -> dict:
        """Validate and atomically activate a replacement LIMS extract.

        Optimistic concurrency: the caller states the version it based the
        correction on.  Two uploads based on the same current version race;
        only the first activates, the other gets 409 and changes nothing.
        """
        # Validate the whole upload before touching the volume or taking the lock.
        if len(raw) > self.settings.max_upload_bytes:
            raise ApiError(
                413,
                "source_too_large",
                f"Upload is {len(raw)} bytes; limit is {self.settings.max_upload_bytes} bytes",
            )
        data = LimsData(raw, error_status=400)
        safe_name = Path(filename or "lims.csv").name or "lims.csv"

        with self._lock:
            with self.db.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                current_row = conn.execute(
                    "SELECT version FROM source_versions ORDER BY version DESC LIMIT 1"
                ).fetchone()
                current = current_row["version"]
                if base_version != current:
                    conn.execute("COMMIT")
                    raise ApiError(
                        409,
                        "source_version_conflict",
                        f"Upload is based on version {base_version} but current version is {current}; "
                        "refetch the current version and rebase the correction",
                    )
                next_version = current + 1
                final_path = self.settings.source_dir / f"v{next_version}.csv"
                # File fully written and fsynced *before* the row is committed,
                # so a crash leaves either no row or a row whose file is durable.
                try:
                    self._atomic_write_bytes(final_path, data.raw)
                except OSError as exc:
                    try:
                        final_path.unlink(missing_ok=True)
                    except OSError:
                        pass
                    conn.execute("COMMIT")
                    raise ApiError(
                        500, "source_persist_failed",
                        f"Could not persist source version {next_version}: {exc}",
                    )
                now = utc_now()
                conn.execute(
                    "INSERT INTO source_versions(version, file_name, file_path, sha256, "
                    "byte_size, row_count, subject_count, base_version, activated_at, boot_id) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        next_version,
                        safe_name,
                        str(final_path),
                        data.sha256,
                        data.byte_size,
                        data.row_count,
                        data.subject_count,
                        base_version,
                        now,
                        self.boot_id,
                    ),
                )
                conn.execute("COMMIT")
            # Commit succeeded: the correction is durable and must survive even
            # if the process dies right after this point.
            self._sources[next_version] = data
            self.current_version = next_version
            self.lims = data
        return self.get_source(next_version)

    # ------------------------------------------------------------------ start-up

    def _bootstrap_consent(self) -> None:
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            seeded = conn.execute("SELECT value FROM meta WHERE key='consent_seeded'").fetchone()
            if seeded is None:
                now = utc_now()
                for subject_id in load_initial_consent(self.settings.initial_consent_csv):
                    conn.execute(
                        "INSERT INTO subjects(subject_id, active, active_since, last_changed_at) "
                        "VALUES(?, 1, ?, ?) ON CONFLICT(subject_id) DO NOTHING",
                        (subject_id, now, now),
                    )
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('consent_seeded', '1') "
                    "ON CONFLICT(key) DO NOTHING"
                )
            conn.execute("COMMIT")

    def _recover_after_restart(self) -> None:
        # Leftover temp files from a crashed worker can never be published.
        for stale in self.settings.batch_dir.glob(".*.tmp"):
            try:
                stale.unlink()
            except OSError:
                pass
        with self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for row in conn.execute("SELECT id FROM batches WHERE status='running'").fetchall():
                self._mark_failed(
                    conn,
                    row["id"],
                    "service restarted before generation finished",
                    boot_id=self.boot_id,
                )
            for row in conn.execute(
                "SELECT id, file_path, file_sha256, file_size FROM batches "
                "WHERE status='completed'"
            ).fetchall():
                problem = self._verify_file(row["file_path"], row["file_sha256"], row["file_size"])
                if problem:
                    self._mark_failed(
                        conn, row["id"], f"generated file unusable after restart: {problem}"
                    )
            conn.execute("COMMIT")

    def _verify_file(self, rel_path: Optional[str], sha: Optional[str], size: Optional[int]) -> Optional[str]:
        if not rel_path or not sha or size is None:
            return "file record missing"
        try:
            with open(rel_path, "rb") as handle:
                return self._verify_stream(handle, sha, size)
        except OSError:
            return "file missing or unreadable"

    @staticmethod
    def _verify_stream(handle, sha: str, size: int, chunk: int = 1 << 20) -> Optional[str]:
        actual_size = 0
        digest = hashlib.sha256()
        while True:
            block = handle.read(chunk)
            if not block:
                break
            actual_size += len(block)
            digest.update(block)
        if actual_size != size:
            return "size mismatch"
        if digest.hexdigest() != sha:
            return "digest mismatch"
        return None

    # ------------------------------------------------------------------ shutdown

    def shutdown(self) -> None:
        """Stop workers promptly; unfinished batches are failed on next start."""
        self._stopping.set()

    # ------------------------------------------------------------------- consent

    def list_subjects(self) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT subject_id, active, active_since, last_changed_at FROM subjects "
                "ORDER BY subject_id"
            ).fetchall()
            epochs = {
                r["subject_id"]: r["epoch"]
                for r in conn.execute(
                    "SELECT subject_id, MAX(epoch) AS epoch FROM consent_events GROUP BY subject_id"
                ).fetchall()
            }
        return [
            {
                "subject_id": r["subject_id"],
                "consented": bool(r["active"]),
                "consented_since": r["active_since"] if r["active"] else None,
                "last_changed_at": r["last_changed_at"],
                "revocation_epoch": epochs.get(r["subject_id"], 0),
            }
            for r in rows
        ]

    def get_subject(self, subject_id: str) -> dict:
        for item in self.list_subjects():
            if item["subject_id"] == subject_id:
                return item
        raise ApiError(404, "subject_not_found", f"Unknown subject {subject_id!r}")

    def set_consent(self, subject_id: str, consented: bool) -> dict:
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = utc_now()
            row = conn.execute(
                "SELECT active FROM subjects WHERE subject_id=?", (subject_id,)
            ).fetchone()
            if row is not None and bool(row["active"]) == consented:
                conn.execute("COMMIT")
                return {**self.get_subject(subject_id), "changed": False}
            if row is None:
                conn.execute(
                    "INSERT INTO subjects(subject_id, active, active_since, last_changed_at) "
                    "VALUES(?, ?, ?, ?)",
                    (subject_id, int(consented), now, now),
                )
            else:
                conn.execute(
                    "UPDATE subjects SET active=?, active_since=?, last_changed_at=? "
                    "WHERE subject_id=?",
                    (int(consented), now if consented else "", now, subject_id),
                )
            max_epoch = conn.execute(
                "SELECT COALESCE(MAX(epoch), 0) FROM consent_events WHERE subject_id=?",
                (subject_id,),
            ).fetchone()[0]
            epoch = max_epoch if consented else max_epoch + 1
            conn.execute(
                "INSERT INTO consent_events(subject_id, active, changed_at, epoch) "
                "VALUES(?, ?, ?, ?)",
                (subject_id, int(consented), now, epoch),
            )
            if not consented:
                for b in conn.execute(
                    "SELECT batch_id FROM batch_subjects WHERE subject_id=?", (subject_id,)
                ).fetchall():
                    self._invalidate_batch(
                        conn, b["batch_id"], f"subject {subject_id} revoked consent"
                    )
            conn.execute("COMMIT")
        return {**self.get_subject(subject_id), "changed": True}

    def _invalidate_batch(self, conn, batch_id: str, reason: str) -> None:
        """Make a batch permanently undownloadable. Idempotent; never revives it.

        A batch that already failed merely loses any file record (a failed
        batch must never be servable); a running or completed batch becomes
        ``invalidated``.  A batch that is already invalidated stays so.
        """
        row = conn.execute("SELECT status FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            return
        if row["status"] in (STATUS_RUNNING, STATUS_COMPLETED):
            conn.execute(
                "UPDATE batches SET status=?, failure_reason=?, finished_at=? WHERE id=?",
                (STATUS_INVALIDATED, reason, utc_now(), batch_id),
            )
            self._event(conn, batch_id, "invalidated", reason)
        self._remove_file(conn, batch_id)

    def _remove_file(self, conn, batch_id: str) -> None:
        row = conn.execute("SELECT file_path FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row and row["file_path"]:
            try:
                Path(row["file_path"]).unlink(missing_ok=True)
            except OSError:
                pass
        conn.execute(
            "UPDATE batches SET file_path=NULL, file_sha256=NULL, file_size=NULL WHERE id=?",
            (batch_id,),
        )

    # ------------------------------------------------------------------- batches

    def create_batch(self) -> dict:
        """Admit currently-consented subjects and start a background generation.

        The source extract version current at this instant is pinned on the
        batch row; the worker reads only that frozen file, so a replacement
        activated during generation never mixes data into this batch.
        """
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            source_version = conn.execute(
                "SELECT MAX(version) AS v FROM source_versions"
            ).fetchone()["v"]
            source = self._source_for_version(source_version)
            batch_id = uuid.uuid4().hex
            now = utc_now()
            conn.execute(
                "INSERT INTO batches(id, created_at, status, boot_id, source_version) "
                "VALUES(?,?,?,?,?)",
                (batch_id, now, STATUS_RUNNING, self.boot_id, source_version),
            )
            admitted_count = 0
            admitted = conn.execute(
                "SELECT s.subject_id, COALESCE(MAX(e.epoch), 0) AS epoch "
                "FROM subjects s LEFT JOIN consent_events e ON e.subject_id = s.subject_id "
                "WHERE s.active = 1 GROUP BY s.subject_id"
            ).fetchall()
            for row in admitted:
                if row["subject_id"] not in source.subject_ids:
                    continue  # nobody in this extract: nothing to generate for them
                self.codes.get_or_create(conn, row["subject_id"], now)
                conn.execute(
                    "INSERT INTO batch_subjects(batch_id, subject_id, consent_epoch) VALUES(?,?,?)",
                    (batch_id, row["subject_id"], row["epoch"]),
                )
                admitted_count += 1
            self._event(
                conn, batch_id, "created",
                f"{admitted_count} consenting subjects admitted; source version {source_version}",
            )
            conn.execute("COMMIT")
        thread = threading.Thread(
            target=self._generate, args=(batch_id,), name=f"batch-{batch_id}", daemon=True
        )
        with self._lock:
            self._workers[batch_id] = thread
        thread.start()
        return self.get_batch(batch_id)

    def _event(self, conn, batch_id: str, kind: str, detail: str = "") -> None:
        conn.execute(
            "INSERT INTO batch_events(batch_id, at, kind, detail) VALUES(?,?,?,?)",
            (batch_id, utc_now(), kind, detail),
        )

    def _mark_failed(self, conn, batch_id: str, reason: str, boot_id: Optional[str] = None) -> None:
        updated = conn.execute(
            "UPDATE batches SET status=?, failure_reason=?, finished_at=? "
            "WHERE id=? AND status IN (?, ?)",
            (STATUS_FAILED, reason, utc_now(), batch_id, STATUS_RUNNING, STATUS_COMPLETED),
        ).rowcount
        if boot_id:
            conn.execute("UPDATE batches SET boot_id=? WHERE id=?", (boot_id, batch_id))
        if updated:
            self._remove_file(conn, batch_id)
            self._event(conn, batch_id, "failed", reason)

    def _generate(self, batch_id: str) -> None:
        tmp_path = self.settings.batch_dir / f".{batch_id}.csv.tmp"
        final_path = self.settings.batch_dir / f"{batch_id}.csv"
        try:
            self._generate_inner(batch_id, tmp_path, final_path)
        except _BatchInvalidated:
            pass  # status already records invalidation; nothing is published
        except _WorkerStopped:
            pass  # left "running"; the next process start marks it failed
        except Exception as exc:  # noqa: BLE001 - any failure must fail the batch
            self._fail(batch_id, f"generation error: {exc.__class__.__name__}: {exc}")
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            self._drop_file_unless_completed(batch_id, final_path)
            with self._lock:
                self._workers.pop(batch_id, None)

    def _drop_file_unless_completed(self, batch_id: str, final_path: Path) -> None:
        """Delete a stray published file if the batch did not complete.

        Failure paths normally record status first and let ``_remove_file``
        unlink; if the process is shutting down, the status write may be
        skipped, so files are removed here as well.
        """
        try:
            published = self._status(batch_id) == STATUS_COMPLETED
        except Exception:  # noqa: BLE001
            published = False
        if not published:
            try:
                final_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _generate_inner(self, batch_id: str, tmp_path: Path, final_path: Path) -> None:
        if self._stopping.is_set():
            raise _WorkerStopped()
        snapshot = self._admitted_epochs(batch_id)
        if snapshot is None:
            raise _BatchInvalidated()
        problem = self._validity_problem(batch_id)
        if problem:
            raise _BatchInvalidated()
        # Resolve the exact extract version pinned at admission; this object is
        # immutable and independent of any replacement activated meanwhile.
        source = self._source_for_version(self._batch_source_version(batch_id))
        codes = self._codes_for(batch_id)
        digest = hashlib.sha256()
        size = 0
        row_count = 0
        check_every = max(self.settings.check_interval_rows, 1)
        with open(tmp_path, "wb") as handle:
            def write_row(fields: list[str]) -> None:
                nonlocal size
                buf = io.StringIO()
                csv.writer(buf, lineterminator=source.line_terminator).writerow(fields)
                data = buf.getvalue().encode("utf-8")
                handle.write(data)
                digest.update(data)
                size += len(data)

            write_row(source.output_header)
            for row in source.rows:
                subject = row[source.subject_idx].strip()
                code = codes.get(subject)
                if code is None:
                    continue  # not consented when this batch started: never include
                out = list(row)
                out[source.subject_idx] = code
                write_row(out)
                row_count += 1
                if self.settings.row_delay_seconds:
                    time.sleep(self.settings.row_delay_seconds)
                if row_count % check_every == 0:
                    if self._stopping.is_set():
                        raise _WorkerStopped()
                    problem = self._validity_problem(batch_id)
                    if problem:
                        raise _BatchInvalidated()
            handle.flush()
            os.fsync(handle.fileno())

        problem = self._validity_problem(batch_id)
        if problem:
            raise _BatchInvalidated()
        os.replace(tmp_path, final_path)  # atomic publish within the data volume
        if not self._commit_completion(
            batch_id, str(final_path), digest.hexdigest(), size, row_count, snapshot
        ):
            final_path.unlink(missing_ok=True)
            raise _BatchInvalidated()

    def _commit_completion(
        self,
        batch_id: str,
        path: str,
        sha: str,
        size: int,
        row_count: int,
        snapshot: dict[str, int],
    ) -> bool:
        with self._lock, self.db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT status FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None or row["status"] != STATUS_RUNNING:
                conn.execute("COMMIT")
                return False
            # Re-check under the same lock consent changes take: no revocation
            # can slip in between the final epoch check and this commit.
            if not self._epochs_match(conn, batch_id, snapshot):
                self._invalidate_batch(
                    conn, batch_id, "subject consent revoked during generation"
                )
                conn.execute("COMMIT")
                return False
            conn.execute(
                "UPDATE batches SET status=?, finished_at=?, row_count=?, file_path=?, "
                "file_sha256=?, file_size=? WHERE id=?",
                (STATUS_COMPLETED, utc_now(), row_count, path, sha, size, batch_id),
            )
            self._event(conn, batch_id, "completed", f"{row_count} rows")
            conn.execute("COMMIT")
        return True

    def _admitted_epochs(self, batch_id: str) -> Optional[dict[str, int]]:
        with self.db.connect() as conn:
            if conn.execute("SELECT 1 FROM batches WHERE id=?", (batch_id,)).fetchone() is None:
                return None
            rows = conn.execute(
                "SELECT subject_id, consent_epoch FROM batch_subjects WHERE batch_id=?",
                (batch_id,),
            ).fetchall()
        return {r["subject_id"]: r["consent_epoch"] for r in rows}

    def _batch_source_version(self, batch_id: str) -> int:
        with self.db.connect() as conn:
            row = conn.execute(
                "SELECT source_version FROM batches WHERE id=?", (batch_id,)
            ).fetchone()
        if row is None:
            raise ApiError(404, "batch_not_found", f"Unknown batch {batch_id!r}")
        return int(row["source_version"])

    def _codes_for(self, batch_id: str) -> dict[str, str]:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT bs.subject_id, c.code FROM batch_subjects bs "
                "JOIN codes c ON c.subject_id = bs.subject_id WHERE bs.batch_id=?",
                (batch_id,),
            ).fetchall()
        return {r["subject_id"]: r["code"] for r in rows}

    def _validity_problem(self, batch_id: str) -> Optional[str]:
        """Return why the batch must stop, or None if it may continue."""
        with self.db.connect() as conn:
            row = conn.execute("SELECT status, failure_reason FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                return "batch missing"
            if row["status"] != STATUS_RUNNING:
                return f"batch is {row['status']}"
            snapshot = {
                r["subject_id"]: r["consent_epoch"]
                for r in conn.execute(
                    "SELECT subject_id, consent_epoch FROM batch_subjects WHERE batch_id=?",
                    (batch_id,),
                ).fetchall()
            }
            if not self._epochs_match(conn, batch_id, snapshot):
                return "subject consent revoked during generation"
        return None

    @staticmethod
    def _epochs_match(conn, batch_id: str, snapshot: dict[str, int]) -> bool:
        current = {
            r["subject_id"]: r["epoch"]
            for r in conn.execute(
                "SELECT bs.subject_id, COALESCE((SELECT MAX(e.epoch) FROM consent_events e "
                "WHERE e.subject_id = bs.subject_id), 0) AS epoch "
                "FROM batch_subjects bs WHERE bs.batch_id=?",
                (batch_id,),
            ).fetchall()
        }
        return all(current.get(s, 0) == e for s, e in snapshot.items())

    def _fail(self, batch_id: str, reason: str) -> None:
        try:
            with self.db.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._mark_failed(conn, batch_id, reason)
                conn.execute("COMMIT")
        except sqlite3.Error:
            pass

    def _status(self, batch_id: str) -> Optional[str]:
        try:
            with self.db.connect() as conn:
                row = conn.execute("SELECT status FROM batches WHERE id=?", (batch_id,)).fetchone()
            return row["status"] if row else None
        except sqlite3.Error:
            return None

    # -------------------------------------------------------------- batch queries

    def list_batches(self) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM batches ORDER BY created_at DESC, id"
            ).fetchall()
        return [self._batch_dict(r) for r in rows]

    def get_batch(self, batch_id: str) -> dict:
        with self.db.connect() as conn:
            row = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            events = conn.execute(
                "SELECT at, kind, detail FROM batch_events WHERE batch_id=? ORDER BY id",
                (batch_id,),
            ).fetchall()
        if row is None:
            raise ApiError(404, "batch_not_found", f"Unknown batch {batch_id!r}")
        result = self._batch_dict(row)
        result["events"] = [dict(e) for e in events]
        return result

    @staticmethod
    def _batch_dict(row) -> dict:
        return {
            "batch_id": row["id"],
            "created_at": row["created_at"],
            "status": row["status"],
            "failure_reason": row["failure_reason"],
            "finished_at": row["finished_at"],
            "row_count": row["row_count"],
            "source_version": row["source_version"],
            "downloadable": row["status"] == STATUS_COMPLETED,
        }

    # ------------------------------------------------------------------ downloads

    def open_download(self, batch_id: str) -> tuple[str, int, BinaryIO]:
        """Verify and open a batch file for streaming.

        Status is checked and the file is opened under the admission/revocation
        lock, so a revocation cannot interleave between the check and the open:
        any request that reaches the status check after the revocation is
        refused, while a download that has opened its handle may finish even if
        the file is unlinked (POSIX keeps the open descriptor readable).
        The open handle itself is then verified against the recorded digest.
        """
        with self._lock, self.db.connect() as conn:
            row = conn.execute(
                "SELECT status, failure_reason, file_path, file_sha256, file_size "
                "FROM batches WHERE id=?",
                (batch_id,),
            ).fetchone()
            if row is None:
                raise ApiError(404, "batch_not_found", f"Unknown batch {batch_id!r}")
            status = row["status"]
            if status == STATUS_INVALIDATED:
                raise ApiError(
                    410,
                    "batch_invalidated",
                    "Batch was permanently invalidated by a consent revocation",
                )
            if status == STATUS_FAILED:
                raise ApiError(
                    409,
                    "batch_failed",
                    f"Batch failed and cannot be downloaded: {row['failure_reason'] or 'unknown'}",
                )
            if status != STATUS_COMPLETED:
                raise ApiError(409, "batch_running", "Batch is still generating; not downloadable yet")
            if not row["file_path"] or not row["file_sha256"] or row["file_size"] is None:
                raise ApiError(409, "file_unavailable", "Generated file is not servable: record missing")
            try:
                handle = open(row["file_path"], "rb")
            except OSError as exc:
                raise ApiError(409, "file_unavailable", f"Cannot open generated file: {exc}")

        # Verify the very handle that will be streamed, not just the path, so
        # the file cannot be swapped between verification and serving.
        problem = self._verify_stream(handle, row["file_sha256"], int(row["file_size"]))
        if problem:
            handle.close()
            raise ApiError(409, "file_unavailable", f"Generated file is not servable: {problem}")
        handle.seek(0)
        return f"{batch_id}.csv", int(row["file_size"]), handle
