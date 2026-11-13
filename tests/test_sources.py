"""Tests for LIMS source versioning: registration, replacement, concurrency,
per-batch pinning, crash-safe activation and legacy data-volume migration."""

from __future__ import annotations

import csv
import io
import shutil
import sqlite3
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def workdir(tmp_path: Path) -> Path:
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    shutil.copy(ROOT / "samples/inputs/lims.csv", inputs / "lims.csv")
    shutil.copy(ROOT / "samples/inputs/lims_correction_1.csv", inputs / "lims_correction_1.csv")
    shutil.copy(ROOT / "samples/inputs/lims_correction_2.csv", inputs / "lims_correction_2.csv")
    shutil.copy(ROOT / "samples/inputs/initial_consent.csv", inputs / "initial_consent.csv")
    return tmp_path


def make_app(workdir: Path, row_delay: float = 0.0):
    settings = Settings(
        input_csv=workdir / "inputs/lims.csv",
        initial_consent_csv=workdir / "inputs/initial_consent.csv",
        data_dir=workdir / "data",
        row_delay_seconds=row_delay,
        check_interval_rows=1,
    )
    return create_app(settings), settings


def read_csv(text: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(text)))


def upload(client, path: Path, base_version: int):
    with path.open("rb") as handle:
        return client.post(
            "/sources",
            files={"file": (path.name, handle, "text/csv")},
            data={"base_version": str(base_version)},
        )


def wait_status(client, batch_id, statuses, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = client.get(f"/batches/{batch_id}").json()
        if body["status"] in statuses:
            return body
        time.sleep(0.02)
    raise AssertionError(f"batch never reached {statuses}: {body}")


def test_config_csv_registered_as_version_one(workdir):
    app, settings = make_app(workdir)
    with TestClient(app) as client:
        current = client.get("/sources/current")
        assert current.status_code == 200
        body = current.json()
        assert body["version"] == 1
        assert body["is_current"] is True
        assert body["base_version"] is None
        assert body["row_count"] == 150
        assert body["subject_count"] == 10
        listing = client.get("/sources").json()
        assert listing["current_version"] == 1
        assert [v["version"] for v in listing["versions"]] == [1]
        # The seeded file lives inside the data volume, not next to the config.
        assert body["sha256"] == read_sha(settings.data_dir / "sources" / "v1.csv")
        # Every new batch pins v1.
        batch = client.post("/batches").json()
        assert batch["source_version"] == 1
        assert "source version 1" in [e["detail"] for e in batch["events"]][0]


def read_sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_replacement_activates_and_subsequent_batches_use_new_version(workdir):
    app, settings = make_app(workdir)
    with TestClient(app) as client:
        # A batch on v1 captures the original S001 value 8.0.
        first = client.post("/batches").json()
        wait_status(client, first["batch_id"], {"completed"})
        assert first["source_version"] == 1
        v1_csv = read_csv(client.get(f"/batches/{first['batch_id']}/download").text)
        s001_v1 = row_for(v1_csv[1:], subject_code(client, "S001"))
        assert s001_v1[3] == "8.0"

        resp = upload(client, workdir / "inputs/lims_correction_1.csv", base_version=1)
        assert resp.status_code == 201, resp.text
        v2 = resp.json()
        assert v2["version"] == 2
        assert v2["base_version"] == 1
        assert v2["is_current"] is True
        assert v2["row_count"] == 150
        assert client.get("/sources/current").json()["version"] == 2
        assert client.get("/sources/1").json()["is_current"] is False

        second = client.post("/batches").json()
        wait_status(client, second["batch_id"], {"completed"})
        assert second["source_version"] == 2
        v2_batch_csv = read_csv(client.get(f"/batches/{second['batch_id']}/download").text)
        s001_v2 = row_for(v2_batch_csv[1:], subject_code(client, "S001"))
        assert s001_v2[3] == "9.0"  # corrected value, codes stay stable

        # The old batch's file and its pinned version are untouched.
        old = client.get(f"/batches/{first['batch_id']}").json()
        assert old["source_version"] == 1
        assert read_csv(client.get(f"/batches/{first['batch_id']}/download").text) == v1_csv


def subject_code(client, subject_id: str) -> str:
    with client.app.state.service.db.connect() as conn:
        return conn.execute(
            "SELECT code FROM codes WHERE subject_id=?", (subject_id,)
        ).fetchone()["code"]


def row_for(rows: list[list[str]], code: str) -> list[str]:
    return next(r for r in rows if r[0] == code)


def test_stale_base_version_conflicts_and_changes_nothing(workdir):
    app, _ = make_app(workdir)
    with TestClient(app) as client:
        first = upload(client, workdir / "inputs/lims_correction_1.csv", base_version=1)
        assert first.status_code == 201
        # An upload still based on v1 must now be rejected.
        conflict = upload(client, workdir / "inputs/lims_correction_2.csv", base_version=1)
        assert conflict.status_code == 409
        assert conflict.json()["error"] == "source_version_conflict"
        assert client.get("/sources/current").json()["version"] == 2

        # Rebasing onto v2 succeeds: correction 2 activates as v3.
        ok = upload(client, workdir / "inputs/lims_correction_2.csv", base_version=2)
        assert ok.status_code == 201
        assert ok.json()["version"] == 3
        assert client.get("/sources/current").json()["version"] == 3
        # v3 adds S011, which now appears in a fresh batch once consented.
        client.put("/subjects/S011/consent", json={"consented": True})
        batch = client.post("/batches").json()
        assert batch["source_version"] == 3
        wait_status(client, batch["batch_id"], {"completed"})
        out = client.get(f"/batches/{batch['batch_id']}/download").text
        assert subject_code(client, "S011") in out


def test_concurrent_replacements_of_same_base_activate_once(workdir):
    app, _ = make_app(workdir)
    raw = (workdir / "inputs/lims_correction_1.csv").read_bytes()
    outcomes: list[str] = []
    barrier = threading.Barrier(2)
    service = app.state.service

    def worker():
        barrier.wait()
        try:
            service.upload_source(raw, "lims_correction_1.csv", 1)
            outcomes.append("ok")
        except Exception as exc:  # noqa: BLE001
            outcomes.append(exc.code if hasattr(exc, "code") else "error")

    with TestClient(app):  # normal lifespan; service is the shared singleton
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert sorted(outcomes) == ["ok", "source_version_conflict"]
    with TestClient(app) as client:
        listing = client.get("/sources").json()
        assert [v["version"] for v in listing["versions"]] == [1, 2]
        assert listing["current_version"] == 2
        # Only the winner's file exists; no temp leftovers.
        files = sorted(p.name for p in (workdir / "data/sources").glob("*"))
        assert files == ["v1.csv", "v2.csv"]


def test_invalid_upload_rejected_without_changing_current(workdir):
    app, _ = make_app(workdir)
    with TestClient(app) as client:
        bad_header = b"subject_id,test_date,test_item\nS001,2025-01-06,X\n"
        resp = client.post(
            "/sources",
            files={"file": ("bad.csv", bad_header, "text/csv")},
            data={"base_version": "1"},
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "source_invalid"

        ragged = b"subject_id,test_date,test_item,value\nS001,2025-01-06,X\n"
        resp2 = client.post(
            "/sources",
            files={"file": ("ragged.csv", ragged, "text/csv")},
            data={"base_version": "1"},
        )
        assert resp2.status_code == 400

        empty = b""
        resp3 = client.post(
            "/sources",
            files={"file": ("empty.csv", empty, "text/csv")},
            data={"base_version": "1"},
        )
        assert resp3.status_code in (400, 422)

        current = client.get("/sources/current").json()
        assert current["version"] == 1
        assert list((workdir / "data/sources").glob("*")) == [
            workdir / "data/sources/v1.csv"
        ]


def test_oversize_upload_rejected_with_413(workdir):
    settings = Settings(
        input_csv=workdir / "inputs/lims.csv",
        initial_consent_csv=workdir / "inputs/initial_consent.csv",
        data_dir=workdir / "data",
        row_delay_seconds=0.0,
        check_interval_rows=1,
        max_upload_bytes=64,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        big = (workdir / "inputs/lims_correction_1.csv").read_bytes()  # well over 64 bytes
        assert len(big) > 64
        resp = client.post(
            "/sources",
            files={"file": ("big.csv", big, "text/csv")},
            data={"base_version": "1"},
        )
        assert resp.status_code == 413
        assert resp.json()["error"] == "source_too_large"
        assert client.get("/sources/current").json()["version"] == 1


def test_swap_during_generation_keeps_batch_on_pinned_version(workdir):
    app, _ = make_app(workdir, row_delay=0.02)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        assert batch["source_version"] == 1
        # Replace the source while the worker is still streaming rows.
        time.sleep(0.15)
        swap = upload(client, workdir / "inputs/lims_correction_1.csv", base_version=1)
        assert swap.status_code == 201
        assert client.get("/sources/current").json()["version"] == 2

        done = wait_status(client, batch["batch_id"], {"completed", "failed", "invalidated"})
        assert done["status"] == "completed"
        assert done["source_version"] == 1  # never mixed in v2 data
        rows = read_csv(client.get(f"/batches/{batch['batch_id']}/download").text)[1:]
        s001 = row_for(rows, subject_code(client, "S001"))
        assert s001[3] == "8.0"  # v1 value, not the corrected 9.0

        # A later batch uses v2.
        nxt = client.post("/batches").json()
        wait_status(client, nxt["batch_id"], {"completed"})
        assert nxt["source_version"] == 2
        rows2 = read_csv(client.get(f"/batches/{nxt['batch_id']}/download").text)[1:]
        s001b = row_for(rows2, subject_code(client, "S001"))
        assert s001b[3] == "9.0"


def test_historical_source_versions_remain_downloadable(workdir):
    app, _ = make_app(workdir)
    with TestClient(app) as client:
        upload(client, workdir / "inputs/lims_correction_1.csv", base_version=1)
        upload(client, workdir / "inputs/lims_correction_2.csv", base_version=2)
        original = (workdir / "inputs/lims.csv").read_bytes()
        r1 = client.get("/sources/1/download")
        assert r1.status_code == 200
        assert r1.content == original
        assert r1.headers["content-disposition"].startswith('attachment; filename="v1-lims.csv"')
        r3 = client.get("/sources/3/download")
        assert r3.content == (workdir / "inputs/lims_correction_2.csv").read_bytes()
        assert client.get("/sources/99/download").status_code == 404


def test_swap_does_not_touch_consent_codes_or_old_batches(workdir):
    app, _ = make_app(workdir)
    with TestClient(app) as client:
        batch = client.post("/batches").json()
        wait_status(client, batch["batch_id"], {"completed"})
        before = client.get("/subjects").json()
        code_before = subject_code(client, "S001")

        upload(client, workdir / "inputs/lims_correction_2.csv", base_version=1)
        # v3 introduces S011; consent roster is unchanged.
        after = client.get("/subjects").json()
        assert after == before
        assert subject_code(client, "S001") == code_before
        old = client.get(f"/batches/{batch['batch_id']}").json()
        assert old["status"] == "completed"
        assert old["source_version"] == 1
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 200

        # Revocation rules still apply regardless of the active version.
        client.put("/subjects/S002/consent", json={"consented": False})
        assert client.get(f"/batches/{batch['batch_id']}/download").status_code == 410


def test_activation_is_durable_across_restart_and_running_batch_fails(workdir):
    app1, _ = make_app(workdir, row_delay=0.0)
    with TestClient(app1) as client:
        upload(client, workdir / "inputs/lims_correction_1.csv", base_version=1)

    # A slow batch starts on v2, then the process is restarted mid-flight.
    app2, _ = make_app(workdir, row_delay=10.0)
    with TestClient(app2) as client:
        running = client.post("/batches").json()
        assert running["source_version"] == 2

    app3, settings3 = make_app(workdir, row_delay=0.0)
    with TestClient(app3) as client:
        # The activated correction survived; current version is still v2.
        assert client.get("/sources/current").json()["version"] == 2
        assert [v["version"] for v in client.get("/sources").json()["versions"]] == [1, 2]
        assert client.get("/sources/2/download").content == (
            workdir / "inputs/lims_correction_1.csv"
        ).read_bytes()
        # Unfinished batch from the previous process fails, pinned version retained.
        dead = client.get(f"/batches/{running['batch_id']}").json()
        assert dead["status"] == "failed"
        assert dead["source_version"] == 2
        # New generation after restart uses the surviving v2.
        fresh = client.post("/batches").json()
        wait_status(client, fresh["batch_id"], {"completed"})
        assert fresh["source_version"] == 2
        rows = read_csv(client.get(f"/batches/{fresh['batch_id']}/download").text)[1:]
        s001 = row_for(rows, subject_code(client, "S001"))
        assert s001[3] == "9.0"
    # No half-activated temp files after restart.
    assert not list((settings3.data_dir / "sources").glob("*.tmp"))


def test_legacy_data_volume_migrates_config_csv_to_version_one(workdir):
    """A volume written by the pre-versioning build gets v1 registration + backfill."""
    # First build an old-style database: source_versions absent, batches without
    # the source_version column.
    data_dir = workdir / "data"
    (data_dir / "batches").mkdir(parents=True)
    db_path = data_dir / "deid.sqlite3"
    legacy = sqlite3.connect(db_path)
    legacy.executescript(
        """
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE subjects (
            subject_id TEXT PRIMARY KEY, active INTEGER NOT NULL,
            active_since TEXT NOT NULL, last_changed_at TEXT NOT NULL);
        CREATE TABLE consent_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, subject_id TEXT NOT NULL,
            active INTEGER NOT NULL, changed_at TEXT NOT NULL, epoch INTEGER NOT NULL);
        CREATE TABLE codes (subject_id TEXT PRIMARY KEY, code TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL);
        CREATE TABLE batches (
            id TEXT PRIMARY KEY, created_at TEXT NOT NULL, status TEXT NOT NULL,
            failure_reason TEXT, finished_at TEXT, boot_id TEXT NOT NULL,
            row_count INTEGER, file_path TEXT, file_sha256 TEXT, file_size INTEGER);
        CREATE TABLE batch_subjects (
            batch_id TEXT NOT NULL, subject_id TEXT NOT NULL,
            consent_epoch INTEGER NOT NULL, PRIMARY KEY (batch_id, subject_id));
        CREATE TABLE batch_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL,
            at TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT);
        """
    )
    legacy.execute(
        "INSERT INTO batches(id, created_at, status, boot_id, row_count) "
        "VALUES('legacybatch','2026-01-01T00:00:00.000+00:00','completed','oldboot',120)"
    )
    legacy.execute("INSERT INTO meta(key,value) VALUES('consent_seeded','1')")
    legacy.commit()
    legacy.close()

    app, _ = make_app(workdir)
    with TestClient(app) as client:
        current = client.get("/sources/current").json()
        assert current["version"] == 1
        assert current["file_name"] == "lims.csv"
        assert (workdir / "data/sources/v1.csv").read_bytes() == (
            workdir / "inputs/lims.csv"
        ).read_bytes()
        legacy_batch = client.get("/batches/legacybatch").json()
        assert legacy_batch["source_version"] == 1
        assert client.get("/batches").json()["batches"][0]["source_version"] == 1
        # Migration is idempotent on the next start.
    app2, _ = make_app(workdir)
    with TestClient(app2) as client:
        assert client.get("/sources/current").json()["version"] == 1
        assert [v["version"] for v in client.get("/sources").json()["versions"]] == [1]


def test_crash_mid_activation_leaves_only_committed_versions(workdir):
    """A file written but never committed must not become an active version."""
    app, settings = make_app(workdir)
    with TestClient(app):
        sources = settings.data_dir / "sources"
        # Simulate dying after the fsynced rename but before the DB commit:
        # an orphan v2 file and a leftover temp upload, with no v2 registry row.
        (sources / "v2.csv").write_bytes((workdir / "inputs/lims_correction_1.csv").read_bytes())
        (sources / ".v3.csv.upload.tmp").write_bytes(b"partial")
        with sqlite3.connect(settings.db_path) as conn:
            assert conn.execute("SELECT MAX(version) FROM source_versions").fetchone()[0] == 1

    app2, settings2 = make_app(workdir)
    with TestClient(app2) as client:
        # Only the fully committed v1 is usable; orphan files were swept away.
        assert client.get("/sources/current").json()["version"] == 1
        assert [v["version"] for v in client.get("/sources").json()["versions"]] == [1]
        assert client.get("/sources/2").status_code == 404
        remaining = sorted(p.name for p in (settings2.data_dir / "sources").glob("*"))
        assert remaining == ["v1.csv"]
