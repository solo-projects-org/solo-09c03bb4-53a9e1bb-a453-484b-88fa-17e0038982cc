#!/usr/bin/env python3
"""End-to-end demonstration against a running API (default http://localhost:8000).

Part A -- source versioning (this build):
  1. query the current source version (the configured CSV is seeded as v1)
  2. start a batch, upload correction 1 *while it is generating* -> the batch
     keeps v1; a later batch uses v2; batch details report the pinned version
  3. a stale-base upload conflicts (409); rebasing activates correction 2 as v3
  4. every historical source version stays downloadable for tracing

Part B -- consent lifecycle:
  5. a batch generating while a subject is revoked -> invalidated, download 410
  6. re-consent does not revive the old batch; a fresh batch completes
  7. revoking again invalidates the completed batch too

The two correction samples are POSTed as multipart/form-data exactly as the
Swagger UI "Try it out" does (file + base_version form fields).
Requires ROW_DELAY_SECONDS > 0 (docker-compose sets 0.02) so steps 2 and 5 are
reliably mid-flight.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://localhost:8000"
REVOKE_SUBJECT = "S003"
SAMPLES = Path(__file__).resolve().parent.parent / "samples" / "inputs"
CORRECTION_1 = SAMPLES / "lims_correction_1.csv"
CORRECTION_2 = SAMPLES / "lims_correction_2.csv"


def call(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            ctype = resp.headers.get("content-type", "")
            return resp.status, (json.loads(raw) if "json" in ctype else raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw


def upload_source(csv_path: Path, base_version: int) -> tuple[int, object]:
    """POST /sources as multipart/form-data, mirroring the Swagger upload form."""
    boundary = "----deiddemo" + uuid_hex()
    content = csv_path.read_bytes()
    body = b"".join(
        [
            f"--{boundary}\r\n".encode(),
            b'Content-Disposition: form-data; name="base_version"\r\n\r\n',
            f"{base_version}\r\n".encode(),
            f"--{boundary}\r\n".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{csv_path.name}"\r\n'.encode(),
            b"Content-Type: text/csv\r\n\r\n",
            content,
            b"\r\n",
            f"--{boundary}--\r\n".encode(),
        ]
    )
    req = urllib.request.Request(
        BASE + "/sources",
        data=body,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def uuid_hex() -> str:
    import uuid

    return uuid.uuid4().hex


def wait(batch_id: str, terminal: set[str], timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, body = call("GET", f"/batches/{batch_id}")
        if status == 200 and body["status"] in terminal:
            return body
        time.sleep(0.1)
    raise SystemExit(f"batch {batch_id} did not reach {terminal}")


def show(label: str, status: int, body: object) -> None:
    if isinstance(body, bytes):
        preview = body.decode(errors="replace").splitlines()[:3]
        body = {"csv_head": preview, "bytes": len(body)}
    print(f"{label}: HTTP {status} {json.dumps(body, ensure_ascii=False)}")


def main() -> int:
    global BASE
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=BASE)
    args = parser.parse_args()
    BASE = args.base

    print("=" * 70)
    print("PART A · source replacement versioning")
    print("=" * 70)

    print("\n== A1. current source version (config CSV seeded as version 1) ==")
    status, current = call("GET", "/sources/current")
    show("GET /sources/current", status, current)
    status, listing = call("GET", "/sources")
    show("GET /sources", status, listing)

    print("\n== A2. start a batch, then replace the source while it generates ==")
    status, batch_v1 = call("POST", "/batches")
    show("POST /batches (pins current source)", status, batch_v1)
    time.sleep(0.4)  # let the worker get into the extract
    status, v2 = upload_source(CORRECTION_1, base_version=current["version"])
    show(f"POST /sources {CORRECTION_1.name} based on v1", status, v2)
    done = wait(batch_v1["batch_id"], {"completed", "failed", "invalidated"})
    show("first batch result (must stay on v1)", 200, done)

    print("\n== A3. a fresh batch uses the new version; codes stay stable ==")
    status, batch_v2 = call("POST", "/batches")
    show("POST /batches (now pins v2)", status, batch_v2)
    wait(batch_v2["batch_id"], {"completed", "failed", "invalidated"})
    show("batch list with pinned source versions", *call("GET", "/batches"))
    for bid, label in ((batch_v1["batch_id"], "v1 batch"), (batch_v2["batch_id"], "v2 batch")):
        _, detail = call("GET", f"/batches/{bid}")
        print(f"  {label} {bid[:8]} -> source_version={detail['source_version']} "
              f"status={detail['status']}")

    print("\n== A4. stale base version conflicts; rebasing activates correction 2 ==")
    status, conflict = upload_source(CORRECTION_2, base_version=1)
    show("POST /sources based on stale v1 (expect 409)", status, conflict)
    status, v3 = upload_source(CORRECTION_2, base_version=2)
    show(f"POST /sources {CORRECTION_2.name} rebased on v2", status, v3)
    show("GET /sources/current", *call("GET", "/sources/current"))

    print("\n== A5. historical source versions remain downloadable for tracing ==")
    for version in (1, 2, 3):
        status, body = call("GET", f"/sources/{version}/download")
        if isinstance(body, bytes):
            line = body.decode(errors="replace").splitlines()[1]
            print(f"  GET /sources/{version}/download -> HTTP {status}, "
                  f"{len(body)} bytes, first data row: {line}")
        else:
            show(f"GET /sources/{version}/download", status, body)

    print("\n" + "=" * 70)
    print("PART B · consent revocation lifecycle (unchanged by versioning)")
    print("=" * 70)

    print(f"\n== B1. start batch, revoke {REVOKE_SUBJECT} while it is generating ==")
    status, batch = call("POST", "/batches")
    show("POST /batches", status, batch)
    time.sleep(0.4)
    _, revoked = call("PUT", f"/subjects/{REVOKE_SUBJECT}/consent", {"consented": False})
    show(f"PUT /subjects/{REVOKE_SUBJECT}/consent (revoke)", 200, revoked)
    final = wait(batch["batch_id"], {"invalidated", "completed", "failed"})
    show("batch after revocation", 200, final)
    show("download invalidated batch", *call("GET", f"/batches/{batch['batch_id']}/download"))

    print(f"\n== B2. re-consent {REVOKE_SUBJECT}; the old batch must stay dead ==")
    show("PUT consent (re-grant)", 200,
         call("PUT", f"/subjects/{REVOKE_SUBJECT}/consent", {"consented": True})[1])
    show("download old batch after re-consent",
         *call("GET", f"/batches/{batch['batch_id']}/download"))

    print("\n== B3. new batch after re-consent generates normally ==")
    status, batch2 = call("POST", "/batches")
    show("POST /batches", status, batch2)
    wait(batch2["batch_id"], {"completed", "failed", "invalidated"})
    show("download new batch", *call("GET", f"/batches/{batch2['batch_id']}/download"))

    print(f"\n== B4. revoke again: completed batch {batch2['batch_id'][:8]} invalidates ==")
    show("PUT consent (revoke)", 200,
         call("PUT", f"/subjects/{REVOKE_SUBJECT}/consent", {"consented": False})[1])
    show("download completed batch after revocation",
         *call("GET", f"/batches/{batch2['batch_id']}/download"))

    _, after = call("GET", "/batches")
    print("\nfinal batch registry (each entry carries its source version):")
    print(json.dumps(after, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
