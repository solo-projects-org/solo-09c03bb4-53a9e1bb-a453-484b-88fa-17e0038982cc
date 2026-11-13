"""Runtime configuration, all overridable via environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default))).expanduser().resolve()


_DEFAULT_MAX_UPLOAD = 50 * 1024 * 1024


@dataclass(frozen=True)
class Settings:
    # Immutable inputs: the LIMS extract and the initial consent roster.
    input_csv: Path
    initial_consent_csv: Path
    # The one persistent volume: consent state, code registry, source version
    # registry, batch manifests, generated files and the SQLite database.
    data_dir: Path
    # Simulated per-row processing cost of a generation worker, in seconds.
    row_delay_seconds: float
    # How often a running worker re-checks whether its batch was invalidated.
    check_interval_rows: int
    # Largest accepted source upload, in bytes (whole file is held in memory).
    max_upload_bytes: int = field(default=_DEFAULT_MAX_UPLOAD)

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = _env_path("DATA_DIR", Path.cwd() / "data")
        return cls(
            input_csv=_env_path("INPUT_CSV", PROJECT_ROOT / "samples/inputs/lims.csv"),
            initial_consent_csv=_env_path(
                "INITIAL_CONSENT_CSV", PROJECT_ROOT / "samples/inputs/initial_consent.csv"
            ),
            data_dir=data_dir,
            row_delay_seconds=float(os.environ.get("ROW_DELAY_SECONDS", "0")),
            check_interval_rows=int(os.environ.get("CHECK_INTERVAL_ROWS", "50")),
            max_upload_bytes=int(os.environ.get("MAX_UPLOAD_BYTES", str(50 * 1024 * 1024))),
        )

    @property
    def db_path(self) -> Path:
        return self.data_dir / "deid.sqlite3"

    @property
    def batch_dir(self) -> Path:
        return self.data_dir / "batches"

    @property
    def source_dir(self) -> Path:
        return self.data_dir / "sources"
