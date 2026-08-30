"""Per-session Parquet storage for raw OBD-II readings.

Sessions are referenced by session_id alone — the folder path is always derived
as SESSIONS_DIR/<session_id>/, never stored as a separate string. Whatever
created the session row only needs to remember the session_id to find its data
back here later.

Per CLAUDE.md: readings are stored raw (the long-format DataFrame from
parse_log()) and immutably; the resampled wide table is derived from this on
export, not stored here.
"""

from pathlib import Path

import pandas as pd

SESSIONS_DIR = Path(__file__).parent / "data" / "sessions"


def session_dir(session_id, base_dir=SESSIONS_DIR):
    """Folder for a session's stored data: <base_dir>/<session_id>/."""
    return Path(base_dir) / str(session_id)


def write_session_readings(session_id, df, base_dir=SESSIONS_DIR, overwrite=False):
    """Write a session's raw readings DataFrame to <base_dir>/<session_id>/raw.parquet.

    Creates the session folder if needed. Raises FileExistsError if raw.parquet
    already exists, unless overwrite=True — readings are meant to be written once
    and treated as immutable, so accidental re-imports shouldn't silently clobber
    the stored data.

    Returns the path written to.
    """

    directory = session_dir(session_id, base_dir)
    directory.mkdir(parents=True, exist_ok=True) ## Create the inner folder to store the parquet file
    path = directory / "raw.parquet"
    if path.exists() and not overwrite: ## If session has already been written, throw error
        raise FileExistsError(
            f"{path} already exists; pass overwrite=True to replace it"
        )
    df.to_parquet(path, index=False) ## Generate a parquet file from the raw dataframe
    return path


def load_session_readings(session_id, base_dir=SESSIONS_DIR):
    """Read back a session's raw readings DataFrame."""
    path = session_dir(session_id, base_dir) / "raw.parquet"
    return pd.read_parquet(path)  ## Given a parquet file path, read that as a dataframe so that we can use to make the wide table later
