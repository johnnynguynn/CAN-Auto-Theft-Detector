"""Create a session row and store its raw readings under that same session_id.

Ties db.create_session() and readings_store.write_session_readings() together
so the two can never disagree about which id they belong to: the session_id
always comes from the sessions table insert (SQLite's AUTOINCREMENT), and that
exact value is what the Parquet folder gets named after — nothing here
generates or guesses an id independently.
"""

from pathlib import Path

from db import create_session, delete_driver, delete_session, get_or_create_driver, session_exists_for_csv
from parser import parse_log
from readings_store import SESSIONS_DIR, write_session_readings


def create_session_with_readings(
    conn,
    driver_id,
    df,
    started_at,
    ended_at=None,
    source_csv=None,
    notes=None,
    base_dir=SESSIONS_DIR,
    overwrite=False,
):
    """Insert a session row, then write its raw readings to sessions/<session_id>/raw.parquet.

    Returns the new session_id.
    """
    session_id = create_session(conn, driver_id, started_at, ended_at, source_csv, notes)
    write_session_readings(session_id, df, base_dir=base_dir, overwrite=overwrite)
    return session_id


def ingest_csv(
    conn,
    csv_path,
    driver_name,
    started_at,
    ended_at=None,
    notes=None,
    base_dir=SESSIONS_DIR,
):
    """Full pipeline: a Car Scanner CSV export -> driver row -> session row -> raw Parquet file.

    Steps, in order:
      1. Resolve csv_path to an absolute, canonical path, and check whether
         that exact file has already been ingested (a session already has it
         as source_csv) — raises if so. Resolving first means the same file
         referenced via a relative path vs. an absolute path is still caught
         as the same file, not treated as two different ones.
      2. Parse the CSV into the raw long-format readings DataFrame.
      3. Look up driver_name in the drivers table; reuse its driver_id if
         found, otherwise insert a new driver row.
      4. Insert a new session row for that driver (source_csv recorded here
         is what step 1 checks against next time).
      5. Write the raw readings to sessions/<session_id>/raw.parquet, using
         that exact session_id — never a separately generated one.

    If step 5 fails (disk error, bad base_dir, etc.), the session row from
    step 4 is deleted, and the driver row from step 3 too if it was newly
    created by this call — so a failed ingest never leaves an orphaned
    driver/session row with no backing Parquet file.

    Returns (driver_id, session_id).
    """
    csv_path = Path(csv_path).resolve()
    if session_exists_for_csv(conn, csv_path):
        raise ValueError(f"{csv_path} has already been ingested as a session")

    raw_df = parse_log(csv_path)
    driver_id, driver_created = get_or_create_driver(conn, driver_name)
    session_id = create_session(
        conn, driver_id, started_at, ended_at, str(csv_path), notes
    )

    try:
        write_session_readings(session_id, raw_df, base_dir=base_dir)
    except Exception:
        delete_session(conn, session_id)
        if driver_created:
            delete_driver(conn, driver_id)
        raise

    return driver_id, session_id
