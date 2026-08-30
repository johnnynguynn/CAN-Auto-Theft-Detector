"""SQLite storage for driver profiles and session metadata.

Per CLAUDE.md: SQLite holds small relational metadata (drivers, sessions).
Bulk readings live in per-session Parquet files, located by session_id via
readings_store.session_dir() — not handled by this module.
"""

import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def init_db(path):
    """Open (creating if needed) the SQLite file at `path` and ensure tables exist.

    Returns an open connection with foreign key enforcement turned on.
    """
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(SCHEMA_PATH.read_text())
    conn.commit()
    return conn


def create_driver(conn, name):
    """Insert a driver row and return its driver_id (assigned by SQLite).

    `name` is stripped of leading/trailing whitespace before storing, so an
    accidental " Johnny" doesn't become a distinct-looking stored name.
    """
    cur = conn.execute("INSERT INTO drivers (name) VALUES (?)", (name.strip(),))
    conn.commit()
    return cur.lastrowid


def get_driver_by_name(conn, name):
    """Return the driver_id of an existing driver matching this name, or None.

    Matches case- and surrounding-whitespace-insensitively (`lower(trim(...))`
    on both sides) so "Johnny", "johnny", and " Johnny" all resolve to the same
    driver instead of silently forking duplicate profiles.
    """
    row = conn.execute(
        "SELECT driver_id FROM drivers WHERE lower(trim(name)) = lower(trim(?)) LIMIT 1",
        (name,),
    ).fetchone()
    return row[0] if row else None


def get_or_create_driver(conn, name):
    """Look up a driver by name; only insert a new row if none exists yet.

    Returns (driver_id, created) — created is True if a new row was inserted,
    False if an existing driver was matched. Callers that need to roll back a
    failed operation can use `created` to know whether it's safe to delete the
    driver row too, or whether it pre-existed and should be left alone.
    """
    driver_id = get_driver_by_name(conn, name)
    if driver_id is not None:
        return driver_id, False
    return create_driver(conn, name), True


def delete_driver(conn, driver_id):
    """Delete a driver row. Used to roll back a driver created during a failed ingest."""
    conn.execute("DELETE FROM drivers WHERE driver_id = ?", (driver_id,))
    conn.commit()


def session_exists_for_csv(conn, source_csv):
    """True if some session already has this exact source_csv on record.

    `source_csv` should be a resolved/canonical path (see session_ingest.py) —
    this does an exact string match and does not itself resolve relative paths
    or symlinks.
    """
    row = conn.execute(
        "SELECT 1 FROM sessions WHERE source_csv = ? LIMIT 1", (str(source_csv),)
    ).fetchone()
    return row is not None


def normalize_session_csv_paths(conn):
    """Backfill: rewrite every sessions.source_csv to its resolved absolute path.

    ingest_csv() resolves csv_path before storing it, so new rows are always
    canonical. This fixes up rows inserted before that was true — otherwise
    their old, non-canonical path strings never text-match a freshly resolved
    path, and session_exists_for_csv() silently misses them as duplicates.

    Returns the number of rows updated.
    """
    rows = conn.execute(
        "SELECT session_id, source_csv FROM sessions WHERE source_csv IS NOT NULL"
    ).fetchall()
    updated = 0
    for session_id, source_csv in rows:
        resolved = str(Path(source_csv).resolve())
        if resolved != source_csv:
            conn.execute(
                "UPDATE sessions SET source_csv = ? WHERE session_id = ?",
                (resolved, session_id),
            )
            updated += 1
    conn.commit()
    return updated


def create_session(conn, driver_id, started_at, ended_at=None, source_csv=None, notes=None):
    """Insert a session row and return its session_id.

    The id is whatever SQLite's AUTOINCREMENT assigns — callers should treat
    this as the one and only session_id for the readings that belong to it
    (e.g. pass it straight to readings_store.write_session_readings), rather
    than generating an id separately.
    """
    cur = conn.execute(
        "INSERT INTO sessions (driver_id, started_at, ended_at, source_csv, notes) "
        "VALUES (?, ?, ?, ?, ?)",
        (driver_id, started_at, ended_at, source_csv, notes),
    )
    conn.commit()
    return cur.lastrowid


def delete_session(conn, session_id):
    """Delete a session row. Used to roll back a session left with no Parquet file."""
    conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
    conn.commit()
