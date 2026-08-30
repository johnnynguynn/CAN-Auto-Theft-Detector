-- SQL queries to create tables 


-- Drivers table
CREATE TABLE IF NOT EXISTS drivers (
    driver_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Sessions table
-- Each entry is linked to a parquet file that stores raw 
-- information about that session drive.
CREATE TABLE IF NOT EXISTS sessions (
    session_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id     INTEGER NOT NULL REFERENCES drivers(driver_id),
    started_at    TEXT NOT NULL,
    ended_at      TEXT,
    source_csv    TEXT,
    notes         TEXT,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_sessions_driver_id ON sessions(driver_id);
