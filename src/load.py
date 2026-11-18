"""Load module: create SQLite star schema and incrementally load data."""

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.utils import DB_PATH, setup_logger

logger = setup_logger("load")

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS dim_city (
    city_id   INTEGER PRIMARY KEY,
    city_name TEXT    NOT NULL,
    state     TEXT    NOT NULL,
    region    TEXT    NOT NULL,
    latitude  REAL    NOT NULL,
    longitude REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     TEXT    PRIMARY KEY,
    date        TEXT    NOT NULL,
    day         INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    day_of_week TEXT    NOT NULL,
    is_weekend  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_readings (
    reading_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id       INTEGER NOT NULL,
    date_id       TEXT    NOT NULL,
    timestamp     TEXT    NOT NULL,
    hour          INTEGER NOT NULL,
    pm10          REAL,
    pm2_5         REAL,
    co            REAL,
    no2           REAL,
    so2           REAL,
    ozone         REAL,
    us_aqi        REAL    NOT NULL,
    aqi_category  TEXT    NOT NULL,
    temperature   REAL,
    humidity      REAL,
    wind_speed    REAL,
    precipitation REAL,
    FOREIGN KEY (city_id) REFERENCES dim_city (city_id),
    FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
    UNIQUE (city_id, timestamp)
);

CREATE TABLE IF NOT EXISTS rejected_rows (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id          INTEGER,
    timestamp        TEXT,
    rejection_reason TEXT,
    raw_data         TEXT,
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    run_time      TEXT    NOT NULL,
    rows_extracted INTEGER NOT NULL,
    rows_loaded   INTEGER NOT NULL,
    rows_rejected INTEGER NOT NULL,
    status        TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_city_id ON fact_readings (city_id);
CREATE INDEX IF NOT EXISTS idx_fact_date_id ON fact_readings (date_id);
"""


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def get_db_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the SQLite database and enable foreign keys.

    Args:
        db_path: Path to the database file.

    Returns:
        A ``sqlite3.Connection``.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def create_tables(conn: sqlite3.Connection) -> None:
    """Create all star-schema tables if they don't already exist.

    Args:
        conn: SQLite connection.
    """
    conn.executescript(CREATE_TABLES_SQL)
    conn.commit()
    logger.info("Database tables created / verified")


# ---------------------------------------------------------------------------
# Dimension loaders
# ---------------------------------------------------------------------------

def load_dim_city(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Upsert city dimension rows (INSERT OR REPLACE).

    Args:
        conn: SQLite connection.
        df: ``dim_city`` DataFrame.

    Returns:
        Number of rows processed.
    """
    rows = df.to_dict("records")
    conn.executemany(
        """INSERT OR REPLACE INTO dim_city
           (city_id, city_name, state, region, latitude, longitude)
           VALUES (:city_id, :city_name, :state, :region, :latitude, :longitude)""",
        rows,
    )
    conn.commit()
    logger.info(f"Loaded {len(rows)} rows into dim_city")
    return len(rows)


def load_dim_date(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Insert new date dimension rows (INSERT OR IGNORE for idempotency).

    Args:
        conn: SQLite connection.
        df: ``dim_date`` DataFrame.

    Returns:
        Number of rows processed.
    """
    rows = df.to_dict("records")
    conn.executemany(
        """INSERT OR IGNORE INTO dim_date
           (date_id, date, day, month, year, day_of_week, is_weekend)
           VALUES (:date_id, :date, :day, :month, :year, :day_of_week, :is_weekend)""",
        rows,
    )
    conn.commit()
    logger.info(f"Loaded date dimension ({len(rows)} dates processed)")
    return len(rows)


# ---------------------------------------------------------------------------
# Fact loader
# ---------------------------------------------------------------------------

def load_fact_readings(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Load fact readings using INSERT OR IGNORE for idempotency.

    The ``UNIQUE (city_id, timestamp)`` constraint silently skips
    duplicates, making re-runs safe.

    Args:
        conn: SQLite connection.
        df: ``fact_readings`` DataFrame.

    Returns:
        Number of **new** rows actually inserted.
    """
    before_count: int = conn.execute(
        "SELECT COUNT(*) FROM fact_readings"
    ).fetchone()[0]

    rows = df.to_dict("records")
    conn.executemany(
        """INSERT OR IGNORE INTO fact_readings
           (city_id, date_id, timestamp, hour, pm10, pm2_5, co, no2, so2, ozone,
            us_aqi, aqi_category, temperature, humidity, wind_speed, precipitation)
           VALUES (:city_id, :date_id, :timestamp, :hour, :pm10, :pm2_5, :co, :no2,
                   :so2, :ozone, :us_aqi, :aqi_category, :temperature, :humidity,
                   :wind_speed, :precipitation)""",
        rows,
    )
    conn.commit()

    after_count: int = conn.execute(
        "SELECT COUNT(*) FROM fact_readings"
    ).fetchone()[0]
    new_rows = after_count - before_count

    logger.info(
        f"Fact load: {new_rows} new rows inserted "
        f"({len(rows)} attempted, {len(rows) - new_rows} duplicates skipped)"
    )
    return new_rows


# ---------------------------------------------------------------------------
# Rejected rows
# ---------------------------------------------------------------------------

def load_rejected_rows(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Write rejected (quarantined) rows to the ``rejected_rows`` table.

    Args:
        conn: SQLite connection.
        df: DataFrame of rejected rows with a ``rejection_reason`` column.

    Returns:
        Number of rows inserted.
    """
    if df.empty:
        return 0

    for _, row in df.iterrows():
        conn.execute(
            """INSERT INTO rejected_rows
               (city_id, timestamp, rejection_reason, raw_data)
               VALUES (?, ?, ?, ?)""",
            (
                int(row.get("city_id", 0)),
                str(row.get("timestamp", "")),
                str(row.get("rejection_reason", "")),
                str(row.to_dict()),
            ),
        )
    conn.commit()
    logger.info(f"Loaded {len(df)} rows into rejected_rows quarantine")
    return len(df)


# ---------------------------------------------------------------------------
# Run log
# ---------------------------------------------------------------------------

def log_etl_run(
    conn: sqlite3.Connection,
    rows_extracted: int,
    rows_loaded: int,
    rows_rejected: int,
    status: str,
) -> None:
    """Write one summary row to ``etl_run_log``.

    Args:
        conn: SQLite connection.
        rows_extracted: Total rows from transform output.
        rows_loaded: New rows inserted into ``fact_readings``.
        rows_rejected: Rows quarantined.
        status: ``'SUCCESS'`` or ``'FAILURE: <reason>'``.
    """
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """INSERT INTO etl_run_log
           (run_time, rows_extracted, rows_loaded, rows_rejected, status)
           VALUES (?, ?, ?, ?, ?)""",
        (run_time, rows_extracted, rows_loaded, rows_rejected, status),
    )
    conn.commit()
    logger.info(
        f"ETL run logged: {rows_extracted} extracted, "
        f"{rows_loaded} loaded, {rows_rejected} rejected — {status}"
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def load_all(
    fact_df: pd.DataFrame,
    dim_city_df: pd.DataFrame,
    dim_date_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    db_path: Path = DB_PATH,
) -> dict[str, int]:
    """Run the full load pipeline into SQLite.

    Args:
        fact_df: Fact readings DataFrame.
        dim_city_df: City dimension DataFrame.
        dim_date_df: Date dimension DataFrame.
        rejected_df: Rejected rows DataFrame.
        db_path: Database file path.

    Returns:
        ``{"rows_extracted": …, "rows_loaded": …, "rows_rejected": …}``
    """
    conn = get_db_connection(db_path)
    try:
        create_tables(conn)
        load_dim_city(conn, dim_city_df)
        load_dim_date(conn, dim_date_df)
        rows_loaded = load_fact_readings(conn, fact_df)
        rows_rejected = load_rejected_rows(conn, rejected_df)

        log_etl_run(conn, len(fact_df), rows_loaded, rows_rejected, "SUCCESS")

        return {
            "rows_extracted": len(fact_df),
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
        }
    except Exception as e:
        log_etl_run(conn, len(fact_df), 0, 0, f"FAILURE: {e}")
        raise
    finally:
        conn.close()
