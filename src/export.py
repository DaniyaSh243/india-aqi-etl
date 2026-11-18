"""Export module: dump star-schema tables to CSV for Power BI consumption."""

import sqlite3
from pathlib import Path

import pandas as pd

from src.utils import DB_PATH, DATA_PROCESSED_DIR, setup_logger

logger = setup_logger("export")


def export_table_to_csv(
    conn: sqlite3.Connection,
    table_name: str,
    output_dir: Path,
) -> str:
    """Export one SQLite table to a CSV file.

    Args:
        conn: SQLite connection.
        table_name: Name of the table to export.
        output_dir: Target directory for the CSV.

    Returns:
        Absolute path of the written CSV file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)  # noqa: S608
    filepath = output_dir / f"{table_name}.csv"
    df.to_csv(filepath, index=False)
    logger.info(f"Exported {table_name}: {len(df)} rows → {filepath}")
    return str(filepath)


def export_all(
    db_path: Path = DB_PATH,
    output_dir: Path = DATA_PROCESSED_DIR,
) -> list[str]:
    """Export dim_city, dim_date, and fact_readings to CSV.

    Args:
        db_path: Path to the SQLite database.
        output_dir: Directory to write CSV files into.

    Returns:
        List of exported file paths.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        tables = ["dim_city", "dim_date", "fact_readings"]
        paths: list[str] = []
        for table in tables:
            path = export_table_to_csv(conn, table, output_dir)
            paths.append(path)
        logger.info(f"Export complete: {len(tables)} tables → {output_dir}")
        return paths
    finally:
        conn.close()
