"""India Air Quality & Weather ETL Pipeline — Single Entry Point.

Usage:
    python run_etl.py
"""

import sys
from datetime import datetime

from src.extract import extract_all
from src.transform import transform_all
from src.load import load_all
from src.export import export_all
from src.utils import setup_logger, load_cities_config


def main() -> None:
    """Run the complete ETL pipeline: Extract → Transform → Load → Export."""
    logger = setup_logger("pipeline")
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("🇮🇳  India Air Quality & Weather ETL Pipeline")
    logger.info(f"Run started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    try:
        # ── CONFIG ──────────────────────────────────────────────────
        cities = load_cities_config()
        logger.info(f"Loaded {len(cities)} cities from config")

        # ── EXTRACT ─────────────────────────────────────────────────
        logger.info("")
        logger.info("--- EXTRACT PHASE ---")
        raw_data = extract_all(cities)

        if not raw_data:
            logger.error("No data extracted. Aborting pipeline.")
            sys.exit(1)

        # ── TRANSFORM ───────────────────────────────────────────────
        logger.info("")
        logger.info("--- TRANSFORM PHASE ---")
        fact_df, dim_city_df, dim_date_df, rejected_df = transform_all(
            raw_data, cities
        )

        # ── LOAD ────────────────────────────────────────────────────
        logger.info("")
        logger.info("--- LOAD PHASE ---")
        stats = load_all(fact_df, dim_city_df, dim_date_df, rejected_df)

        # ── EXPORT ──────────────────────────────────────────────────
        logger.info("")
        logger.info("--- EXPORT PHASE ---")
        export_all()

        # ── SUMMARY ─────────────────────────────────────────────────
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("")
        logger.info("=" * 60)
        logger.info("✅  ETL PIPELINE COMPLETE")
        logger.info(f"  Rows extracted:  {stats['rows_extracted']}")
        logger.info(f"  Rows loaded:     {stats['rows_loaded']}")
        logger.info(f"  Rows rejected:   {stats['rows_rejected']}")
        logger.info(f"  Duration:        {elapsed:.1f}s")
        logger.info("=" * 60)

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
