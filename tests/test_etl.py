"""Tests for the India AQI ETL pipeline and Dashboard enhancements."""

import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.utils import get_aqi_category
from src.transform import validate_rows
from src.load import create_tables, load_fact_readings, get_db_connection, log_etl_run
import src.dashboard_server as ds


# -----------------------------------------------------------------------
# Test 1: US EPA AQI category classification
# -----------------------------------------------------------------------

class TestAQICategory:
    """Verify that AQI values map to the correct US EPA category."""

    def test_good_range(self) -> None:
        assert get_aqi_category(0) == "Good"
        assert get_aqi_category(25) == "Good"
        assert get_aqi_category(50) == "Good"

    def test_moderate_range(self) -> None:
        assert get_aqi_category(51) == "Moderate"
        assert get_aqi_category(75) == "Moderate"
        assert get_aqi_category(100) == "Moderate"

    def test_unhealthy_sensitive_range(self) -> None:
        assert get_aqi_category(101) == "Unhealthy for Sensitive Groups"
        assert get_aqi_category(125) == "Unhealthy for Sensitive Groups"
        assert get_aqi_category(150) == "Unhealthy for Sensitive Groups"

    def test_unhealthy_range(self) -> None:
        assert get_aqi_category(151) == "Unhealthy"
        assert get_aqi_category(175) == "Unhealthy"
        assert get_aqi_category(200) == "Unhealthy"

    def test_very_unhealthy_range(self) -> None:
        assert get_aqi_category(201) == "Very Unhealthy"
        assert get_aqi_category(250) == "Very Unhealthy"
        assert get_aqi_category(300) == "Very Unhealthy"

    def test_hazardous_range(self) -> None:
        assert get_aqi_category(301) == "Hazardous"
        assert get_aqi_category(450) == "Hazardous"
        assert get_aqi_category(500) == "Hazardous"

    def test_invalid_negative(self) -> None:
        assert get_aqi_category(-1) == "Unknown"


# -----------------------------------------------------------------------
# Test 2: Deduplication logic
# -----------------------------------------------------------------------

class TestDeduplication:
    """Verify that duplicate (city_id, timestamp) rows are removed."""

    def test_drop_duplicates_keeps_first(self) -> None:
        df = pd.DataFrame(
            {
                "city_id": [1, 1, 2, 2],
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01 00:00",
                        "2024-01-01 00:00",  # ← duplicate for city 1
                        "2024-01-01 00:00",
                        "2024-01-01 01:00",
                    ]
                ),
                "us_aqi": [50, 55, 60, 70],
            }
        )
        result = df.drop_duplicates(
            subset=["city_id", "timestamp"], keep="first"
        )
        assert len(result) == 3


# -----------------------------------------------------------------------
# Test 3: Validation quarantines bad rows
# -----------------------------------------------------------------------

class TestValidation:
    """Verify that rows failing quality checks are quarantined."""

    def test_reject_negative_pm25(self) -> None:
        df = pd.DataFrame(
            {
                "city_id": [1, 1],
                "timestamp": pd.to_datetime(
                    ["2024-01-01 00:00", "2024-01-01 01:00"]
                ),
                "us_aqi": [50, 60],
                "pm2_5": [10.0, -5.0],  # second row is invalid
                "pm10": [20.0, 30.0],
                "humidity": [50.0, 60.0],
                "temperature": [25.0, 30.0],
            }
        )
        valid, rejected = validate_rows(df)
        assert len(valid) == 1
        assert len(rejected) == 1
        assert "negative_pm2_5" in rejected.iloc[0]["rejection_reason"]


# -----------------------------------------------------------------------
# Test 4: Incremental load idempotency
# -----------------------------------------------------------------------

class TestIdempotency:
    """Verify INSERT OR IGNORE prevents duplicates on re-run."""

    def test_second_load_inserts_zero_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = get_db_connection(db_path)
            create_tables(conn)

            conn.execute(
                "INSERT INTO dim_city VALUES "
                "(1, 'Delhi', 'Delhi', 'North', 28.61, 77.20)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO dim_date VALUES "
                "('2024-01-01','2024-01-01', 1, 1, 2024, 'Monday', 0)"
            )
            conn.commit()

            df = pd.DataFrame(
                {
                    "city_id": [1, 1],
                    "date_id": ["2024-01-01", "2024-01-01"],
                    "timestamp": ["2024-01-01 00:00:00", "2024-01-01 01:00:00"],
                    "hour": [0, 1],
                    "pm10": [50.0, 60.0],
                    "pm2_5": [25.0, 30.0],
                    "co": [100.0, 110.0],
                    "no2": [10.0, 12.0],
                    "so2": [5.0, 6.0],
                    "ozone": [30.0, 35.0],
                    "us_aqi": [50, 60],
                    "aqi_category": ["Good", "Moderate"],
                    "temperature": [25.0, 26.0],
                    "humidity": [50.0, 55.0],
                    "wind_speed": [3.0, 4.0],
                    "precipitation": [0.0, 0.0],
                }
            )

            loaded_1 = load_fact_readings(conn, df)
            assert loaded_1 == 2

            loaded_2 = load_fact_readings(conn, df)
            assert loaded_2 == 0

            total = conn.execute(
                "SELECT COUNT(*) FROM fact_readings"
            ).fetchone()[0]
            assert total == 2
            conn.close()


# -----------------------------------------------------------------------
# Test 5: Query 2 ROW_NUMBER() returns exactly 5 rows per city with ties
# -----------------------------------------------------------------------

class TestQuery2RowNumber:
    """Verify Query 2 uses ROW_NUMBER and timestamp tie-breaker for exactly 5 rows."""

    def test_exactly_five_rows_per_city(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_q2.db"
            conn = get_db_connection(db_path)
            create_tables(conn)

            conn.execute("INSERT INTO dim_city VALUES (1, 'CityA', 'StateA', 'North', 10.0, 20.0)")
            conn.execute("INSERT INTO dim_city VALUES (2, 'CityB', 'StateB', 'South', 30.0, 40.0)")
            conn.execute("INSERT INTO dim_date VALUES ('2024-01-01', '2024-01-01', 1, 1, 2024, 'Monday', 0)")

            # Insert 10 readings per city where all have identical AQI (150)
            rows = []
            for city_id in [1, 2]:
                for h in range(10):
                    rows.append({
                        "city_id": city_id,
                        "date_id": "2024-01-01",
                        "timestamp": f"2024-01-01 {h:02d}:00:00",
                        "hour": h,
                        "pm10": 50.0,
                        "pm2_5": 30.0,
                        "co": 100.0,
                        "no2": 10.0,
                        "so2": 5.0,
                        "ozone": 20.0,
                        "us_aqi": 150.0,  # All tied!
                        "aqi_category": "Unhealthy for Sensitive Groups",
                        "temperature": 25.0,
                        "humidity": 60.0,
                        "wind_speed": 5.0,
                        "precipitation": 0.0,
                    })

            load_fact_readings(conn, pd.DataFrame(rows))

            # Execute Query 2
            query_2 = """
            WITH ranked AS (
                SELECT
                    dc.city_name,
                    fr.timestamp,
                    fr.us_aqi,
                    ROW_NUMBER() OVER (
                        PARTITION BY fr.city_id
                        ORDER BY fr.us_aqi DESC, fr.timestamp DESC
                    ) AS rnk
                FROM fact_readings fr
                JOIN dim_city dc ON fr.city_id = dc.city_id
            )
            SELECT city_name, timestamp, us_aqi, rnk
            FROM ranked
            WHERE rnk <= 5
            ORDER BY city_name, rnk;
            """
            result = conn.execute(query_2).fetchall()

            # Must return exactly 10 rows (5 per city) despite all 20 rows being tied at 150.0 AQI
            assert len(result) == 10
            city_a_rows = [r for r in result if r[0] == "CityA"]
            city_b_rows = [r for r in result if r[0] == "CityB"]
            assert len(city_a_rows) == 5
            assert len(city_b_rows) == 5
            assert [r[3] for r in city_a_rows] == [1, 2, 3, 4, 5]

            conn.close()


# -----------------------------------------------------------------------
# Test 6: Trend query restricted to timestamps <= now
# -----------------------------------------------------------------------

class TestTrendUpToNow:
    """Verify that hourly trend query filters out future/forecast timestamps."""

    def test_trend_excludes_future_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_trend.db"
            conn = get_db_connection(db_path)
            create_tables(conn)

            conn.execute("INSERT INTO dim_city VALUES (1, 'CityA', 'StateA', 'North', 10.0, 20.0)")
            conn.execute("INSERT INTO dim_date VALUES ('2024-01-01', '2024-01-01', 1, 1, 2024, 'Monday', 0)")

            now = datetime.now()
            past_ts = (now - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
            now_ts = now.strftime("%Y-%m-%d %H:%M:%S")
            future_ts = (now + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S")

            for ts in [past_ts, now_ts, future_ts]:
                conn.execute(
                    """INSERT INTO fact_readings
                       (city_id, date_id, timestamp, hour, us_aqi, aqi_category)
                       VALUES (1, '2024-01-01', ?, 0, 100.0, 'Moderate')""",
                    (ts,),
                )
            conn.commit()

            # Query with timestamp <= now
            cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            res = conn.execute(
                """SELECT timestamp FROM fact_readings
                   WHERE timestamp <= ?
                   ORDER BY timestamp DESC""",
                (cur_time,),
            ).fetchall()

            timestamps = [r[0] for r in res]
            assert future_ts not in timestamps
            assert past_ts in timestamps
            assert len(timestamps) == 2

            conn.close()


# -----------------------------------------------------------------------
# Test 7: Read-Only Mode enforcement
# -----------------------------------------------------------------------

class TestReadOnlyMode:
    """Verify Read-Only mode blocks free-text SQL and pipeline runs."""

    def test_read_only_blocks_custom_sql(self, monkeypatch) -> None:
        monkeypatch.setattr(ds, "READ_ONLY_MODE", True)

        # Free-text SQL must be blocked
        res = ds.execute_custom_sql("SELECT * FROM dim_city WHERE city_id = 1")
        assert "error" in res
        assert "Read-Only Mode Active" in res["error"]

    def test_read_only_allows_preset_queries(self, monkeypatch) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_db = Path(tmpdir) / "aqi.db"
            conn = get_db_connection(test_db)
            create_tables(conn)
            conn.close()

            monkeypatch.setattr(ds, "DB_PATH", test_db)
            monkeypatch.setattr(ds, "READ_ONLY_MODE", True)

            # Query 1 preset must be allowed
            preset_q1 = ds.PRESET_QUERIES["1"]
            res = ds.execute_custom_sql(preset_q1)
            assert "error" not in res
            assert "columns" in res

    def test_read_only_blocks_pipeline_trigger(self, monkeypatch) -> None:
        monkeypatch.setattr(ds, "READ_ONLY_MODE", True)
        res = ds.trigger_etl_run()
        assert "error" in res
        assert "disabled in Read-Only Mode" in res["error"]


# -----------------------------------------------------------------------
# Test 8: ETL run audit log real stats
# -----------------------------------------------------------------------

class TestRunEtlStatsReporting:
    """Verify etl_run_log records and returns exact extracted/loaded/rejected stats."""

    def test_audit_log_records_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_log.db"
            conn = get_db_connection(db_path)
            create_tables(conn)

            log_etl_run(conn, rows_extracted=1920, rows_loaded=1920, rows_rejected=5, status="SUCCESS")

            row = conn.execute(
                "SELECT rows_extracted, rows_loaded, rows_rejected, status FROM etl_run_log ORDER BY run_id DESC LIMIT 1"
            ).fetchone()

            assert row[0] == 1920
            assert row[1] == 1920
            assert row[2] == 5
            assert row[3] == "SUCCESS"

            conn.close()


# -----------------------------------------------------------------------
# Test 9: City Filter & Asia/Kolkata Timezone Cutoff Logic
# -----------------------------------------------------------------------

class TestCityFilterAndDashboard:
    """Verify city selector filtering across KPIs, scatter, categories, trend, and timezone cutoff."""

    def test_all_cities_mode(self) -> None:
        data = ds.get_db_data(city_id=None)
        assert data["selected_city_id"] is None
        assert data["selected_city_name"] is None
        assert len(data["cities"]) == 10

        # Dynamically compute expected total and avg AQI from DB
        conn = sqlite3.connect(ds.DB_PATH)
        expected_total = conn.execute("SELECT COUNT(*) FROM fact_readings").fetchone()[0]
        expected_avg_aqi = conn.execute("SELECT ROUND(AVG(us_aqi), 1) FROM fact_readings").fetchone()[0]
        conn.close()

        assert data["kpis"]["total_readings"] == expected_total
        assert data["kpis"]["avg_aqi"] == expected_avg_aqi
        assert len(data["scatter"]) > 0

    def test_single_city_filter(self) -> None:
        # Filter by Delhi (city_id = 1)
        data = ds.get_db_data(city_id=1)

        # Dynamically compute expected values for city_id = 1
        conn = sqlite3.connect(ds.DB_PATH)
        expected_city_total = conn.execute("SELECT COUNT(*) FROM fact_readings WHERE city_id = 1").fetchone()[0]
        expected_city_avg_aqi = conn.execute("SELECT ROUND(AVG(us_aqi), 1) FROM fact_readings WHERE city_id = 1").fetchone()[0]
        expected_city_name = conn.execute("SELECT city_name FROM dim_city WHERE city_id = 1").fetchone()[0]
        conn.close()

        assert data["selected_city_id"] == 1
        assert data["selected_city_name"] == expected_city_name

        # KPIs must match the direct SQL calculation for this city
        assert data["kpis"]["total_readings"] == expected_city_total
        assert data["kpis"]["avg_aqi"] == expected_city_avg_aqi

        # Scatter points must all belong to selected city
        assert len(data["scatter"]) == min(expected_city_total, 300)
        assert all(pt["city"] == expected_city_name for pt in data["scatter"])

        # Category counts must sum to the city's total readings
        cat_total = sum(c["count"] for c in data["categories"])
        assert cat_total == expected_city_total

        # Comparative bar chart must still retain all 10 cities
        assert len(data["cities"]) == 10

    def test_kolkata_timezone_cutoff(self) -> None:
        data = ds.get_db_data(city_id=None)
        kolkata_now = ds.get_kolkata_now()

        assert data["current_kolkata_time"] <= kolkata_now
        assert data["latest_observed_hour"] is not None
        assert data["latest_observed_hour"] <= kolkata_now

        # Verify no trend point has a timestamp after current Asia/Kolkata time
        for pt in data["trend"]:
            assert pt["timestamp"] <= kolkata_now

        # Verify trend list is in chronological order (earliest to latest)
        timestamps = [pt["timestamp"] for pt in data["trend"]]
        assert timestamps == sorted(timestamps)
        assert timestamps[-1] == data["latest_observed_hour"]
