"""Transform module: clean, validate, and model raw data into star-schema DataFrames."""

from typing import Any

import pandas as pd

from src.utils import (
    COLUMN_RENAME,
    get_aqi_category,
    get_time_of_day,
    setup_logger,
)

logger = setup_logger("transform")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def flatten_hourly_data(
    hourly_data: dict[str, list],
    data_type: str,
) -> pd.DataFrame:
    """Flatten an hourly API response dict into a tidy DataFrame.

    Args:
        hourly_data: The ``hourly`` dict from an Open-Meteo response.
        data_type: ``'air_quality'`` or ``'weather'`` (for log messages).

    Returns:
        DataFrame with one row per hour and a ``timestamp`` datetime column.
    """
    df = pd.DataFrame(hourly_data)
    df.rename(columns={"time": "timestamp"}, inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def merge_aq_weather(
    aq_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    """Inner-merge air-quality and weather DataFrames on timestamp.

    Args:
        aq_df: Air-quality DataFrame.
        weather_df: Weather DataFrame.

    Returns:
        Merged DataFrame.
    """
    return pd.merge(aq_df, weather_df, on="timestamp", how="inner")


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add analytical columns derived from existing data.

    New columns: ``aqi_category``, ``date``, ``hour``, ``day_of_week``,
    ``is_weekend``, ``time_of_day``.

    Args:
        df: DataFrame with ``us_aqi`` and ``timestamp`` columns.

    Returns:
        DataFrame with the new columns appended.
    """
    df = df.copy()
    df["aqi_category"] = df["us_aqi"].apply(get_aqi_category)
    df["date"] = df["timestamp"].dt.date.astype(str)
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.day_name()
    df["is_weekend"] = df["timestamp"].dt.dayofweek.isin([5, 6]).astype(int)
    df["time_of_day"] = df["hour"].apply(get_time_of_day)
    return df


def validate_rows(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate data-quality rules and quarantine failing rows.

    Rules checked:
    * ``humidity`` must be in [0, 100].
    * ``pm2_5`` and ``pm10`` must be ≥ 0.
    * ``temperature`` must be in [-60, 60].

    Args:
        df: DataFrame to validate.

    Returns:
        ``(valid_df, rejected_df)`` — rejected rows carry a
        ``rejection_reason`` column.
    """
    invalid_mask = pd.Series(False, index=df.index)
    reasons = pd.Series("", index=df.index)

    # Humidity 0–100
    if "humidity" in df.columns:
        bad = df["humidity"].notna() & (
            (df["humidity"] < 0) | (df["humidity"] > 100)
        )
        invalid_mask |= bad
        reasons = reasons.where(~bad, reasons + "humidity_out_of_range;")

    # PM2.5 ≥ 0
    if "pm2_5" in df.columns:
        bad = df["pm2_5"].notna() & (df["pm2_5"] < 0)
        invalid_mask |= bad
        reasons = reasons.where(~bad, reasons + "negative_pm2_5;")

    # PM10 ≥ 0
    if "pm10" in df.columns:
        bad = df["pm10"].notna() & (df["pm10"] < 0)
        invalid_mask |= bad
        reasons = reasons.where(~bad, reasons + "negative_pm10;")

    # Temperature -60 to 60
    if "temperature" in df.columns:
        bad = df["temperature"].notna() & (
            (df["temperature"] < -60) | (df["temperature"] > 60)
        )
        invalid_mask |= bad
        reasons = reasons.where(~bad, reasons + "temperature_out_of_range;")

    rejected = df[invalid_mask].copy()
    if not rejected.empty:
        rejected["rejection_reason"] = reasons[invalid_mask]
    else:
        rejected["rejection_reason"] = pd.Series(dtype="str")

    valid = df[~invalid_mask].copy()

    if len(rejected) > 0:
        logger.warning(f"Quarantined {len(rejected)} rows failing validation")

    return valid, rejected


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def build_dim_city(cities: list[dict[str, Any]]) -> pd.DataFrame:
    """Build the ``dim_city`` dimension table from the config list.

    Args:
        cities: City configuration dicts.

    Returns:
        ``dim_city`` DataFrame.
    """
    df = pd.DataFrame(cities)
    df = df.rename(columns={"name": "city_name"})
    return df[["city_id", "city_name", "state", "region", "latitude", "longitude"]]


def build_dim_date(dates: pd.Series) -> pd.DataFrame:
    """Build the ``dim_date`` dimension table from unique date strings.

    Args:
        dates: Series of date strings (``YYYY-MM-DD``).

    Returns:
        ``dim_date`` DataFrame.
    """
    unique_dates = pd.to_datetime(dates.unique())
    unique_dates = unique_dates.sort_values()
    return pd.DataFrame(
        {
            "date_id": unique_dates.strftime("%Y-%m-%d"),
            "date": unique_dates.strftime("%Y-%m-%d"),
            "day": unique_dates.day,
            "month": unique_dates.month,
            "year": unique_dates.year,
            "day_of_week": unique_dates.day_name(),
            "is_weekend": unique_dates.dayofweek.isin([5, 6]).astype(int),
        }
    )


# ---------------------------------------------------------------------------
# Main transform orchestrator
# ---------------------------------------------------------------------------

def transform_all(
    raw_data: dict[str, dict[str, Any]],
    cities: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run the full transform pipeline on extracted data.

    Steps: flatten → merge → clean nulls → deduplicate → derive columns →
    validate → build dimensions.

    Args:
        raw_data: Output of ``extract_all()``.
        cities: City config list.

    Returns:
        ``(fact_readings, dim_city, dim_date, rejected_rows)``
    """
    all_frames: list[pd.DataFrame] = []
    total_dropped_aqi_null = 0
    total_ffilled = 0

    for city_name, data in raw_data.items():
        city_info = data["city_info"]

        # 1. Flatten hourly arrays into DataFrames
        aq_df = flatten_hourly_data(data["air_quality"]["hourly"], "air_quality")
        weather_df = flatten_hourly_data(data["weather"]["hourly"], "weather")

        # 2. Merge air quality + weather on timestamp
        merged = merge_aq_weather(aq_df, weather_df)
        merged["city_id"] = city_info["city_id"]

        # 3. Rename API columns → schema names
        merged.rename(columns=COLUMN_RENAME, inplace=True)

        # 4. Drop rows where us_aqi is null
        before = len(merged)
        merged = merged.dropna(subset=["us_aqi"])
        dropped = before - len(merged)
        total_dropped_aqi_null += dropped
        if dropped:
            logger.info(f"  {city_name}: dropped {dropped} rows with null us_aqi")

        # 5. Forward-fill weather gaps (limit 2 hours)
        weather_cols = ["temperature", "humidity", "wind_speed", "precipitation"]
        present = [c for c in weather_cols if c in merged.columns]
        if present:
            before_nulls = int(merged[present].isnull().sum().sum())
            merged[present] = merged[present].ffill(limit=2)
            after_nulls = int(merged[present].isnull().sum().sum())
            filled = before_nulls - after_nulls
            total_ffilled += filled
            if filled:
                logger.info(
                    f"  {city_name}: forward-filled {filled} weather null values"
                )

        all_frames.append(merged)

    if not all_frames:
        logger.error("No data to transform!")
        empty: pd.DataFrame = pd.DataFrame()
        return empty, build_dim_city(cities), pd.DataFrame(), empty

    # 6. Concatenate all cities
    combined = pd.concat(all_frames, ignore_index=True)

    # 7. Remove duplicates on (city_id, timestamp)
    before_dedup = len(combined)
    combined = combined.drop_duplicates(
        subset=["city_id", "timestamp"], keep="first"
    )
    deduped = before_dedup - len(combined)
    if deduped:
        logger.info(f"Removed {deduped} duplicate rows")

    # 8. Add derived columns
    combined = add_derived_columns(combined)

    # 9. Validate
    valid, rejected = validate_rows(combined)

    # 10. Build dimensions
    dim_city = build_dim_city(cities)
    dim_date = build_dim_date(valid["date"])

    # 11. Prepare fact table
    valid["date_id"] = valid["date"]
    valid["timestamp"] = valid["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    fact_columns = [
        "city_id", "date_id", "timestamp", "hour",
        "pm10", "pm2_5", "co", "no2", "so2", "ozone", "us_aqi",
        "aqi_category", "temperature", "humidity", "wind_speed", "precipitation",
    ]
    fact = valid[fact_columns]

    logger.info(
        f"Transform complete: {len(fact)} fact rows, "
        f"{len(rejected)} rejected, "
        f"{total_dropped_aqi_null} null-AQI dropped, "
        f"{total_ffilled} values forward-filled"
    )
    return fact, dim_city, dim_date, rejected
