"""Utility functions: logging setup, configuration loader, and constants."""

import json
import logging
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG_DIR = PROJECT_ROOT / "config"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DB_DIR = PROJECT_ROOT / "db"
LOG_DIR = PROJECT_ROOT / "logs"
DB_PATH = DB_DIR / "aqi.db"

# ---------------------------------------------------------------------------
# API configuration
# ---------------------------------------------------------------------------
AIR_QUALITY_API = "https://air-quality-api.open-meteo.com/v1/air-quality"
WEATHER_API = "https://api.open-meteo.com/v1/forecast"

AQ_PARAMS = [
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "sulphur_dioxide", "ozone", "us_aqi",
]
WEATHER_PARAMS = [
    "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation",
]
TIMEZONE = "Asia/Kolkata"
PAST_DAYS = 7

# ---------------------------------------------------------------------------
# US EPA AQI category thresholds
# ---------------------------------------------------------------------------
AQI_CATEGORIES: list[tuple[int, float, str]] = [
    (0, 50, "Good"),
    (51, 100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, float("inf"), "Hazardous"),
]

# Column rename mapping: API names → schema names
COLUMN_RENAME: dict[str, str] = {
    "carbon_monoxide": "co",
    "nitrogen_dioxide": "no2",
    "sulphur_dioxide": "so2",
    "temperature_2m": "temperature",
    "relative_humidity_2m": "humidity",
    "wind_speed_10m": "wind_speed",
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def setup_logger(name: str = "etl") -> logging.Logger:
    """Set up a logger that writes to both console and logs/etl.log.

    Args:
        name: Logger name.

    Returns:
        Configured logger instance.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (INFO and above)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler (DEBUG and above)
    file_handler = logging.FileHandler(LOG_DIR / "etl.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def load_cities_config() -> list[dict[str, Any]]:
    """Load city configuration from config/cities.json.

    Returns:
        List of city dictionaries with keys:
        city_id, name, state, region, latitude, longitude.
    """
    config_path = CONFIG_DIR / "cities.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config["cities"]


def get_aqi_category(aqi_value: float) -> str:
    """Classify a US AQI value into official US EPA categories.

    Args:
        aqi_value: The US AQI value.

    Returns:
        Category string: Good (0-50), Moderate (51-100),
        Unhealthy for Sensitive Groups (101-150), Unhealthy (151-200),
        Very Unhealthy (201-300), or Hazardous (301+).
    """
    if aqi_value is None or aqi_value < 0:
        return "Unknown"
    if aqi_value <= 50:
        return "Good"
    elif aqi_value <= 100:
        return "Moderate"
    elif aqi_value <= 150:
        return "Unhealthy for Sensitive Groups"
    elif aqi_value <= 200:
        return "Unhealthy"
    elif aqi_value <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


def get_time_of_day(hour: int) -> str:
    """Classify an hour (0-23) into a time-of-day bucket.

    Args:
        hour: Hour of the day.

    Returns:
        One of Morning (6-11), Afternoon (12-16), Evening (17-20), Night.
    """
    if 6 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    else:
        return "Night"
