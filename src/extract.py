"""Extract module: fetch air quality and weather data from Open-Meteo APIs."""

import json
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils import (
    AIR_QUALITY_API,
    WEATHER_API,
    AQ_PARAMS,
    WEATHER_PARAMS,
    TIMEZONE,
    PAST_DAYS,
    DATA_RAW_DIR,
    setup_logger,
)

logger = setup_logger("extract")


def create_session() -> requests.Session:
    """Create an HTTP session with automatic retries and exponential backoff.

    Returns:
        Configured requests Session.
    """
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_air_quality(
    session: requests.Session,
    latitude: float,
    longitude: float,
    past_days: int = PAST_DAYS,
) -> dict[str, Any]:
    """Fetch hourly air quality data for a single location.

    Args:
        session: Requests session with retry configuration.
        latitude: Location latitude.
        longitude: Location longitude.
        past_days: Number of past days to retrieve.

    Returns:
        Raw API response as a dictionary.

    Raises:
        requests.RequestException: If the request fails after retries.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(AQ_PARAMS),
        "past_days": past_days,
        "timezone": TIMEZONE,
    }
    response = session.get(AIR_QUALITY_API, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_weather(
    session: requests.Session,
    latitude: float,
    longitude: float,
    past_days: int = PAST_DAYS,
) -> dict[str, Any]:
    """Fetch hourly weather forecast/history data for a single location.

    Args:
        session: Requests session with retry configuration.
        latitude: Location latitude.
        longitude: Location longitude.
        past_days: Number of past days to retrieve.

    Returns:
        Raw API response as a dictionary.

    Raises:
        requests.RequestException: If the request fails after retries.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(WEATHER_PARAMS),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": TIMEZONE,
    }
    response = session.get(WEATHER_API, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw_data(city_name: str, data: dict[str, Any], data_type: str) -> str:
    """Persist a raw API response as a timestamped JSON file.

    Args:
        city_name: Name of the city.
        data: Raw API response dictionary.
        data_type: Either 'air_quality' or 'weather'.

    Returns:
        Absolute path of the saved file.
    """
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{city_name.lower()}_{data_type}_{timestamp}.json"
    filepath = DATA_RAW_DIR / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return str(filepath)


def extract_all(cities: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Extract air quality and weather data for every configured city.

    One city failing does **not** crash the whole run; it is logged and
    skipped so the remaining cities can still be processed.

    Args:
        cities: List of city configuration dictionaries.

    Returns:
        ``{city_name: {"air_quality": …, "weather": …, "city_info": …}}``
    """
    session = create_session()
    raw_data: dict[str, dict[str, Any]] = {}

    for city in cities:
        city_name = city["name"]
        logger.info(f"Extracting data for {city_name}…")

        try:
            aq_data = fetch_air_quality(session, city["latitude"], city["longitude"])
            weather_data = fetch_weather(session, city["latitude"], city["longitude"])

            # Persist raw responses for reproducibility
            save_raw_data(city_name, aq_data, "air_quality")
            save_raw_data(city_name, weather_data, "weather")

            raw_data[city_name] = {
                "air_quality": aq_data,
                "weather": weather_data,
                "city_info": city,
            }

            aq_rows = len(aq_data.get("hourly", {}).get("time", []))
            wx_rows = len(weather_data.get("hourly", {}).get("time", []))
            logger.info(f"  ✓ {city_name}: {aq_rows} AQ rows, {wx_rows} weather rows")

        except requests.RequestException as e:
            logger.error(f"  ✗ Failed to extract data for {city_name}: {e}")
            continue

    logger.info(
        f"Extraction complete: {len(raw_data)}/{len(cities)} cities successful"
    )
    return raw_data
