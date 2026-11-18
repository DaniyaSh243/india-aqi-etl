"""Interactive Local Web Dashboard for India AQI & Weather ETL.

Zero external dependencies - runs on Python standard library (http.server, sqlite3, json, zoneinfo).
Serves a responsive UI with Chart.js visualizations, KPI metrics, city filter,
temperature vs PM2.5 scatter plot with legend, and a live SQL query console.
Supports a `--read-only` flag to secure public/demo environments.
"""

import argparse
import http.server
import json
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "aqi.db"
SQL_FILE = PROJECT_ROOT / "sql" / "analysis_queries.sql"
VENV_PYTHON = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"
if not VENV_PYTHON.exists():
    VENV_PYTHON = Path(sys.executable)

PORT = 8050
READ_ONLY_MODE = os.environ.get("READ_ONLY", "").lower() in ("1", "true", "yes")

# Standard 6 prebuilt SQL queries (canonical whitespace)
PRESET_QUERIES = {
    "1": (
        "SELECT dc.city_name, dc.state, ROUND(AVG(fr.us_aqi), 1) AS avg_aqi, "
        "RANK() OVER (ORDER BY AVG(fr.us_aqi) DESC) AS pollution_rank "
        "FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id "
        "GROUP BY dc.city_name, dc.state ORDER BY pollution_rank;"
    ),
    "2": (
        "WITH ranked AS ( "
        "SELECT dc.city_name, fr.timestamp, fr.us_aqi, "
        "ROW_NUMBER() OVER (PARTITION BY fr.city_id ORDER BY fr.us_aqi DESC, fr.timestamp DESC) AS rnk "
        "FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id "
        ") SELECT city_name, timestamp, us_aqi, rnk FROM ranked WHERE rnk <= 5 ORDER BY city_name, rnk;"
    ),
    "3": (
        "SELECT dc.city_name, fr.timestamp, fr.pm2_5, "
        "ROUND(AVG(fr.pm2_5) OVER (PARTITION BY fr.city_id ORDER BY fr.timestamp ROWS BETWEEN 23 PRECEDING AND CURRENT ROW), 2) AS pm25_24h_avg "
        "FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id "
        "ORDER BY dc.city_name, fr.timestamp LIMIT 100;"
    ),
    "4": (
        "WITH daily AS ( "
        "SELECT dc.city_name, dd.date, ROUND(AVG(fr.us_aqi), 1) AS daily_avg_aqi "
        "FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id "
        "JOIN dim_date dd ON fr.date_id = dd.date_id GROUP BY dc.city_name, dd.date "
        ") SELECT city_name, date, daily_avg_aqi, "
        "LAG(daily_avg_aqi) OVER (PARTITION BY city_name ORDER BY date) AS prev_day_aqi, "
        "ROUND(daily_avg_aqi - LAG(daily_avg_aqi) OVER (PARTITION BY city_name ORDER BY date), 1) AS aqi_change "
        "FROM daily ORDER BY city_name, date;"
    ),
    "5": (
        "SELECT dc.city_name, COUNT(*) AS total_hours, "
        "SUM(CASE WHEN fr.aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) AS unhealthy_or_worse_hours, "
        "ROUND(100.0 * SUM(CASE WHEN fr.aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_unhealthy_or_worse "
        "FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id "
        "GROUP BY dc.city_name ORDER BY pct_unhealthy_or_worse DESC;"
    ),
    "6": (
        "WITH comparison AS ( "
        "SELECT dc.city_name, dd.is_weekend, ROUND(AVG(fr.us_aqi), 1) AS avg_aqi, ROUND(AVG(fr.pm2_5), 1) AS avg_pm25 "
        "FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id "
        "JOIN dim_date dd ON fr.date_id = dd.date_id GROUP BY dc.city_name, dd.is_weekend "
        ") SELECT city_name, CASE is_weekend WHEN 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, avg_aqi, avg_pm25 "
        "FROM comparison ORDER BY city_name, is_weekend;"
    ),
}


def normalize_sql(sql: str) -> str:
    """Normalize SQL by removing comments, extra whitespace, and trailing semicolons."""
    sql = re.sub(r"--.*?\n", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"\s+", " ", sql).strip().rstrip(";")
    return sql.lower()


NORMALIZED_PRESETS = {normalize_sql(q) for q in PRESET_QUERIES.values()}


def get_kolkata_now() -> str:
    """Return the current time formatted as YYYY-MM-DD HH:MM:SS in Asia/Kolkata timezone."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")


def get_db_data(city_id: int | None = None) -> dict:
    """Fetch dashboard metrics, chart series, and logs from SQLite.

    Limits 48-hour trend to timestamps <= Asia/Kolkata current time.
    Filters KPIs, categories, trend, and scatter by city_id when provided.
    Always includes all 10 cities for the comparative bar chart.
    """
    if not DB_PATH.exists():
        return {
            "read_only": READ_ONLY_MODE,
            "selected_city_id": city_id,
            "selected_city_name": None,
            "latest_observed_hour": None,
            "kpis": {},
            "cities": [],
            "categories": [],
            "trend": [],
            "scatter": [],
            "logs": [],
        }

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now_kolkata = get_kolkata_now()

    # City list for selector
    city_rows = cur.execute("SELECT city_id, city_name, state, region FROM dim_city ORDER BY city_name").fetchall()
    city_list = [dict(r) for r in city_rows]

    # Resolve selected city name if city_id given
    selected_city_name = None
    if city_id is not None:
        for c in city_list:
            if c["city_id"] == city_id:
                selected_city_name = c["city_name"]
                break

    # WHERE filter clause for city-specific widgets
    where_fact = "WHERE 1=1"
    params_kpi: list = []
    if city_id is not None:
        where_fact += " AND fr.city_id = ?"
        params_kpi.append(city_id)

    # 1. KPIs (US EPA Standard, filtered by city if selected)
    kpi_sql = f"""
        SELECT
            ROUND(AVG(fr.us_aqi), 1) as avg_aqi,
            ROUND(AVG(fr.pm2_5), 1) as avg_pm25,
            ROUND(MAX(fr.us_aqi), 1) as max_aqi,
            COUNT(*) as total_readings,
            ROUND(100.0 * SUM(CASE WHEN fr.aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_poor
        FROM fact_readings fr
        {where_fact}
    """
    kpi = cur.execute(kpi_sql, params_kpi).fetchone()

    # 2. City Averages (always shows all 10 cities for comparison)
    city_avgs = cur.execute("""
        SELECT
            dc.city_id,
            dc.city_name,
            dc.state,
            ROUND(AVG(fr.us_aqi), 1) as avg_aqi,
            ROUND(AVG(fr.pm2_5), 1) as avg_pm25,
            ROUND(AVG(fr.temperature), 1) as avg_temp
        FROM fact_readings fr
        JOIN dim_city dc ON fr.city_id = dc.city_id
        GROUP BY dc.city_id, dc.city_name, dc.state
        ORDER BY avg_aqi DESC
    """).fetchall()

    # 3. AQI Categories (US EPA, filtered by city if selected)
    cat_sql = f"""
        SELECT fr.aqi_category, COUNT(*) as count
        FROM fact_readings fr
        {where_fact}
        GROUP BY fr.aqi_category
        ORDER BY count DESC
    """
    categories = cur.execute(cat_sql, params_kpi).fetchall()

    # 4. Hourly Trend (restricted to timestamps <= now in Asia/Kolkata, limited to 48 hours)
    trend_where = "WHERE fr.timestamp <= ?"
    trend_params = [now_kolkata]
    if city_id is not None:
        trend_where += " AND fr.city_id = ?"
        trend_params.append(city_id)

    trend_sql = f"""
        SELECT fr.timestamp, ROUND(AVG(fr.us_aqi), 1) as avg_aqi
        FROM fact_readings fr
        {trend_where}
        GROUP BY fr.timestamp
        ORDER BY fr.timestamp DESC
        LIMIT 48
    """
    trend = cur.execute(trend_sql, trend_params).fetchall()
    trend_list = [dict(r) for r in reversed(trend)]
    latest_observed_hour = trend_list[-1]["timestamp"] if trend_list else None

    # 5. Temperature vs PM2.5 Scatter Data (filtered by city if selected)
    scatter_where = "WHERE fr.temperature IS NOT NULL AND fr.pm2_5 IS NOT NULL"
    scatter_params: list = []
    if city_id is not None:
        scatter_where += " AND fr.city_id = ?"
        scatter_params.append(city_id)

    scatter_sql = f"""
        SELECT
            dc.city_name,
            fr.timestamp,
            ROUND(fr.temperature, 1) as temperature,
            ROUND(fr.pm2_5, 1) as pm2_5,
            fr.us_aqi,
            fr.aqi_category
        FROM fact_readings fr
        JOIN dim_city dc ON fr.city_id = dc.city_id
        {scatter_where}
        ORDER BY fr.timestamp DESC
        LIMIT 300
    """
    scatter = cur.execute(scatter_sql, scatter_params).fetchall()
    scatter_list = [
        {
            "x": r["temperature"],
            "y": r["pm2_5"],
            "city": r["city_name"],
            "aqi": r["us_aqi"],
            "category": r["aqi_category"],
            "timestamp": r["timestamp"],
        }
        for r in scatter
    ]

    # 6. ETL Logs
    logs = cur.execute("""
        SELECT * FROM etl_run_log ORDER BY run_id DESC LIMIT 5
    """).fetchall()

    conn.close()

    return {
        "read_only": READ_ONLY_MODE,
        "selected_city_id": city_id,
        "selected_city_name": selected_city_name,
        "current_kolkata_time": now_kolkata,
        "latest_observed_hour": latest_observed_hour,
        "kpis": dict(kpi) if kpi else {},
        "city_list": city_list,
        "cities": [dict(r) for r in city_avgs],
        "categories": [dict(r) for r in categories],
        "trend": trend_list,
        "scatter": scatter_list,
        "logs": [dict(r) for r in logs],
    }


def execute_custom_sql(query_text: str) -> dict:
    """Execute SQL query against aqi.db, enforcing read-only constraints if enabled."""
    if not DB_PATH.exists():
        return {"error": "Database not initialized. Please run ETL first."}

    # Security check: Read-Only mode restriction
    if READ_ONLY_MODE:
        normalized = normalize_sql(query_text)
        if normalized not in NORMALIZED_PRESETS:
            return {
                "error": "🔒 Read-Only Mode Active: Free-text SQL is disabled. Please select one of the 6 prebuilt queries."
            }

    # Block write operations regardless
    blocked_keywords = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "replace", "vacuum"]
    first_word = query_text.strip().split()[0].lower() if query_text.strip() else ""
    if first_word in blocked_keywords:
        return {"error": f"Write operation '{first_word.upper()}' is not permitted in the SQL query console."}

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute(query_text)
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall()
    conn.close()
    return {"columns": cols, "rows": [list(r) for r in rows[:200]]}


def trigger_etl_run() -> dict:
    """Execute run_etl.py and read the resulting stats from etl_run_log."""
    if READ_ONLY_MODE:
        return {"error": "🔒 Pipeline execution is disabled in Read-Only Mode."}

    proc = subprocess.run(
        [str(VENV_PYTHON), str(PROJECT_ROOT / "run_etl.py")],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )

    # Read latest stats from etl_run_log
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    row = cur.execute(
        "SELECT rows_extracted, rows_loaded, rows_rejected, status FROM etl_run_log ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    conn.close()

    if row:
        return {
            "status": row[3],
            "extracted": row[0],
            "loaded": row[1],
            "rejected": row[2],
            "output": proc.stdout[-300:],
        }
    return {"status": "SUCCESS", "extracted": 0, "loaded": 0, "rejected": 0, "output": proc.stdout[-300:]}


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>🇮🇳 India Air Quality & Weather ETL — Local Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    body { font-family: 'Inter', sans-serif; background-color: #0f172a; color: #f8fafc; }
  </style>
</head>
<body class="p-6">
  <div class="max-w-7xl mx-auto space-y-6">

    <!-- Header -->
    <header class="flex flex-col md:flex-row md:items-center md:justify-between border-b border-slate-800 pb-4 gap-4">
      <div>
        <div class="flex items-center space-x-3">
          <span class="text-3xl">🇮🇳</span>
          <h1 class="text-2xl font-bold text-white">India Air Quality & Weather ETL Dashboard</h1>
          <span id="readOnlyBadge" class="hidden px-2.5 py-0.5 rounded-full text-xs font-semibold bg-amber-950 text-amber-300 border border-amber-800">
            🔒 Read-Only Mode
          </span>
        </div>
        <p class="text-slate-400 text-sm mt-1">Live data modeled into SQLite Star Schema using US EPA AQI Standards</p>
      </div>

      <div class="flex flex-wrap items-center gap-3">
        <!-- City Selector -->
        <div class="flex items-center space-x-2 bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5">
          <label for="cityFilter" class="text-xs font-semibold text-slate-400 uppercase">City:</label>
          <select id="cityFilter" onchange="loadDashboard()" class="bg-transparent text-sm font-medium text-white focus:outline-none cursor-pointer">
            <option value="" class="bg-slate-900 text-white">All 10 Cities</option>
          </select>
        </div>

        <button id="refreshBtn" onclick="loadDashboard()" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-sm font-medium rounded-lg border border-slate-700 transition">
          🔄 Refresh
        </button>

        <button id="runPipelineBtn" onclick="triggerPipeline()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg transition flex items-center space-x-2">
          <span>⚡ Run ETL Now</span>
        </button>
      </div>
    </header>

    <!-- KPI Cards -->
    <div class="grid grid-cols-2 md:grid-cols-5 gap-4" id="kpiContainer">
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider">Average AQI</p>
        <p class="text-3xl font-bold text-amber-400 mt-2" id="kpiAvgAqi">--</p>
        <span class="text-xs text-slate-500" id="kpiCityScope">Across 10 cities</span>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider">Average PM2.5</p>
        <p class="text-3xl font-bold text-rose-400 mt-2" id="kpiAvgPm25">--</p>
        <span class="text-xs text-slate-500">µg/m³</span>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider">% Unhealthy+ Hours</p>
        <p class="text-3xl font-bold text-orange-400 mt-2" id="kpiPctPoor">--%</p>
        <span class="text-xs text-slate-500">US EPA Unhealthy+</span>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider">Peak AQI</p>
        <p class="text-3xl font-bold text-red-500 mt-2" id="kpiMaxAqi">--</p>
        <span class="text-xs text-slate-500">Max observed value</span>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-4">
        <p class="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Readings</p>
        <p class="text-3xl font-bold text-emerald-400 mt-2" id="kpiTotal">--</p>
        <span class="text-xs text-slate-500">Hourly records</span>
      </div>
    </div>

    <!-- Charts Row 1: City Ranking & Category Donut -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h2 class="text-base font-semibold text-slate-200 mb-4" id="cityBarTitle">City AQI Comparison</h2>
        <div class="h-72">
          <canvas id="cityBarChart"></canvas>
        </div>
      </div>

      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h2 class="text-base font-semibold text-slate-200 mb-4">US EPA AQI Category Breakdown</h2>
        <div class="h-72">
          <canvas id="categoryDonutChart"></canvas>
        </div>
      </div>
    </div>

    <!-- Charts Row 2: 48-Hour Trend & Temp vs PM2.5 Scatter -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- 48-Hour Trend (up to now) -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div class="flex items-center justify-between mb-4">
          <div>
            <h2 class="text-base font-semibold text-slate-200" id="trendTitle">48-Hour Hourly Trend (Up to Now)</h2>
            <p class="text-xs text-slate-400 mt-0.5" id="trendSubtitle">Asia/Kolkata cutoff: latest observed hour</p>
          </div>
          <span class="text-xs text-indigo-400 font-mono" id="trendLatestBadge"></span>
        </div>
        <div class="h-64">
          <canvas id="trendLineChart"></canvas>
        </div>
      </div>

      <!-- Temperature vs PM2.5 Scatter with Legend & Subtitle -->
      <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div class="mb-3">
          <div class="flex items-center justify-between">
            <h2 class="text-base font-semibold text-slate-200" id="scatterTitle">Temperature vs PM2.5 Correlation</h2>
            <span class="text-xs text-slate-500" id="scatterCountBadge"></span>
          </div>
          <p class="text-xs text-slate-400 italic mt-0.5">AQI uses 24h rolling PM averages, so hourly PM2.5 won't match color exactly</p>
        </div>
        <div class="h-64">
          <canvas id="scatterChart"></canvas>
        </div>
      </div>
    </div>

    <!-- Interactive SQL Query Console -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5 space-y-4">
      <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
        <div>
          <h2 class="text-base font-semibold text-slate-200">Interactive SQL Analysis</h2>
          <p class="text-xs text-slate-400" id="sqlHelpText">Run pre-built CTE & Window function queries against db/aqi.db</p>
        </div>
        <select id="querySelect" onchange="loadPresetQuery()" class="bg-slate-800 border border-slate-700 text-slate-200 text-sm rounded-lg p-2 focus:ring-indigo-500">
          <option value="1">1. Pollution Ranking by City (RANK)</option>
          <option value="2">2. Worst 5 Hours per City (ROW_NUMBER with tie-breaker)</option>
          <option value="3">3. 24-Hour Moving Average of PM2.5</option>
          <option value="4">4. Day-over-Day AQI Change (LAG)</option>
          <option value="5">5. % Hours in Unhealthy or Worse Tier (US EPA)</option>
          <option value="6">6. Weekday vs Weekend Comparison</option>
        </select>
      </div>

      <textarea id="sqlInput" rows="4" class="w-full bg-slate-950 border border-slate-800 rounded-lg p-3 text-sm font-mono text-emerald-400 focus:outline-none focus:border-indigo-500"></textarea>

      <div class="flex justify-between items-center">
        <button id="runQueryBtn" onclick="runQuery()" class="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-semibold rounded-lg transition">
          ▶ Execute Query
        </button>
        <span id="queryStatus" class="text-xs text-slate-400"></span>
      </div>

      <!-- SQL Results Table -->
      <div class="overflow-x-auto max-h-72 border border-slate-800 rounded-lg">
        <table id="sqlTable" class="min-w-full divide-y divide-slate-800 text-left text-xs">
          <thead id="sqlTableHead" class="bg-slate-950 text-slate-400 uppercase sticky top-0"></thead>
          <tbody id="sqlTableBody" class="divide-y divide-slate-800 text-slate-300 font-mono"></tbody>
        </table>
      </div>
    </div>

    <!-- ETL Run Logs -->
    <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
      <h2 class="text-base font-semibold text-slate-200 mb-3">Recent ETL Audit Runs (etl_run_log)</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-xs text-left">
          <thead class="text-slate-400 border-b border-slate-800 pb-2">
            <tr>
              <th class="py-2">Run ID</th>
              <th>Run Time</th>
              <th>Extracted</th>
              <th>Loaded</th>
              <th>Rejected</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="logsTableBody" class="divide-y divide-slate-800 font-mono"></tbody>
        </table>
      </div>
    </div>

  </div>

  <script>
    let cityChart = null, categoryChart = null, trendChart = null, scatterChart = null;
    let isReadOnly = false;
    let cityListLoaded = false;

    const PRESET_QUERIES = {
      "1": `SELECT dc.city_name, dc.state, ROUND(AVG(fr.us_aqi), 1) AS avg_aqi, RANK() OVER (ORDER BY AVG(fr.us_aqi) DESC) AS pollution_rank FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id GROUP BY dc.city_name, dc.state ORDER BY pollution_rank;`,
      "2": `WITH ranked AS (SELECT dc.city_name, fr.timestamp, fr.us_aqi, ROW_NUMBER() OVER (PARTITION BY fr.city_id ORDER BY fr.us_aqi DESC, fr.timestamp DESC) AS rnk FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id) SELECT city_name, timestamp, us_aqi, rnk FROM ranked WHERE rnk <= 5 ORDER BY city_name, rnk;`,
      "3": `SELECT dc.city_name, fr.timestamp, fr.pm2_5, ROUND(AVG(fr.pm2_5) OVER (PARTITION BY fr.city_id ORDER BY fr.timestamp ROWS BETWEEN 23 PRECEDING AND CURRENT ROW), 2) AS pm25_24h_avg FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id ORDER BY dc.city_name, fr.timestamp LIMIT 100;`,
      "4": `WITH daily AS (SELECT dc.city_name, dd.date, ROUND(AVG(fr.us_aqi), 1) AS daily_avg_aqi FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id JOIN dim_date dd ON fr.date_id = dd.date_id GROUP BY dc.city_name, dd.date) SELECT city_name, date, daily_avg_aqi, LAG(daily_avg_aqi) OVER (PARTITION BY city_name ORDER BY date) AS prev_day_aqi, ROUND(daily_avg_aqi - LAG(daily_avg_aqi) OVER (PARTITION BY city_name ORDER BY date), 1) AS aqi_change FROM daily ORDER BY city_name, date;`,
      "5": `SELECT dc.city_name, COUNT(*) AS total_hours, SUM(CASE WHEN fr.aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) AS unhealthy_or_worse_hours, ROUND(100.0 * SUM(CASE WHEN fr.aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous') THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_unhealthy_or_worse FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id GROUP BY dc.city_name ORDER BY pct_unhealthy_or_worse DESC;`,
      "6": `WITH comparison AS (SELECT dc.city_name, dd.is_weekend, ROUND(AVG(fr.us_aqi), 1) AS avg_aqi, ROUND(AVG(fr.pm2_5), 1) AS avg_pm25 FROM fact_readings fr JOIN dim_city dc ON fr.city_id = dc.city_id JOIN dim_date dd ON fr.date_id = dd.date_id GROUP BY dc.city_name, dd.is_weekend) SELECT city_name, CASE is_weekend WHEN 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, avg_aqi, avg_pm25 FROM comparison ORDER BY city_name, is_weekend;`
    };

    const EPA_COLORS = {
      'Good': '#10b981',
      'Moderate': '#eab308',
      'Unhealthy for Sensitive Groups': '#f97316',
      'Unhealthy': '#ef4444',
      'Very Unhealthy': '#8b5cf6',
      'Hazardous': '#7f1d1d'
    };

    const EPA_ORDER = [
      'Good',
      'Moderate',
      'Unhealthy for Sensitive Groups',
      'Unhealthy',
      'Very Unhealthy',
      'Hazardous'
    ];

    function loadPresetQuery() {
      const id = document.getElementById('querySelect').value;
      document.getElementById('sqlInput').value = PRESET_QUERIES[id];
      runQuery();
    }

    async function loadDashboard() {
      const cityId = document.getElementById('cityFilter').value;
      const url = cityId ? `/api/data?city_id=${cityId}` : '/api/data';
      const res = await fetch(url);
      const data = await res.json();

      // Read-only handling
      isReadOnly = !!data.read_only;
      if (isReadOnly) {
        document.getElementById('readOnlyBadge').classList.remove('hidden');
        document.getElementById('runPipelineBtn').disabled = true;
        document.getElementById('runPipelineBtn').title = "Disabled in Read-Only Mode";
        document.getElementById('sqlInput').setAttribute('readonly', 'readonly');
        document.getElementById('sqlHelpText').innerText = "🔒 Read-Only Mode: Select pre-built queries from the dropdown below";
      }

      // Populate City Selector dropdown once
      if (!cityListLoaded && data.city_list) {
        const sel = document.getElementById('cityFilter');
        data.city_list.forEach(c => {
          const opt = document.createElement('option');
          opt.value = c.city_id;
          opt.textContent = `${c.city_name} (${c.region})`;
          opt.className = "bg-slate-900 text-white";
          sel.appendChild(opt);
        });
        cityListLoaded = true;
      }

      const selectedCityName = data.selected_city_name;

      // Update KPIs
      document.getElementById('kpiAvgAqi').innerText = data.kpis.avg_aqi || '--';
      document.getElementById('kpiAvgPm25').innerText = data.kpis.avg_pm25 || '--';
      document.getElementById('kpiPctPoor').innerText = (data.kpis.pct_poor || 0) + '%';
      document.getElementById('kpiMaxAqi').innerText = data.kpis.max_aqi || '--';
      document.getElementById('kpiTotal').innerText = Number(data.kpis.total_readings || 0).toLocaleString();
      document.getElementById('kpiCityScope').innerText = selectedCityName ? `Selected: ${selectedCityName}` : "Across 10 cities";

      // 1. Render Comparative Bar Chart (keep all 10 cities, highlight selected, dim others)
      const barTitleEl = document.getElementById('cityBarTitle');
      if (selectedCityName) {
        barTitleEl.innerText = `City AQI Comparison (selected: ${selectedCityName})`;
      } else {
        barTitleEl.innerText = "City AQI Comparison";
      }

      const cityLabels = data.cities.map(c => c.city_name);
      const cityAqi = data.cities.map(c => c.avg_aqi);

      let bgColors, borderColors, borderWidths;
      if (selectedCityName) {
        // Highlight selected city with accent, dim the other 9
        bgColors = data.cities.map(c => c.city_name === selectedCityName ? '#0ea5e9' : 'rgba(100, 116, 139, 0.25)');
        borderColors = data.cities.map(c => c.city_name === selectedCityName ? '#38bdf8' : 'rgba(100, 116, 139, 0.1)');
        borderWidths = data.cities.map(c => c.city_name === selectedCityName ? 2 : 0);
      } else {
        // No highlight when "All cities" selected
        bgColors = cityAqi.map(v => v >= 201 ? '#8b5cf6' : v >= 151 ? '#ef4444' : v >= 101 ? '#f97316' : v >= 51 ? '#eab308' : '#10b981');
        borderColors = 'transparent';
        borderWidths = 0;
      }

      if (cityChart) cityChart.destroy();
      cityChart = new Chart(document.getElementById('cityBarChart'), {
        type: 'bar',
        data: {
          labels: cityLabels,
          datasets: [{
            label: 'Avg US AQI',
            data: cityAqi,
            backgroundColor: bgColors,
            borderColor: borderColors,
            borderWidth: borderWidths,
            borderRadius: 6
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            y: { grid: { color: '#1e293b' }, ticks: { color: '#94a3b8' } },
            x: {
              grid: { display: false },
              ticks: {
                color: (ctx) => {
                  const label = cityLabels[ctx.index];
                  return (selectedCityName && label === selectedCityName) ? '#38bdf8' : '#cbd5e1';
                },
                font: (ctx) => {
                  const label = cityLabels[ctx.index];
                  return (selectedCityName && label === selectedCityName) ? { weight: 'bold' } : { weight: 'normal' };
                }
              }
            }
          }
        }
      });

      // 2. Render Categories Donut
      const catLabels = data.categories.map(c => c.aqi_category);
      const catCounts = data.categories.map(c => c.count);
      if (categoryChart) categoryChart.destroy();
      categoryChart = new Chart(document.getElementById('categoryDonutChart'), {
        type: 'doughnut',
        data: {
          labels: catLabels,
          datasets: [{
            data: catCounts,
            backgroundColor: catLabels.map(l => EPA_COLORS[l] || '#64748b'),
            borderWidth: 0
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: 'bottom', labels: { color: '#94a3b8', font: { size: 11 } } }
          }
        }
      });

      // 3. Render 48-Hour Trend Line Chart (up to Asia/Kolkata now)
      const trendTitleEl = document.getElementById('trendTitle');
      trendTitleEl.innerText = selectedCityName ? `48-Hour Trend: ${selectedCityName}` : "48-Hour Overall Hourly Trend";

      const badgeEl = document.getElementById('trendLatestBadge');
      if (data.latest_observed_hour) {
        badgeEl.innerText = `Latest: ${data.latest_observed_hour.slice(5, 16)}`;
      } else {
        badgeEl.innerText = "";
      }

      const trendTimes = data.trend.map(t => t.timestamp.split(' ')[1] ? t.timestamp.slice(5, 16) : t.timestamp);
      const trendValues = data.trend.map(t => t.avg_aqi);
      if (trendChart) trendChart.destroy();
      trendChart = new Chart(document.getElementById('trendLineChart'), {
        type: 'line',
        data: {
          labels: trendTimes,
          datasets: [{
            label: selectedCityName ? `${selectedCityName} Hourly AQI` : 'Overall Hourly Avg AQI',
            data: trendValues,
            borderColor: selectedCityName ? '#0ea5e9' : '#6366f1',
            backgroundColor: selectedCityName ? 'rgba(14, 165, 233, 0.1)' : 'rgba(99, 102, 241, 0.1)',
            fill: true,
            tension: 0.3,
            pointRadius: 2
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { labels: { color: '#cbd5e1' } } },
          scales: {
            y: { grid: { color: '#1e293b' }, ticks: { color: '#94a3b8' } },
            x: { grid: { color: '#1e293b' }, ticks: { color: '#94a3b8', maxTicksLimit: 10 } }
          }
        }
      });

      // 4. Render Temperature vs PM2.5 Scatter Chart with Category Legend
      document.getElementById('scatterTitle').innerText = selectedCityName
        ? `Temperature vs PM2.5 (${selectedCityName})`
        : "Temperature vs PM2.5 Correlation";
      document.getElementById('scatterCountBadge').innerText = `${data.scatter.length} readings`;

      // Group scatter points into datasets per EPA category so Chart.js displays a proper legend
      const scatterDatasets = [];
      EPA_ORDER.forEach(cat => {
        const points = data.scatter.filter(s => s.category === cat);
        if (points.length > 0) {
          scatterDatasets.push({
            label: cat,
            data: points.map(s => ({ x: s.x, y: s.y, raw: s })),
            backgroundColor: EPA_COLORS[cat] || '#6366f1',
            borderColor: 'transparent',
            pointRadius: 4,
            pointHoverRadius: 6
          });
        }
      });

      if (scatterChart) scatterChart.destroy();
      scatterChart = new Chart(document.getElementById('scatterChart'), {
        type: 'scatter',
        data: {
          datasets: scatterDatasets
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: {
              display: true,
              position: 'bottom',
              labels: {
                color: '#cbd5e1',
                boxWidth: 10,
                boxHeight: 10,
                font: { size: 10 },
                padding: 10
              }
            },
            tooltip: {
              callbacks: {
                label: function(ctx) {
                  const r = ctx.raw.raw;
                  return `${r.city}: ${r.temperature}°C, PM2.5: ${r.pm2_5} µg/m³ (AQI: ${r.aqi} - ${r.category})`;
                }
              }
            }
          },
          scales: {
            x: {
              title: { display: true, text: 'Temperature (°C)', color: '#94a3b8' },
              grid: { color: '#1e293b' },
              ticks: { color: '#94a3b8' }
            },
            y: {
              title: { display: true, text: 'PM2.5 (µg/m³)', color: '#94a3b8' },
              grid: { color: '#1e293b' },
              ticks: { color: '#94a3b8' }
            }
          }
        }
      });

      // Render Logs Table
      const logsBody = document.getElementById('logsTableBody');
      logsBody.innerHTML = data.logs.map(l => `
        <tr class="hover:bg-slate-800/50">
          <td class="py-2 text-indigo-400">#${l.run_id}</td>
          <td>${l.run_time}</td>
          <td>${l.rows_extracted}</td>
          <td class="${l.rows_loaded > 0 ? 'text-emerald-400 font-bold' : 'text-slate-400'}">${l.rows_loaded}</td>
          <td>${l.rows_rejected}</td>
          <td><span class="px-2 py-0.5 rounded text-[10px] font-semibold ${l.status === 'SUCCESS' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' : 'bg-red-950 text-red-400'}">${l.status}</span></td>
        </tr>
      `).join('');
    }

    async function runQuery() {
      const sql = document.getElementById('sqlInput').value.trim();
      const statusEl = document.getElementById('queryStatus');
      statusEl.innerText = "Executing...";

      try {
        const res = await fetch('/api/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: sql })
        });
        const data = await res.json();
        if (data.error) {
          statusEl.innerText = data.error;
          return;
        }

        const thead = document.getElementById('sqlTableHead');
        const tbody = document.getElementById('sqlTableBody');
        thead.innerHTML = `<tr>${data.columns.map(c => `<th class="py-2 px-3">${c}</th>`).join('')}</tr>`;
        tbody.innerHTML = data.rows.map(r => `
          <tr class="hover:bg-slate-900">
            ${r.map(val => `<td class="py-1.5 px-3 whitespace-nowrap">${val === null ? '<span class=\"text-slate-600\">null</span>' : val}</td>`).join('')}
          </tr>
        `).join('');

        statusEl.innerText = `${data.rows.length} rows returned`;
      } catch (e) {
        statusEl.innerText = "Network error: " + e;
      }
    }

    async function triggerPipeline() {
      if (isReadOnly) {
        alert("🔒 Pipeline execution is disabled in Read-Only Mode.");
        return;
      }

      const btn = document.getElementById('runPipelineBtn');
      const oldHtml = btn.innerHTML;
      btn.innerHTML = `<span>⏳ Extracting...</span>`;
      btn.disabled = true;

      try {
        const res = await fetch('/api/run-pipeline', { method: 'POST' });
        const data = await res.json();
        if (data.error) {
          alert("Pipeline error: " + data.error);
        } else {
          alert(`✅ ETL Run Complete!\\n\\nStatus: ${data.status}\\nRows Extracted: ${data.extracted}\\nRows Loaded: ${data.loaded}\\nRows Rejected: ${data.rejected}`);
          await loadDashboard();
        }
      } catch (e) {
        alert("Pipeline trigger failed: " + e);
      } finally {
        btn.innerHTML = oldHtml;
        btn.disabled = false;
      }
    }

    window.onload = () => {
      loadDashboard();
      loadPresetQuery();
    };
  </script>
</body>
</html>
"""


class DashboardHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        if url.path in ["/", "/index.html"]:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif url.path == "/api/data":
            try:
                params = parse_qs(url.query)
                city_id = None
                if "city_id" in params and params["city_id"][0]:
                    try:
                        city_id = int(params["city_id"][0])
                    except ValueError:
                        pass
                data = get_db_data(city_id=city_id)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        url = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        post_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else ""

        if url.path == "/api/query":
            try:
                body = json.loads(post_body) if post_body else {}
                query = body.get("query", "")
                result = execute_custom_sql(query)
                status_code = 403 if "error" in result and "Read-Only Mode" in result["error"] else 200
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode("utf-8"))
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))

        elif url.path == "/api/run-pipeline":
            if READ_ONLY_MODE:
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Pipeline execution is disabled in Read-Only Mode."}).encode("utf-8"))
                return

            try:
                result = trigger_etl_run()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "FAILED", "error": str(e)}).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return


def run_server(port: int = PORT, read_only: bool = False):
    global READ_ONLY_MODE
    if read_only:
        READ_ONLY_MODE = True
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), DashboardHandler)
    mode_str = " [READ-ONLY MODE]" if READ_ONLY_MODE else ""
    print(f"🚀 Dashboard running locally at http://localhost:{port}{mode_str}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard server...")
        server.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="India AQI Local Web Dashboard")
    parser.add_argument("--port", type=int, default=PORT, help=f"Server port (default {PORT})")
    parser.add_argument("--read-only", action="store_true", help="Enable read-only mode (disable Run ETL & lock SQL)")
    args = parser.parse_args()
    run_server(port=args.port, read_only=args.read_only)
