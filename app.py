"""India Air Quality & Weather ETL — Streamlit Dashboard.

Features:
- Live KPI cards (Avg AQI, Avg PM2.5, % Unhealthy+ Hours, Peak AQI, Total Readings)
- City selector (All 10 cities or individual city filter)
- City comparison bar chart with selected city highlighted in accent color
- US EPA AQI category donut chart
- 48-hour observed-only trend (Asia/Kolkata timezone cutoff)
- Temperature vs PM2.5 scatter plot with legend and subtitle
- 6 prebuilt analytical SQL queries (strictly read-only)
- ETL audit log viewer
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "db" / "aqi.db"

# US EPA Category Colors
EPA_COLORS = {
    "Good": "#10b981",
    "Moderate": "#eab308",
    "Unhealthy for Sensitive Groups": "#f97316",
    "Unhealthy": "#ef4444",
    "Very Unhealthy": "#8b5cf6",
    "Hazardous": "#7f1d1d",
}

EPA_ORDER = [
    "Good",
    "Moderate",
    "Unhealthy for Sensitive Groups",
    "Unhealthy",
    "Very Unhealthy",
    "Hazardous",
]

PRESET_QUERIES = {
    "1. Pollution Ranking by City (RANK)": """
SELECT
    dc.city_name,
    dc.state,
    ROUND(AVG(fr.us_aqi), 1)                              AS avg_aqi,
    RANK() OVER (ORDER BY AVG(fr.us_aqi) DESC)             AS pollution_rank
FROM fact_readings fr
JOIN dim_city dc ON fr.city_id = dc.city_id
GROUP BY dc.city_name, dc.state
ORDER BY pollution_rank;
""",
    "2. Worst 5 Hours per City (ROW_NUMBER with tie-breaker)": """
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
""",
    "3. 24-Hour Moving Average of PM2.5": """
SELECT
    dc.city_name,
    fr.timestamp,
    fr.pm2_5,
    ROUND(
        AVG(fr.pm2_5) OVER (
            PARTITION BY fr.city_id
            ORDER BY fr.timestamp
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ), 2
    ) AS pm25_24h_avg
FROM fact_readings fr
JOIN dim_city dc ON fr.city_id = dc.city_id
ORDER BY dc.city_name, fr.timestamp
LIMIT 100;
""",
    "4. Day-over-Day AQI Change (LAG)": """
WITH daily AS (
    SELECT
        dc.city_name,
        dd.date,
        ROUND(AVG(fr.us_aqi), 1) AS daily_avg_aqi
    FROM fact_readings fr
    JOIN dim_city dc ON fr.city_id = dc.city_id
    JOIN dim_date dd ON fr.date_id = dd.date_id
    GROUP BY dc.city_name, dd.date
)
SELECT
    city_name,
    date,
    daily_avg_aqi,
    LAG(daily_avg_aqi) OVER (
        PARTITION BY city_name ORDER BY date
    ) AS prev_day_aqi,
    ROUND(
        daily_avg_aqi - LAG(daily_avg_aqi) OVER (
            PARTITION BY city_name ORDER BY date
        ), 1
    ) AS aqi_change
FROM daily
ORDER BY city_name, date;
""",
    "5. % Hours in Unhealthy or Worse Tier (US EPA)": """
SELECT
    dc.city_name,
    COUNT(*)                                                   AS total_hours,
    SUM(CASE
        WHEN fr.aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous')
        THEN 1 ELSE 0
    END)                                                       AS unhealthy_or_worse_hours,
    ROUND(
        100.0 * SUM(CASE
            WHEN fr.aqi_category IN ('Unhealthy', 'Very Unhealthy', 'Hazardous')
            THEN 1 ELSE 0
        END) / COUNT(*), 1
    )                                                          AS pct_unhealthy_or_worse
FROM fact_readings fr
JOIN dim_city dc ON fr.city_id = dc.city_id
GROUP BY dc.city_name
ORDER BY pct_unhealthy_or_worse DESC;
""",
    "6. Weekday vs Weekend Comparison": """
WITH comparison AS (
    SELECT
        dc.city_name,
        dd.is_weekend,
        ROUND(AVG(fr.us_aqi), 1) AS avg_aqi,
        ROUND(AVG(fr.pm2_5), 1)  AS avg_pm25
    FROM fact_readings fr
    JOIN dim_city dc ON fr.city_id = dc.city_id
    JOIN dim_date dd ON fr.date_id = dd.date_id
    GROUP BY dc.city_name, dd.is_weekend
)
SELECT
    city_name,
    CASE is_weekend WHEN 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    avg_aqi,
    avg_pm25
FROM comparison
ORDER BY city_name, is_weekend;
""",
}


def get_db_connection() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH))


def get_kolkata_now() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")


def load_data():
    conn = get_db_connection()
    cities_df = pd.read_sql_query("SELECT city_id, city_name, state, region FROM dim_city ORDER BY city_name", conn)
    readings_df = pd.read_sql_query(
        """
        SELECT
            fr.reading_id,
            fr.city_id,
            dc.city_name,
            dc.state,
            dc.region,
            fr.date_id,
            fr.timestamp,
            fr.hour,
            fr.pm10,
            fr.pm2_5,
            fr.co,
            fr.no2,
            fr.so2,
            fr.ozone,
            fr.us_aqi,
            fr.aqi_category,
            fr.temperature,
            fr.humidity,
            fr.wind_speed,
            fr.precipitation
        FROM fact_readings fr
        JOIN dim_city dc ON fr.city_id = dc.city_id
        """,
        conn,
    )
    logs_df = pd.read_sql_query("SELECT * FROM etl_run_log ORDER BY run_id DESC LIMIT 10", conn)
    conn.close()
    return cities_df, readings_df, logs_df


def main():
    st.set_page_config(
        page_title="India Air Quality & Weather ETL",
        page_icon="🇮🇳",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    if not DB_PATH.exists():
        st.error("Database db/aqi.db not found. Please run run_etl.py first.")
        st.stop()

    cities_df, readings_df, logs_df = load_data()
    now_kolkata = get_kolkata_now()

    # Sidebar: City Selector & Read-Only Badge
    st.sidebar.title("🇮🇳 India AQI ETL")
    st.sidebar.markdown(
        """
        <div style="background-color: #1e293b; padding: 10px; border-radius: 8px; border: 1px solid #334155; margin-bottom: 15px;">
            <span style="color: #f59e0b; font-weight: 600;">🔒 Read-Only Dashboard</span><br>
            <span style="color: #94a3b8; font-size: 12px;">Prebuilt analytical views from SQLite Star Schema</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    city_options = ["All 10 Cities"] + cities_df["city_name"].tolist()
    selected_option = st.sidebar.selectbox("Select City Filter", city_options, index=0)
    selected_city = None if selected_option == "All 10 Cities" else selected_option

    # Filter fact readings for selected city
    if selected_city:
        filtered_df = readings_df[readings_df["city_name"] == selected_city].copy()
    else:
        filtered_df = readings_df.copy()

    # Header
    st.title("🇮🇳 India Air Quality & Weather ETL Dashboard")
    st.caption(f"US EPA Standard • Star Schema in SQLite • Current Asia/Kolkata Time: {now_kolkata}")

    st.markdown("---")

    # 1. KPI Cards (5 columns)
    k1, k2, k3, k4, k5 = st.columns(5)

    avg_aqi = filtered_df["us_aqi"].mean() if not filtered_df.empty else 0
    avg_pm25 = filtered_df["pm2_5"].mean() if not filtered_df.empty else 0
    max_aqi = filtered_df["us_aqi"].max() if not filtered_df.empty else 0
    total_readings = len(filtered_df)

    unhealthy_count = (
        filtered_df["aqi_category"].isin(["Unhealthy", "Very Unhealthy", "Hazardous"]).sum()
        if not filtered_df.empty
        else 0
    )
    pct_unhealthy = (unhealthy_count / total_readings * 100) if total_readings > 0 else 0

    scope_caption = f"Selected: {selected_city}" if selected_city else "Across 10 Cities"

    with k1:
        st.metric(label="Average AQI", value=f"{avg_aqi:.1f}", help=scope_caption)
    with k2:
        st.metric(label="Average PM2.5", value=f"{avg_pm25:.1f} µg/m³", help="Particulate Matter 2.5")
    with k3:
        st.metric(label="% Unhealthy+ Hours", value=f"{pct_unhealthy:.1f}%", help="US EPA Unhealthy, Very Unhealthy, or Hazardous")
    with k4:
        st.metric(label="Peak AQI", value=f"{max_aqi:.0f}", help="Highest recorded hourly AQI")
    with k5:
        st.metric(label="Total Readings", value=f"{total_readings:,}", help=scope_caption)

    st.markdown("---")

    # 2. Charts Row 1: City Bar Chart & Category Donut
    c1, c2 = st.columns([3, 2])

    with c1:
        # Comparative Bar Chart: always all 10 cities, highlight selected city
        city_avg = (
            readings_df.groupby("city_name")["us_aqi"]
            .mean()
            .reset_index()
            .sort_values(by="us_aqi", ascending=False)
        )

        bar_title = (
            f"City AQI Comparison (selected: {selected_city})"
            if selected_city
            else "City AQI Comparison"
        )

        if selected_city:
            # Highlight selected in bright cyan, dim the other 9
            colors = [
                "#0ea5e9" if c == selected_city else "rgba(148, 163, 184, 0.35)"
                for c in city_avg["city_name"]
            ]
        else:
            # Color by EPA category bands
            def get_color(val):
                if val >= 201:
                    return "#8b5cf6"
                if val >= 151:
                    return "#ef4444"
                if val >= 101:
                    return "#f97316"
                if val >= 51:
                    return "#eab308"
                return "#10b981"

            colors = [get_color(v) for v in city_avg["us_aqi"]]

        fig_bar = go.Figure(
            data=[
                go.Bar(
                    x=city_avg["city_name"],
                    y=city_avg["us_aqi"],
                    marker=dict(color=colors),
                    text=city_avg["us_aqi"].round(1),
                    textposition="auto",
                )
            ]
        )
        fig_bar.update_layout(
            title=bar_title,
            xaxis_title="City",
            yaxis_title="Average US AQI",
            template="plotly_dark",
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with c2:
        # Category Donut (filtered by selected city)
        cat_counts = (
            filtered_df["aqi_category"]
            .value_counts()
            .reindex(EPA_ORDER)
            .dropna()
            .reset_index()
        )
        cat_counts.columns = ["Category", "Count"]
        cat_counts = cat_counts[cat_counts["Count"] > 0]

        donut_colors = [EPA_COLORS.get(cat, "#64748b") for cat in cat_counts["Category"]]

        fig_donut = go.Figure(
            data=[
                go.Pie(
                    labels=cat_counts["Category"],
                    values=cat_counts["Count"],
                    hole=0.55,
                    marker=dict(colors=donut_colors),
                    textinfo="percent+label",
                    insidetextorientation="radial",
                )
            ]
        )
        fig_donut.update_layout(
            title="US EPA AQI Category Breakdown",
            template="plotly_dark",
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            showlegend=False,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    # 3. Charts Row 2: 48-Hour Trend & Temp vs PM2.5 Scatter
    c3, c4 = st.columns(2)

    with c3:
        # 48-Hour Trend: timestamps <= now_kolkata only
        trend_df = (
            filtered_df[filtered_df["timestamp"] <= now_kolkata]
            .groupby("timestamp")["us_aqi"]
            .mean()
            .reset_index()
            .sort_values(by="timestamp", ascending=False)
            .head(48)
            .sort_values(by="timestamp", ascending=True)
        )

        latest_hour = trend_df["timestamp"].iloc[-1] if not trend_df.empty else "N/A"
        trend_title = (
            f"48-Hour Trend: {selected_city}"
            if selected_city
            else "48-Hour Overall Hourly Trend"
        )

        fig_trend = go.Figure()
        fig_trend.add_trace(
            go.Scatter(
                x=trend_df["timestamp"],
                y=trend_df["us_aqi"],
                mode="lines+markers",
                line=dict(color="#0ea5e9" if selected_city else "#6366f1", width=2.5),
                marker=dict(size=4),
                name="US AQI",
            )
        )
        fig_trend.update_layout(
            title=dict(
                text=f"{trend_title}<br><sup>Cutoff: Asia/Kolkata now | Latest observed: {latest_hour}</sup>"
            ),
            xaxis_title="Timestamp",
            yaxis_title="Hourly AQI",
            template="plotly_dark",
            height=380,
            margin=dict(l=20, r=20, t=60, b=20),
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    with c4:
        # Temperature vs PM2.5 Scatter with Legend & Subtitle
        scatter_df = filtered_df.dropna(subset=["temperature", "pm2_5"]).copy()
        scatter_title = (
            f"Temperature vs PM2.5 ({selected_city})"
            if selected_city
            else "Temperature vs PM2.5 Correlation"
        )

        fig_scatter = px.scatter(
            scatter_df,
            x="temperature",
            y="pm2_5",
            color="aqi_category",
            category_orders={"aqi_category": EPA_ORDER},
            color_discrete_map=EPA_COLORS,
            hover_data={
                "city_name": True,
                "timestamp": True,
                "temperature": ":.1f",
                "pm2_5": ":.1f",
                "us_aqi": True,
                "aqi_category": True,
            },
            labels={
                "temperature": "Temperature (°C)",
                "pm2_5": "PM2.5 (µg/m³)",
                "aqi_category": "EPA Category",
            },
        )
        fig_scatter.update_layout(
            title=dict(
                text=f"{scatter_title}<br><sup>AQI uses 24h rolling PM averages, so hourly PM2.5 won't match color exactly</sup>"
            ),
            template="plotly_dark",
            height=380,
            legend=dict(orientation="h", yanchor="bottom", y=-0.35, xanchor="center", x=0.5),
            margin=dict(l=20, r=20, t=60, b=20),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.markdown("---")

    # 4. Interactive SQL Analysis (Read-Only)
    st.subheader("🔍 Prebuilt SQL Analysis (Read-Only)")
    st.caption("Execute predefined CTE & Window function queries against db/aqi.db")

    query_name = st.selectbox("Select Analytical Query", list(PRESET_QUERIES.keys()), index=0)
    query_sql = PRESET_QUERIES[query_name]

    with st.expander("View SQL Query Definition", expanded=False):
        st.code(query_sql.strip(), language="sql")

    conn = get_db_connection()
    result_df = pd.read_sql_query(query_sql, conn)
    conn.close()

    st.dataframe(result_df, use_container_width=True, hide_index=True)
    st.caption(f"{len(result_df)} rows returned")

    st.markdown("---")

    # 5. ETL Audit Run Log
    st.subheader("📋 Recent ETL Runs (`etl_run_log`)")
    st.dataframe(logs_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
