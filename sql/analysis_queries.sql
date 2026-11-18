-- ============================================================
-- India AQI ETL — Analytical Queries
-- Uses CTEs and window functions for resume-worthy SQL.
-- ============================================================

-- 1. Average AQI by city (ranked)
-- --------------------------------------------------------
SELECT
    dc.city_name,
    dc.state,
    ROUND(AVG(fr.us_aqi), 1)                              AS avg_aqi,
    RANK() OVER (ORDER BY AVG(fr.us_aqi) DESC)             AS pollution_rank
FROM fact_readings fr
JOIN dim_city dc ON fr.city_id = dc.city_id
GROUP BY dc.city_name, dc.state
ORDER BY pollution_rank;


-- 2. Worst 5 hours per city (using ROW_NUMBER with timestamp tie-breaker)
-- --------------------------------------------------------
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


-- 3. 24-hour moving average of PM2.5 per city
-- --------------------------------------------------------
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
ORDER BY dc.city_name, fr.timestamp;


-- 4. Day-over-day AQI change using LAG
-- --------------------------------------------------------
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


-- 5. Percentage of hours in "Unhealthy" or worse per city (US EPA standard)
-- --------------------------------------------------------
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


-- 6. Weekday vs Weekend AQI comparison
-- --------------------------------------------------------
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
