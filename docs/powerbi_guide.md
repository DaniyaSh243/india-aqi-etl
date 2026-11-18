# Power BI Dashboard Guide

Step-by-step instructions to build an **India Air Quality & Weather Dashboard** in Power BI Desktop using the CSV exports from this ETL pipeline.

---

## 1. Import the CSV Files

1. Open **Power BI Desktop** → **Home** → **Get Data** → **Text/CSV**.
2. Import these three files from `data/processed/`:
   | File | Table |
   |------|-------|
   | `dim_city.csv` | dim_city |
   | `dim_date.csv` | dim_date |
   | `fact_readings.csv` | fact_readings |
3. Click **Load** (or **Transform Data** first if you want to review).

---

## 2. Set Up the Star Schema Relationships

1. Go to **Model** view (left sidebar, third icon).
2. Create these relationships by dragging keys:

| From (Fact) | To (Dimension) | Cardinality | Cross-filter |
|-------------|-----------------|-------------|--------------|
| `fact_readings.city_id` | `dim_city.city_id` | Many-to-One | Single |
| `fact_readings.date_id` | `dim_date.date_id` | Many-to-One | Single |

3. Verify the star schema looks correct — two dimensions connected to one fact table.

---

## 3. Create DAX Measures

Go to **Modeling** → **New Measure** and create each of these:

### Avg AQI
```dax
Avg AQI = AVERAGE(fact_readings[us_aqi])
```

### Avg PM2.5
```dax
Avg PM2.5 = AVERAGE(fact_readings[pm2_5])
```

### % Unhealthy Hours (US EPA)
```dax
% Unhealthy Hours =
DIVIDE(
    CALCULATE(
        COUNTROWS(fact_readings),
        fact_readings[aqi_category] IN {"Unhealthy", "Very Unhealthy", "Hazardous"}
    ),
    COUNTROWS(fact_readings),
    0
) * 100
```

### Max AQI
```dax
Max AQI = MAX(fact_readings[us_aqi])
```

### Latest AQI by City
```dax
Latest AQI =
CALCULATE(
    LASTNONBLANK(fact_readings[us_aqi], 1),
    TOPN(1, fact_readings, fact_readings[timestamp], DESC)
)
```

---

## 4. Build the Dashboard

Create a new report page and add these visuals:

### 4.1 KPI Cards (top row)
Add **Card** visuals for each measure:
- **Avg AQI** — format: 1 decimal, color: conditional (green < 100, red ≥ 200)
- **Avg PM2.5** — format: 1 decimal
- **% Unhealthy Hours** — format: percentage with 1 decimal
- **Max AQI** — format: whole number

### 4.2 City Comparison Bar Chart
- Visual: **Clustered Bar Chart**
- Y-axis: `dim_city[city_name]`
- X-axis: `[Avg AQI]`
- Sort: Descending by Avg AQI
- Conditional formatting: data bars green → red

### 4.3 Hourly AQI Trend Line
- Visual: **Line Chart**
- X-axis: `fact_readings[timestamp]`
- Y-axis: `[Avg AQI]`
- Legend: `dim_city[city_name]`
- Drill: by date → hour

### 4.4 Temperature vs PM2.5 Scatter
- Visual: **Scatter Chart**
- X-axis: `fact_readings[temperature]` (Average)
- Y-axis: `fact_readings[pm2_5]` (Average)
- Legend: `dim_city[city_name]`
- Size: `fact_readings[us_aqi]` (Average)

### 4.5 AQI Category Donut
- Visual: **Donut Chart**
- Legend: `fact_readings[aqi_category]`
- Values: Count of `fact_readings[reading_id]`
- Colors (US EPA Standard): Good=Green, Moderate=Yellow, Unhealthy for Sensitive Groups=Orange, Unhealthy=Red, Very Unhealthy=Purple, Hazardous=Maroon

### 4.6 India Map by City
- Visual: **Map** (or **Azure Map**)
- Location: `dim_city[city_name]`
- Latitude: `dim_city[latitude]`
- Longitude: `dim_city[longitude]`
- Size: `[Avg AQI]`
- Color: `[Avg AQI]` (diverging: green → red)

### 4.7 Slicers
Add **Slicer** visuals for:
- `dim_city[city_name]` — multi-select dropdown
- `dim_date[date]` — date range slider
- `fact_readings[aqi_category]` — dropdown

---

## 5. Formatting Tips

| Tip | How |
|-----|-----|
| Theme | Use a dark theme for impact — **View** → **Themes** → **Storm** or custom |
| Title | Add a text box: "🇮🇳 India Air Quality Dashboard" |
| Tooltips | Add `timestamp`, `temperature`, `humidity` to tooltip fields |
| Bookmarks | Create city-specific bookmarks for quick navigation |
| Refresh | After re-running the ETL, click **Home** → **Refresh** to pull updated CSVs |

---

## 6. Sample Layout

```
┌──────────┬──────────┬──────────┬──────────┐
│ Avg AQI  │ Avg PM2.5│ % Poor   │ Max AQI  │   ← KPI Cards
├──────────┴──────────┼──────────┴──────────┤
│                     │                     │
│  City Bar Chart     │   AQI Trend Line    │
│                     │                     │
├─────────────────────┼─────────────────────┤
│                     │                     │
│  Temp vs PM2.5      │   AQI Donut + Map   │
│  Scatter            │                     │
├─────────────────────┴─────────────────────┤
│  [City Slicer]  [Date Slicer]  [Category] │   ← Slicers
└───────────────────────────────────────────┘
```

---

## 7. Publishing (Optional)

1. **File** → **Publish** → **Publish to Power BI** (requires Power BI Pro).
2. Set up a **Scheduled Refresh** in the Power BI Service if you deploy the CSVs to a shared location.

---

> **Tip:** Keep the ETL running hourly (see README for scheduling) and simply hit **Refresh** in Power BI to see the latest data.
