-- ============================================================================
-- Business Analytics Queries - NYC Citibike
-- ============================================================================

-- Query 1: Daily Revenue Trend
-- Purpose: Track revenue performance over time
SELECT 
    d.date,
    d.day_of_week,
    d.is_weekend,
    COUNT(DISTINCT f.trip_sk) AS total_trips,
    SUM(f.trip_amount) AS total_revenue,
    AVG(f.trip_amount) AS avg_revenue_per_trip,
    AVG(f.trip_duration) / 60 AS avg_duration_minutes
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
JOIN CITIBIKE_PROD.GOLD.DIM_DATE d ON f.start_date_fk = d.date_sk
WHERE d.date >= DATEADD(month, -3, CURRENT_DATE())
GROUP BY d.date, d.day_of_week, d.is_weekend
ORDER BY d.date DESC;

-- Query 2: Top 20 Stations by Trip Volume
-- Purpose: Identify high-traffic stations for capacity planning
SELECT 
    s.station_name,
    s.region_id,
    COUNT(DISTINCT f.trip_sk) AS total_trips,
    SUM(f.trip_amount) AS total_revenue,
    AVG(f.trip_duration) / 60 AS avg_duration_minutes,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS popularity_rank
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
JOIN CITIBIKE_PROD.GOLD.DIM_STATION s ON f.start_station_fk = s.station_sk
WHERE f.start_time >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY s.station_name, s.region_id
ORDER BY total_trips DESC
LIMIT 20;

-- Query 3: User Type Comparison
-- Purpose: Analyze behavior differences between customers and subscribers
SELECT 
    u.user_type,
    u.membership_type,
    COUNT(DISTINCT f.trip_sk) AS trip_count,
    SUM(f.trip_amount) AS total_revenue,
    AVG(f.trip_amount) AS avg_trip_cost,
    AVG(f.trip_duration) / 60 AS avg_duration_minutes,
    AVG(f.trip_distance_km) AS avg_distance_km,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.trip_duration) AS median_duration
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
JOIN CITIBIKE_PROD.GOLD.DIM_USER_TYPE u ON f.user_type_fk = u.user_type_sk
JOIN CITIBIKE_PROD.GOLD.DIM_DATE d ON f.start_date_fk = d.date_sk
WHERE d.date >= DATEADD(month, -1, CURRENT_DATE())
GROUP BY u.user_type, u.membership_type;

-- Query 4: Peak Usage Hours
-- Purpose: Identify demand patterns for operational planning
SELECT 
    EXTRACT(HOUR FROM f.start_time) AS hour_of_day,
    d.day_of_week,
    d.is_weekend,
    COUNT(*) AS trip_count,
    AVG(f.trip_duration) / 60 AS avg_duration_minutes,
    SUM(f.trip_amount) AS total_revenue
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
JOIN CITIBIKE_PROD.GOLD.DIM_DATE d ON f.start_date_fk = d.date_sk
WHERE f.start_time >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY EXTRACT(HOUR FROM f.start_time), d.day_of_week, d.is_weekend
ORDER BY hour_of_day, d.day_of_week;

-- Query 5: Station-to-Station Flow Analysis
-- Purpose: Understand trip patterns between stations
SELECT 
    s_start.station_name AS start_station,
    s_end.station_name AS end_station,
    COUNT(*) AS trip_count,
    AVG(f.trip_duration) / 60 AS avg_duration_minutes,
    AVG(f.trip_distance_km) AS avg_distance_km,
    SUM(f.trip_amount) AS total_revenue
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
JOIN CITIBIKE_PROD.GOLD.DIM_STATION s_start ON f.start_station_fk = s_start.station_sk
JOIN CITIBIKE_PROD.GOLD.DIM_STATION s_end ON f.end_station_fk = s_end.station_sk
WHERE f.start_time >= DATEADD(month, -1, CURRENT_DATE())
GROUP BY s_start.station_name, s_end.station_name
HAVING COUNT(*) > 50
ORDER BY trip_count DESC
LIMIT 50;

-- Query 6: Monthly Revenue by Region
-- Purpose: Regional performance analysis
SELECT 
    DATE_TRUNC('month', d.date) AS month,
    s.region_id,
    COUNT(DISTINCT f.trip_sk) AS total_trips,
    SUM(f.trip_amount) AS total_revenue,
    AVG(f.trip_amount) AS avg_revenue_per_trip,
    SUM(f.trip_amount) / COUNT(DISTINCT d.date) AS avg_daily_revenue
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
JOIN CITIBIKE_PROD.GOLD.DIM_DATE d ON f.start_date_fk = d.date_sk
JOIN CITIBIKE_PROD.GOLD.DIM_STATION s ON f.start_station_fk = s.station_sk
WHERE d.year = YEAR(CURRENT_DATE())
GROUP BY DATE_TRUNC('month', d.date), s.region_id
ORDER BY month DESC, total_revenue DESC;

-- Query 7: Weekend vs Weekday Analysis
-- Purpose: Compare usage patterns
SELECT 
    d.is_weekend,
    COUNT(DISTINCT f.trip_sk) AS total_trips,
    SUM(f.trip_amount) AS total_revenue,
    AVG(f.trip_duration) / 60 AS avg_duration_minutes,
    AVG(f.trip_distance_km) AS avg_distance_km,
    COUNT(DISTINCT f.bike_id) AS unique_bikes_used
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
JOIN CITIBIKE_PROD.GOLD.DIM_DATE d ON f.start_date_fk = d.date_sk
WHERE d.date >= DATEADD(month, -1, CURRENT_DATE())
GROUP BY d.is_weekend;

-- Query 8: Bike Utilization Analysis
-- Purpose: Identify underutilized or overutilized bikes
SELECT 
    f.bike_id,
    COUNT(*) AS trip_count,
    SUM(f.trip_duration) / 3600 AS total_hours_used,
    AVG(f.trip_duration) / 60 AS avg_trip_duration_minutes,
    SUM(f.trip_amount) AS total_revenue_generated,
    MIN(f.start_time) AS first_trip,
    MAX(f.start_time) AS last_trip
FROM CITIBIKE_PROD.GOLD.FACT_TRIPS f
WHERE f.start_time >= DATEADD(month, -1, CURRENT_DATE())
GROUP BY f.bike_id
HAVING COUNT(*) > 10
ORDER BY trip_count DESC
LIMIT 100;
