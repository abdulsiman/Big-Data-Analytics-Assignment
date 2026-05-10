-- ============================================================
-- HIVE QUERIES FOR NOISE POLLUTION TREND ANALYSIS
-- ============================================================

-- 1. CREATE EXTERNAL TABLE FOR NOISE DATA IN HDFS
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS noise_sensor_data (
    timestamp STRING,
    participant_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    noise_level_db DOUBLE,
    noise_category STRING,
    location_type STRING,
    noise_kind STRING,
    hour INT,
    day_of_week INT,
    geo_zone STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hadoop/noise_pollution/raw_data'
TBLPROPERTIES ('skip.header.line.count'='1');


-- 2. CREATE AGGREGATED STATISTICS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS noise_statistics_hourly (
    geo_zone STRING,
    hour INT,
    avg_noise_db DOUBLE,
    max_noise_db DOUBLE,
    min_noise_db DOUBLE,
    std_dev_db DOUBLE,
    sample_count BIGINT,
    noise_category_distribution MAP<STRING, BIGINT>
)
PARTITIONED BY (date_partition STRING)
STORED AS PARQUET;


-- 3. QUERY: CITYWIDE NOISE TRENDS BY HOUR
-- ============================================================

SELECT 
    hour,
    COUNT(*) as measurement_count,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(STDDEV(noise_level_db), 2) as std_dev_db,
    ROUND(MIN(noise_level_db), 2) as min_noise_db,
    ROUND(MAX(noise_level_db), 2) as max_noise_db,
    ROUND(PERCENTILE_APPROX(noise_level_db, 0.5), 2) as median_noise_db,
    ROUND(PERCENTILE_APPROX(noise_level_db, 0.95), 2) as p95_noise_db
FROM noise_sensor_data
WHERE noise_level_db IS NOT NULL
GROUP BY hour
ORDER BY hour;


-- 4. QUERY: NOISE HOTSPOT IDENTIFICATION
-- ============================================================

SELECT 
    geo_zone,
    COUNT(*) as measurement_count,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(MAX(noise_level_db), 2) as max_noise_db,
    SUM(CASE WHEN noise_level_db > 80 THEN 1 ELSE 0 END) as extreme_noise_count,
    ROUND(100.0 * SUM(CASE WHEN noise_level_db > 80 THEN 1 ELSE 0 END) / COUNT(*), 2) as extreme_noise_pct
FROM noise_sensor_data
WHERE geo_zone IS NOT NULL
GROUP BY geo_zone
HAVING COUNT(*) >= 10  -- Minimum sample size
ORDER BY avg_noise_db DESC
LIMIT 20;


-- 5. QUERY: WEEKDAY VS WEEKEND COMPARISON
-- ============================================================

SELECT 
    CASE 
        WHEN day_of_week IN (5, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as period_type,
    COUNT(*) as measurement_count,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(STDDEV(noise_level_db), 2) as std_dev_db,
    ROUND(MIN(noise_level_db), 2) as min_noise_db,
    ROUND(MAX(noise_level_db), 2) as max_noise_db
FROM noise_sensor_data
WHERE noise_level_db IS NOT NULL
GROUP BY 
    CASE 
        WHEN day_of_week IN (5, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END;


-- 6. QUERY: LOCATION TYPE ANALYSIS
-- ============================================================

SELECT 
    location_type,
    COUNT(*) as measurement_count,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(STDDEV(noise_level_db), 2) as std_dev_db,
    ROUND(MIN(noise_level_db), 2) as min_noise_db,
    ROUND(MAX(noise_level_db), 2) as max_noise_db,
    COLLECT_SET(noise_category)[0] as most_common_category
FROM noise_sensor_data
WHERE location_type IS NOT NULL 
  AND location_type != ''
  AND noise_level_db IS NOT NULL
GROUP BY location_type
ORDER BY avg_noise_db DESC;


-- 7. QUERY: PEAK NOISE HOURS BY LOCATION
-- ============================================================

SELECT 
    location_type,
    hour,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    COUNT(*) as measurement_count
FROM noise_sensor_data
WHERE location_type IS NOT NULL 
  AND location_type != ''
  AND noise_level_db IS NOT NULL
GROUP BY location_type, hour
ORDER BY location_type, avg_noise_db DESC;


-- 8. QUERY: TEMPORAL TRENDS - MONTHLY AGGREGATION
-- ============================================================

SELECT 
    SUBSTRING(timestamp, 1, 7) as month,
    COUNT(*) as measurement_count,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(STDDEV(noise_level_db), 2) as std_dev_db,
    SUM(CASE WHEN noise_level_db > 70 THEN 1 ELSE 0 END) as high_noise_count
FROM noise_sensor_data
WHERE noise_level_db IS NOT NULL
GROUP BY SUBSTRING(timestamp, 1, 7)
ORDER BY month;


-- 9. QUERY: NOISE CATEGORY DISTRIBUTION
-- ============================================================

SELECT 
    noise_category,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(MIN(noise_level_db), 2) as min_noise_db,
    ROUND(MAX(noise_level_db), 2) as max_noise_db
FROM noise_sensor_data
WHERE noise_category IS NOT NULL
GROUP BY noise_category
ORDER BY count DESC;


-- 10. QUERY: GEOGRAPHIC CORRELATION ANALYSIS
-- ============================================================

SELECT 
    geo_zone,
    hour,
    day_of_week,
    COUNT(*) as sample_size,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(STDDEV(noise_level_db), 2) as std_dev_db
FROM noise_sensor_data
WHERE geo_zone IS NOT NULL
  AND noise_level_db IS NOT NULL
GROUP BY geo_zone, hour, day_of_week
HAVING COUNT(*) >= 5
ORDER BY avg_noise_db DESC
LIMIT 100;


-- 11. QUERY: CREATE SUMMARY VIEW FOR DASHBOARDS
-- ============================================================

CREATE VIEW IF NOT EXISTS noise_pollution_summary AS
SELECT 
    geo_zone,
    location_type,
    hour,
    day_of_week,
    CASE 
        WHEN day_of_week IN (5, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as total_measurements,
    ROUND(AVG(noise_level_db), 2) as avg_noise_db,
    ROUND(STDDEV(noise_level_db), 2) as std_dev_db,
    ROUND(MIN(noise_level_db), 2) as min_noise_db,
    ROUND(MAX(noise_level_db), 2) as max_noise_db,
    ROUND(PERCENTILE_APPROX(noise_level_db, 0.5), 2) as median_noise_db,
    SUM(CASE WHEN noise_level_db > 80 THEN 1 ELSE 0 END) as extreme_noise_events,
    SUM(CASE WHEN noise_level_db < 50 THEN 1 ELSE 0 END) as quiet_events
FROM noise_sensor_data
WHERE noise_level_db IS NOT NULL
GROUP BY geo_zone, location_type, hour, day_of_week;


-- 12. QUERY: EXPORT DATA FOR MONGODB
-- ============================================================

INSERT OVERWRITE DIRECTORY '/user/hadoop/noise_pollution/mongodb_export'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT 
    geo_zone,
    location_type,
    hour,
    day_of_week,
    AVG(noise_level_db) as avg_noise_db,
    STDDEV(noise_level_db) as std_dev_db,
    COUNT(*) as sample_count,
    MAX(noise_level_db) as max_noise_db,
    MIN(noise_level_db) as min_noise_db
FROM noise_sensor_data
WHERE noise_level_db IS NOT NULL
GROUP BY geo_zone, location_type, hour, day_of_week;
