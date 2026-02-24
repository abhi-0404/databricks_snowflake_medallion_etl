-- ============================================================================
-- Snowflake DDL - Star Schema for Citibike Analytics
-- ============================================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS CITIBIKE_PROD.RAW;
CREATE SCHEMA IF NOT EXISTS CITIBIKE_PROD.SILVER;
CREATE SCHEMA IF NOT EXISTS CITIBIKE_PROD.GOLD;
CREATE SCHEMA IF NOT EXISTS CITIBIKE_PROD.ANALYTICS;

-- ============================================================================
-- GOLD LAYER - Dimension Tables
-- ============================================================================

-- Dimension: Station
CREATE OR REPLACE TABLE CITIBIKE_PROD.GOLD.DIM_STATION (
    station_sk INT PRIMARY KEY,
    station_id STRING UNIQUE NOT NULL,
    station_name STRING,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    capacity INT,
    region_id STRING,
    is_active BOOLEAN DEFAULT TRUE,
    effective_date DATE DEFAULT CURRENT_DATE(),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (region_id, is_active)
COMMENT = 'Dimension table for bike stations - SCD Type 1';

-- Dimension: Date
CREATE OR REPLACE TABLE CITIBIKE_PROD.GOLD.DIM_DATE (
    date_sk INT PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (year, month)
COMMENT = 'Date dimension table with calendar attributes';

-- Dimension: User Type
CREATE OR REPLACE TABLE CITIBIKE_PROD.GOLD.DIM_USER_TYPE (
    user_type_sk INT PRIMARY KEY,
    user_type STRING UNIQUE NOT NULL,
    membership_type STRING,
    description STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'User type dimension - static reference data';

-- ============================================================================
-- GOLD LAYER - Fact Table
-- ============================================================================

CREATE OR REPLACE TABLE CITIBIKE_PROD.GOLD.FACT_TRIPS (
    trip_sk BIGINT PRIMARY KEY,
    start_station_fk INT NOT NULL,
    end_station_fk INT NOT NULL,
    start_date_fk INT NOT NULL,
    end_date_fk INT NOT NULL,
    user_type_fk INT NOT NULL,
    trip_duration INT NOT NULL,
    trip_distance_km DECIMAL(10,2),
    trip_amount DECIMAL(10,2) NOT NULL,
    bike_id STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    stop_time TIMESTAMP NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign key constraints
    CONSTRAINT fk_start_station FOREIGN KEY (start_station_fk) 
        REFERENCES CITIBIKE_PROD.GOLD.DIM_STATION(station_sk),
    CONSTRAINT fk_end_station FOREIGN KEY (end_station_fk) 
        REFERENCES CITIBIKE_PROD.GOLD.DIM_STATION(station_sk),
    CONSTRAINT fk_start_date FOREIGN KEY (start_date_fk) 
        REFERENCES CITIBIKE_PROD.GOLD.DIM_DATE(date_sk),
    CONSTRAINT fk_end_date FOREIGN KEY (end_date_fk) 
        REFERENCES CITIBIKE_PROD.GOLD.DIM_DATE(date_sk),
    CONSTRAINT fk_user_type FOREIGN KEY (user_type_fk) 
        REFERENCES CITIBIKE_PROD.GOLD.DIM_USER_TYPE(user_type_sk)
)
CLUSTER BY (start_date_fk, start_station_fk)
COMMENT = 'Fact table containing all bike trips with calculated metrics';

-- ============================================================================
-- Indexes for Performance
-- ============================================================================

-- Enable search optimization for high-cardinality columns
ALTER TABLE CITIBIKE_PROD.GOLD.FACT_TRIPS 
    ADD SEARCH OPTIMIZATION ON EQUALITY(bike_id, trip_sk);

-- ============================================================================
-- Table Statistics
-- ============================================================================

-- Gather statistics for query optimization
ALTER TABLE CITIBIKE_PROD.GOLD.FACT_TRIPS RESUME RECLUSTER;
ALTER TABLE CITIBIKE_PROD.GOLD.DIM_STATION RESUME RECLUSTER;
