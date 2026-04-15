
-- PostgreSQL 16 + PostGIS 3.4

-- Ensure PostGIS is enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================
-- RESORTS
-- Curated dataset of ski resorts across CA/CO/WA
-- ============================================
CREATE TABLE resorts (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(150) NOT NULL,
    state           VARCHAR(2) NOT NULL CHECK (state IN ('CA', 'CO', 'WA')),
    base_elevation_ft   INT,
    summit_elevation_ft INT,
    num_lifts       INT,
    website_url     VARCHAR(300),
    geom            GEOMETRY(Point, 4326) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_resorts_geom ON resorts USING GIST (geom);
CREATE INDEX idx_resorts_state ON resorts (state);

-- ============================================
-- SNOTEL STATIONS
-- NRCS monitoring stations across CA/CO/WA
-- ============================================
CREATE TABLE snotel_stations (
    id              SERIAL PRIMARY KEY,
    station_triplet VARCHAR(30) NOT NULL UNIQUE,  -- e.g. "784:CA:SNTL"
    name            VARCHAR(150) NOT NULL,
    elevation_ft    INT,
    state           VARCHAR(2) NOT NULL CHECK (state IN ('CA', 'CO', 'WA')),
    geom            GEOMETRY(Point, 4326) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stations_geom ON snotel_stations USING GIST (geom);
CREATE INDEX idx_stations_state ON snotel_stations (state);

-- ============================================
-- RESORT ↔ STATION LINKS
-- Spatial relationship between resorts and nearby stations
-- Computed via PostGIS distance + elevation similarity
-- ============================================
CREATE TABLE resort_station_links (
    id              SERIAL PRIMARY KEY,
    resort_id       INT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    station_id      INT NOT NULL REFERENCES snotel_stations(id) ON DELETE CASCADE,
    distance_miles  FLOAT NOT NULL,
    elevation_diff_ft INT NOT NULL,
    weight          FLOAT NOT NULL DEFAULT 1.0,  -- higher = more influence on resort score
    UNIQUE (resort_id, station_id)
);

CREATE INDEX idx_links_resort ON resort_station_links (resort_id);
CREATE INDEX idx_links_station ON resort_station_links (station_id);

-- ============================================
-- SNOW OBSERVATIONS
-- Daily SNOTEL readings ingested by ETL
-- ============================================
CREATE TABLE snow_observations (
    id              SERIAL PRIMARY KEY,
    station_id      INT NOT NULL REFERENCES snotel_stations(id) ON DELETE CASCADE,
    obs_date        DATE NOT NULL,
    snow_depth_in   FLOAT,
    swe_in          FLOAT,
    precip_accum_in FLOAT,
    temp_max_f      FLOAT,
    temp_min_f      FLOAT,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (station_id, obs_date)
);

CREATE INDEX idx_obs_station_date ON snow_observations (station_id, obs_date DESC);
CREATE INDEX idx_obs_date ON snow_observations (obs_date DESC);

-- ============================================
-- FORECASTS
-- NWS forecast data per resort
-- ============================================
CREATE TABLE forecasts (
    id                      SERIAL PRIMARY KEY,
    resort_id               INT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    forecast_date           DATE NOT NULL,
    projected_snowfall_in   FLOAT,
    temp_high_f             FLOAT,
    temp_low_f              FLOAT,
    wind_speed_mph          FLOAT,
    fetched_at              TIMESTAMP DEFAULT NOW(),
    UNIQUE (resort_id, forecast_date)
);

CREATE INDEX idx_forecasts_resort_date ON forecasts (resort_id, forecast_date DESC);

-- ============================================
-- RESORT CONDITIONS (ETL output — intelligence layer)
-- Computed composite scores per resort per day
-- ============================================
CREATE TABLE resort_conditions (
    id                      SERIAL PRIMARY KEY,
    resort_id               INT NOT NULL REFERENCES resorts(id) ON DELETE CASCADE,
    computed_date           DATE NOT NULL,
    current_snow_depth_in   FLOAT,
    snowfall_48h_in         FLOAT,
    snowfall_7d_in          FLOAT,
    swe_in                  FLOAT,
    snowpack_trend          VARCHAR(20) CHECK (snowpack_trend IN ('rising', 'stable', 'declining')),
    forecast_snowfall_72h_in FLOAT,
    temp_avg_f              FLOAT,
    condition_score         INT CHECK (condition_score BETWEEN 0 AND 100),
    score_explanation       TEXT,
    updated_at              TIMESTAMP DEFAULT NOW(),
    UNIQUE (resort_id, computed_date)
);

CREATE INDEX idx_conditions_resort_date ON resort_conditions (resort_id, computed_date DESC);
CREATE INDEX idx_conditions_score ON resort_conditions (condition_score DESC);

-- ============================================
-- ETL RUN LOG
-- Track pipeline execution for monitoring
-- ============================================
CREATE TABLE etl_runs (
    id              SERIAL PRIMARY KEY,
    job_name        VARCHAR(50) NOT NULL,  -- 'ingest_snotel', 'ingest_nws', 'compute_conditions', 'export_sync'
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMP,
    status          VARCHAR(20) DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed')),
    records_processed INT DEFAULT 0,
    error_message   TEXT,
    CONSTRAINT valid_timestamps CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE INDEX idx_etl_runs_job ON etl_runs (job_name, started_at DESC);
