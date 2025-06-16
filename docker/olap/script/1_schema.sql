CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS sc_vehicle (
    id UUID default gen_random_uuid(),
    created_at TIMESTAMP default now(),
    frame_path TEXT NULL,
    frame_time TIMESTAMP WITH TIME ZONE NOT NULL,
    plate_bbox INTEGER[],
    vehicle_bbox INTEGER[],
    type_name VARCHAR(255),
    type_id INTEGER,
    color_name VARCHAR(50),
    color_id INTEGER,
    make VARCHAR(255),
    model VARCHAR(255),
    plate_text_latin VARCHAR(255),
    plate_text_arabic VARCHAR(255),
    mmc_confidence FLOAT,
    plate_confidence FLOAT,
    text_confidence FLOAT,
    zone_id UUID,
    camera_id UUID,
    category_id INTEGER,
    tenant_id UUID,
    speed DECIMAL(10, 2) NOT NULL,
    dwell_time_seconds FLOAT
);

-- ==========================================
-- TIMESCALEDB HYPERTABLE PARTITIONING
-- ==========================================

-- Create hypertable with time-based partitioning
-- 1-day chunks are optimal for your query patterns
SELECT create_hypertable('sc_vehicle', 'frame_time', chunk_time_interval => INTERVAL '1 day', if_not_exists  => TRUE);

-- ==========================================
-- CORE INDEXES FOR QUERY OPTIMIZATION
-- ==========================================

-- 1. PRIMARY TIME-BASED INDEXES
-- Essential for all time-range queries
CREATE INDEX IF NOT EXISTS idx_sc_vehicle_frame_time ON sc_vehicle (frame_time DESC);

-- 2. ZONE-BASED INDEXES
-- Critical for zone filtering and grouping
CREATE INDEX IF NOT EXISTS idx_sc_vehicle_zone_time ON sc_vehicle (zone_id, frame_time DESC);

-- 3. CAMERA-BASED INDEXES
-- For camera-specific queries
CREATE INDEX IF NOT EXISTS idx_sc_vehicle_camera_time ON sc_vehicle (camera_id, frame_time DESC);

-- 4. PLATE TEXT INDEXES
-- Composite index for plate analysis with time filtering
CREATE INDEX IF NOT EXISTS idx_sc_vehicle_plate_time ON sc_vehicle (plate_text_latin, frame_time DESC)
WHERE plate_text_latin IS NOT NULL AND plate_text_latin != '';

-- 5. CATEGORY-BASED INDEXES
-- For luxury/premium/economy analysis
CREATE INDEX IF NOT EXISTS idx_sc_vehicle_category_time ON sc_vehicle (category_id, frame_time DESC);

-- 6. TYPE-BASED INDEXES
-- For car type analysis
CREATE INDEX IF NOT EXISTS idx_sc_vehicle_type_time ON sc_vehicle (type_id, frame_time DESC);

-- 7. drop not null contraint on speed column
ALTER TABLE sc_vehicle
ALTER COLUMN speed DROP NOT NULL;
