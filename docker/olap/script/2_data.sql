COPY sc_vehicle (
    id,
    frame_path,
    frame_time,
    plate_bbox,
    vehicle_bbox,
    type_name,
    type_id,
    color_name,
    color_id,
    make,
    model,
    plate_text_latin,
    plate_text_arabic,
    mmc_confidence,
    plate_confidence,
    text_confidence,
    zone_id,
    camera_id,
    category_id,
    tenant_id,
    speed,
    dwell_time_seconds
)
FROM '/data/vehicle.csv'
DELIMITER ','
CSV HEADER;
