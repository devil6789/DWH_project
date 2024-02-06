with dim_is_undersupply_backordered__source AS (
    SELECT 'TRUE' AS is_undersupply_backordered_boolean,'Undersupply Backordered' AS is_undersupply_backordered
    UNION ALL 
    SELECT 'FALSE' AS is_undersupply_backordered_boolean,'Not Undersupply Backordered' AS is_undersupply_backordered
)

    SELECT 
        FARM_FINGERPRINT(concat(dim_package_type.package_type_key, ',', dim_is_undersupply_backordered__source.is_undersupply_backordered)) AS sales_order_line_indicator_key
        , *
    FROM `learn-dwh-411512.wide_world_importers_dwh.dim_package_type` AS dim_package_type
    CROSS JOIN dim_is_undersupply_backordered__source
    
