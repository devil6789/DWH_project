with dim_is_undersupply_backordered__source AS (
    SELECT 'TRUE' AS is_undersupply_backordered_boolean,'Undersupply Backordered' AS is_undersupply_backordered
    UNION ALL 
    SELECT 'FALSE' AS is_undersupply_backordered_boolean,'Not Undersupply Backordered' AS is_undersupply_backordered
)

    SELECT 
        FARM_FINGERPRINT(concat(dim_package_type.package_type_key, ',', dim_is_undersupply_backordered__source.is_undersupply_backordered)) AS sales_order_line_indicator_key
        , package_type_key	
        , package_type_name	
        , is_undersupply_backordered_boolean	
        , is_undersupply_backordered
    FROM {{ ref("dim_package_type") }} AS dim_package_type
    CROSS JOIN dim_is_undersupply_backordered__source
    
