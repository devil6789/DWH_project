with dim_is_undersupply_backordered__source AS (
    SELECT 'TRUE' AS is_undersupply_backordered_boolean,'Undersupply Backorder' AS is_undersupply_backordered
    UNION ALL 
    SELECT 'FALSE' AS is_undersupply_backordered_boolean,'Not Undersupply Backorder' AS is_undersupply_backordered
)

    SELECT 
        concat(package_type_key, ',', is_undersupply_backordered_boolean) AS sales_order_line_indicator_key
        , *
    FROM `learn-dwh-411512.wide_world_importers_dwh.dim_package_type`
    CROSS JOIN dim_is_undersupply_backordered__source
    ORDER BY 3,1
