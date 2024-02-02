with dim_is_undersupply_backorder AS (
    SELECT 1 AS is_undersupply_backorder_boolean,'Undersupply Backorder' AS is_undersupply_backorder
    UNION ALL 
    SELECT 2 AS is_undersupply_backorder_boolean,'Not Undersupply Backorder' AS is_undersupply_backorder
)

    SELECT *
    FROM `learn-dwh-411512.wide_world_importers_dwh.dim_package_type`
    CROSS JOIN dim_is_undersupply_backorder
    ORDER BY 3,1
