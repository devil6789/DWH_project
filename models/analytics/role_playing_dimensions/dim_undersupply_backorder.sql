    SELECT
        "true" AS is_undersupply_backorder_key
        , "Undersupply Backordered" AS is_undersupply_backorder_name
    
    UNION ALL
    SELECT
        "false" AS is_undersupply_backorder_key
        , "Not Undersupply Backordered" AS is_undersupply_backorder_name
        