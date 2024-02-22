WITH dim_is_order_finalized__source AS (
    SELECT
        "true" AS is_order_finalized_boolean
        , "Order Finalized" AS is_order_finalized
    
    UNION ALL
    SELECT
        "false" AS is_order_finalized_boolean
        , "Not Order Finalized" AS is_order_finalized
)

, dim_is_order_line_finalized__source AS (
    SELECT
        "true" AS is_order_line_finalized_boolean
        , "Order Line Finalized" AS is_order_line_finalized
    
    UNION ALL
    SELECT
        "false" AS is_order_line_finalized_boolean
        , "Not Order Line Finalized" AS is_order_line_finalized

)

    SELECT
        FARM_FINGERPRINT(CONCAT(is_order_finalized, ", ", is_order_line_finalized)) AS purchase_order_line_indicator_key      
        , is_order_finalized_boolean
        , is_order_finalized
        , is_order_line_finalized_boolean
        , is_order_line_finalized
    FROM `dim_is_order_finalized__source`
    CROSS JOIN `dim_is_order_line_finalized__source`

    UNION ALL 
    SELECT
        0
        , 'Undefined'
        , 'Undefined'
        , 'Undefined'
        , 'Undefined'

    UNION ALL
    SELECT
        -1
        , 'Invalid'
        , 'Invalid'
        , 'Invalid'
        , 'Invalid'    