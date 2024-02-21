WITH dim_delivery_method__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, dim_delivery_method__rename AS ( 
    SELECT
        delivery_method_id AS delivery_method_key
        , delivery_method_name
    FROM `dim_delivery_method__source`
)

, dim_delivery_method__cast_type AS (
    SELECT
        CAST(delivery_method_key AS INT) AS delivery_method_key
        , CAST(delivery_method_name AS STRING) AS delivery_method_name
    FROM `dim_delivery_method__rename`
)
    SELECT 
        delivery_method_key
        , delivery_method_name
    FROM `dim_delivery_method__cast_type`
    UNION ALL
    SELECT 
        0
        , 'Undefined'

    UNION ALL
    SELECT 
        -1
        , 'Invalid'