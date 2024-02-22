WITH stg_dim_customer_buying_group__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, stg_dim_customer_buying_group__rename AS (
    SELECT
        buying_group_id AS buying_group_key
        , buying_group_name
    FROM `stg_dim_customer_buying_group__source`
)

, stg_dim_customer_buying_group__cast_type AS (
    SELECT
        CAST(buying_group_key AS INT) AS buying_group_key
        , CAST(buying_group_name AS STRING) AS buying_group_name
    FROM `stg_dim_customer_buying_group__rename`
)

, stg_dim_customer_buying_group__handle_null AS (
    SELECT
        buying_group_key
        , COALESCE(buying_group_name, 'Undefined') AS buying_group_name
    FROM `stg_dim_customer_buying_group__cast_type`
)



    SELECT
        buying_group_key
        , buying_group_name
    FROM `stg_dim_customer_buying_group__handle_null`
    UNION ALL
    SELECT 
        0
        , 'Undefined'

    UNION ALL
    SELECT 
        -1
        , 'Invalid'