WITH dim_category__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.external__categories`
)

, dim_category__rename AS (
    SELECT
        category_id AS category_key
        , category_name
        , parent_category_id AS parent_category_key
        , category_level
    FROM `dim_category__source`
)

, dim_category__cast_type AS (
    SELECT
        CAST(category_key AS INT) AS category_key
        , CAST(category_name AS STRING) AS category_name
        , CAST(parent_category_key AS INT) AS parent_category_key
        , CAST(category_level AS INT) AS category_level        
    FROM `dim_category__rename`
)

, dim_category__handle_null AS (
    SELECT
        category_key
        , category_name
        , COALESCE(parent_category_key, 0) AS parent_category_key
        , COALESCE(category_level, 0) AS category_level
    FROM `dim_category__cast_type`
)

, dim_category__join AS (
    SELECT
        dim_category.category_key
        , dim_category.category_name
        , dim_category.parent_category_key
        , COALESCE(dim_category_parent.category_name, 'Undefined') AS parent_category_name
        , dim_category.category_level
    FROM `dim_category__handle_null` AS dim_category
    LEFT JOIN `dim_category__handle_null` AS dim_category_parent
    ON dim_category.parent_category_key = dim_category_parent.category_key
)

, dim_category__add_values_undefined_invalid AS (
    SELECT
        category_key
        , category_name
        , category_level
        , parent_category_key
        , parent_category_name
    FROM `dim_category__join`

    UNION ALL 
    SELECT
        0 AS category_key
        , 'Undefined' AS category_name
        , 0 AS category_level
        , 0 AS parent_category_key
        , 'Undefined' AS parent_category_name

    UNION ALL 
    SELECT
        -1 AS category_key
        , 'Invalid' AS category_name
        , -1 AS category_level
        , -1 AS parent_category_key
        , 'Invalid' AS parent_category_name
)

    SELECT
        category_key
        , category_name
        , category_level
        , parent_category_key
        , parent_category_name
    FROM `dim_category__add_values_undefined_invalid`