WITH dim_supplier_category__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)

, dim_supplier_category__rename AS (
    SELECT
        supplier_category_id as supplier_category_key
        , supplier_category_name
    FROM `dim_supplier_category__source`
)

, dim_supplier_category__cast_type AS (
    SELECT
        CAST(supplier_category_key AS INT) AS supplier_category_key
        , CAST(supplier_category_name AS STRING) AS supplier_category_name
    FROM `dim_supplier_category__rename`
)

, dim_supplier_category__handle_null AS (
    SELECT
        supplier_category_key
        , COALESCE(supplier_category_name, 'Undefined') AS supplier_category_name
    FROM `dim_supplier_category__cast_type`
)
    SELECT 
        supplier_category_key
        , supplier_category_name
    FROM `dim_supplier_category__handle_null`
    UNION ALL
    SELECT 
        0
        , 'Undefined'

    UNION ALL
    SELECT 
        -1
        , 'Invalid'

