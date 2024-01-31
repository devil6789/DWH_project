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
    SELECT 
        supplier_category_key
        , supplier_category_name
    FROM `dim_supplier_category__cast_type`