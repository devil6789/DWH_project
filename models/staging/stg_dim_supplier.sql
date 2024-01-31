WITH dim_supplier__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename AS (
    SELECT 
        supplier_id as supplier_key
        , supplier_name
        , payment_days as supplier_payment_days
    FROM `dim_supplier__source`
)

, dim_supplier__cast_type AS (
    SELECT
        CAST(supplier_key AS INT) AS supplier_key
        , CAST(supplier_name AS STRING) AS supplier_name
        , CAST(supplier_payment_days AS INT) AS supplier_payment_days
    FROM `dim_supplier__rename`
)

    SELECT *
    FROM `dim_supplier__cast_type`