WITH dim_supplier__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename AS (
    SELECT 
        supplier_id as supplier_key
        , supplier_name
        , payment_days as supplier_payment_days
        , supplier_category_id as supplier_category_key
    FROM `dim_supplier__source`
)

, dim_supplier__cast_type AS (
    SELECT
        CAST(supplier_key AS INT) AS supplier_key
        , CAST(supplier_name AS STRING) AS supplier_name
        , CAST(supplier_payment_days AS INT) AS supplier_payment_days
        , CAST(supplier_category_key AS INT) AS supplier_category_key

    FROM `dim_supplier__rename`
)

    SELECT 
        dim_supplier.supplier_key
        , dim_supplier.supplier_name
        , dim_supplier.supplier_payment_days
        , dim_supplier.supplier_category_key
        , dim_supplier_category.supplier_category_name
    FROM `dim_supplier__cast_type` AS dim_supplier
    LEFT JOIN {{ ref("stg_dim_supplier_category") }} as dim_supplier_category
    ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key