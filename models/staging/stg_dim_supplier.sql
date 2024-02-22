WITH dim_supplier__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename AS (
    SELECT 
        supplier_id AS supplier_key
        , supplier_name
        , payment_days AS supplier_payment_days
        , supplier_category_id AS supplier_category_key
        , delivery_method_id AS delivery_method_key
    FROM `dim_supplier__source`
)

, dim_supplier__cast_type AS (
    SELECT
        CAST(supplier_key AS INT) AS supplier_key
        , CAST(supplier_name AS STRING) AS supplier_name
        , CAST(supplier_payment_days AS INT) AS supplier_payment_days
        , CAST(supplier_category_key AS INT) AS supplier_category_key
        , CAST(delivery_method_key AS INT) AS delivery_method_key
    FROM `dim_supplier__rename`
)

    SELECT 
        dim_supplier.supplier_key
        , dim_supplier.supplier_name
        , dim_supplier.supplier_payment_days
        , dim_supplier.supplier_category_key
        , COALESCE(dim_supplier_category.supplier_category_name, 'Invalid') AS supplier_category_name
        , delivery_method_key
        , COALESCE(dim_delivery_method.delivery_method_name, 'Invalid') AS delivery_method_name
    FROM `dim_supplier__cast_type` AS dim_supplier
      LEFT JOIN {{ ref("stg_dim_supplier_category") }} AS dim_supplier_category
        ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key

      LEFT JOIN {{ ref("stg_dim_delivery_method") }} AS dim_delivery_method
        ON dim_supplier.delivery_method_key = dim_delivery_method.delivery_method_key