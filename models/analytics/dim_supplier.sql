WITH dim_supplier__source AS (
    SELECT *
    FROM {{ ref("stg_dim_supplier") }}
)

    SELECT 
        supplier_key
        , supplier_name
        , supplier_payment_days
        , supplier_category_key
        , supplier_category_name
        , delivery_method_key
        , delivery_method_name
    FROM `dim_supplier__source`