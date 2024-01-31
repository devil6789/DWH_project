WITH dim_product__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename AS (
    SELECT
        stock_item_id AS product_key
        , stock_item_name AS product_name
        , brand AS brand_name
        , size AS product_size
        , quantity_per_outer
        , tax_rate
        , unit_price
        , recommended_retail_price
        , lead_time_days
        , supplier_id AS supplier_key
        , color_id AS colour_key
        , unit_package_id AS unit_package_key
        , outer_package_id AS outer_package_key
    FROM `dim_product__source`
)

, dim_product__cast_type AS (
    SELECT
        CAST(product_key AS INT) AS product_key 
        , CAST(product_name AS STRING) AS product_name
        , CAST(brand_name AS STRING) AS brand_name
        , CAST(product_size AS STRING) AS product_size 
        , CAST(quantity_per_outer AS INT) AS quantity_per_outer 
        , CAST(tax_rate AS NUMERIC) AS tax_rate
        , CAST(unit_price AS NUMERIC) AS unit_price
        , CAST(recommended_retail_price AS NUMERIC) AS recommended_retail_price
        , CAST(lead_time_days AS INT) AS lead_time_days 
        , CAST(supplier_key AS INT) AS supplier_key
        , CAST(colour_key AS INT) AS colour_key
        , CAST(unit_package_key AS INT) AS unit_package_key
        , CAST(outer_package_key AS INT) AS outer_package_key
    FROM `dim_product__rename`    
)

    SELECT
        dim_product.product_key
        , dim_product.product_name
        , dim_product.brand_name
        , dim_product.product_size
        , dim_product.quantity_per_outer
        , dim_product.tax_rate
        , dim_product.unit_price
        , dim_product.recommended_retail_price
        , dim_product.lead_time_days
        , dim_product.supplier_key
        , dim_supplier.supplier_name
        , dim_supplier.supplier_payment_days
        , dim_supplier.supplier_category_key
        , dim_supplier.supplier_category_name
        , dim_supplier.delivery_method_key
        , dim_supplier.delivery_method_name
        , dim_product.colour_key
        , dim_colour.colour_name
        , dim_product.unit_package_key AS unit_package_type_key
        , dim_unit_package_type.package_type_name AS unit_package_type_name
        , dim_product.outer_package_key AS outer_package_type_key
        , dim_outer_package_type.package_type_name AS outer_package_type_name
    FROM `dim_product__cast_type` AS dim_product
      LEFT JOIN {{ ref("stg_dim_supplier") }} AS dim_supplier
        ON dim_product.supplier_key = dim_supplier.supplier_key

      LEFT JOIN {{ ref("stg_dim_colour") }} AS dim_colour 
        ON dim_product.colour_key = dim_colour.colour_key

      LEFT JOIN {{ ref("dim_package_type") }} AS dim_unit_package_type
        ON dim_product.unit_package_key = dim_unit_package_type.package_type_key
        
      LEFT JOIN {{ ref("dim_package_type") }} AS dim_outer_package_type
        ON dim_product.outer_package_key = dim_outer_package_type.package_type_key
        