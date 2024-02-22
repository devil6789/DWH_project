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

, dim_product__handle_null AS (
    SELECT 
        product_key
        , COALESCE(product_name, 'Invalid') AS product_name 
        , COALESCE(brand_name, 'Invalid') AS brand_name 
        , COALESCE(product_size, 'Invalid') AS product_size 
        , COALESCE(quantity_per_outer, 0) AS quantity_per_outer 
        , COALESCE(tax_rate, 0) AS tax_rate 
        , COALESCE(unit_price, 0) AS unit_price 
        , COALESCE(recommended_retail_price, 0) AS recommended_retail_price 
        , COALESCE(lead_time_days, 0) AS lead_time_days 
        , COALESCE(supplier_key, 0) AS supplier_key 
        , COALESCE(colour_key, 0) AS colour_key 
        , COALESCE(unit_package_key, 0) AS unit_package_key 
        , COALESCE(outer_package_key, 0) AS outer_package_key 
    FROM `dim_product__cast_type`
)

, dim_product__join AS (
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
        , COALESCE(dim_supplier.supplier_name, 'Invalid') AS supplier_name 
        , COALESCE(dim_supplier.supplier_payment_days, -1) AS supplier_payment_days 
        , COALESCE(dim_supplier.supplier_category_key, -1) AS supplier_category_key 
        , COALESCE(dim_supplier.supplier_category_name, 'Invalid') AS supplier_category_name 
        , COALESCE(dim_supplier.delivery_method_key, -1) AS delivery_method_key 
        , COALESCE(dim_supplier.delivery_method_name, 'Invalid') AS delivery_method_name 
        , dim_product.colour_key
        , COALESCE(dim_colour.colour_name, 'Invalid') AS colour_name
        , dim_product.unit_package_key AS unit_package_type_key
        , COALESCE(dim_unit_package_type.package_type_name, 'Invalid') AS unit_package_type_name
        , dim_product.outer_package_key AS outer_package_type_key
        , COALESCE(dim_outer_package_type.package_type_name, 'Invalid') AS outer_package_type_name
    FROM `dim_product__handle_null` AS dim_product
      LEFT JOIN {{ ref("stg_dim_supplier") }} AS dim_supplier
        ON dim_product.supplier_key = dim_supplier.supplier_key

      LEFT JOIN {{ ref("stg_dim_colour") }} AS dim_colour 
        ON dim_product.colour_key = dim_colour.colour_key

      LEFT JOIN {{ ref("dim_package_type") }} AS dim_unit_package_type
        ON dim_product.unit_package_key = dim_unit_package_type.package_type_key
        
      LEFT JOIN {{ ref("dim_package_type") }} AS dim_outer_package_type
        ON dim_product.outer_package_key = dim_outer_package_type.package_type_key
)

, dim_product__add_undefined_invalid AS (
    SELECT
        product_key
        , product_name
        , brand_name
        , product_size
        , quantity_per_outer
        , tax_rate
        , unit_price
        , recommended_retail_price
        , lead_time_days
        , supplier_key
        , supplier_name
        , supplier_payment_days
        , supplier_category_key
        , supplier_category_name
        , delivery_method_key
        , delivery_method_name
        , colour_key
        , colour_name
        , unit_package_type_key
        , unit_package_type_name
        , outer_package_type_key
        , outer_package_type_name
    FROM `dim_product__join`

    UNION ALL 
    SELECT 
        0
        , 'Undefined'
        , 'Undefined'
        , 'Undefined'
        , 0
        , 0
        , 0
        , 0
        , 0
        , 0
        , 'Undefined'
        , 0
        , 0
        , 'Undefined'
        , 0
        , 'Undefined'
        , 0
        , 'Undefined'
        , 0
        , 'Undefined'
        , 0
        , 'Undefined'

    UNION ALL 
    SELECT 
        -1
        , 'Invalid'
        , 'Invalid'
        , 'Invalid'
        , -1
        , -1
        , -1
        , -1
        , -1
        , -1
        , 'Invalid'
        , -1
        , -1
        , 'Invalid'
        , -1
        , 'Invalid'
        , -1
        , 'Invalid'
        , -1
        , 'Invalid'
        , -1
        , 'Invalid'
)

    SELECT
        product_key
        , product_name
        , brand_name
        , product_size
        , quantity_per_outer
        , tax_rate
        , unit_price
        , recommended_retail_price
        , lead_time_days
        , supplier_key
        , supplier_name
        , supplier_payment_days
        , supplier_category_key
        , supplier_category_name
        , delivery_method_key
        , delivery_method_name
        , colour_key
        , colour_name
        , unit_package_type_key
        , unit_package_type_name
        , outer_package_type_key
        , outer_package_type_name
    FROM `dim_product__add_undefined_invalid`

    
    
        