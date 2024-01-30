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
    FROM `dim_product__rename`    
)

SELECT * FROM dim_product__cast_type