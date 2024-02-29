WITH stg_dim_product__external AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.external__stock_item`
)

, stg_dim_product__rename AS (
    SELECT
        stock_item_id AS product_key
        , category_id AS category_key
    FROM `stg_dim_product__external`
)

, stg_dim_product__cast_type AS (
    SELECT
        CAST(product_key AS INT) AS product_key 
        , CAST(category_key AS INT) AS category_key 
    FROM `stg_dim_product__rename`
)

    SELECT
        product_key
        , category_key
    FROM `stg_dim_product__cast_type`