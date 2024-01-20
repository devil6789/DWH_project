with dim_product__source AS  (
    SELECT * from `vit-lam-data.wide_world_importers.warehouse__stock_items`
    )

, dim_product__rename_column AS 
    (
      SELECT 
      stock_item_id  as	product_key
    , stock_item_name  as	product_name
    , supplier_id as supplier_key
    , brand  as brand_name
FROM `dim_product__source`
    )

, dim_product__casted AS 
    (
      SELECT 
        cast(product_key as INT)    as	product_key
        , cast(product_name as STRING) as	product_name
        , cast(supplier_key as INT) as supplier_key
        , cast(brand_name AS STRING) as brand_name
      FROM `dim_product__rename_column`  
    ) 

SELECT product_key
      , product_name
      , supplier_key
      , brand_name
from dim_product__casted 




