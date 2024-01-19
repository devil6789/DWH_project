with dim_product__source AS  (
    SELECT * from `vit-lam-data.wide_world_importers.warehouse__stock_items`
    )
, dim_product__rename_column AS 
    (
      SELECT 
      stock_item_id  as	product_key
    , stock_item_name  as	product_name
    , brand  as brand_name
FROM `dim_product__source`
    )
SELECT * from dim_product__rename_column




