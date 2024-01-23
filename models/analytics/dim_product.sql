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
    , is_chiller_stock
FROM `dim_product__source`
    )

, dim_product__casted AS 
    (
      SELECT 
        cast(product_key as INT)    as	product_key
        , cast(product_name as STRING) as	product_name
        , cast(supplier_key as INT) as supplier_key
        , cast(brand_name AS STRING) as brand_name
        , cast(is_chiller_stock as BOOLEAN) as is_chiller_stock
      FROM `dim_product__rename_column`  
    ) 

SELECT dim_product.product_key
      , dim_product.product_name
      , dim_product.supplier_key
      , dim_supplier.supplier_name
      , dim_product.brand_name
      , dim_product.is_chiller_stock
from dim_product__casted as dim_product
left join {{ ref('dim_supplier') }} as dim_supplier
on dim_product.supplier_key = dim_supplier.supplier_key




