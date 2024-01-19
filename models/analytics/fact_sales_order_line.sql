with fact_sales_order_line__source AS (
  SELECT * from `vit-lam-data.wide_world_importers.sales__order_lines` 
)

, fact_sales_order_line__rename_column AS (
  SELECT
      order_line_id as	sales_order_line_key
    , stock_item_id  as product_key
    , quantity  as quantity
    , unit_price as unit_price
   FROM `fact_sales_order_line__source`
)

, fact_sales_order_line__cast_type AS (
  SELECT 
      cast(sales_order_line_key as int)  as	sales_order_line_key
      , cast(product_key as int ) as product_key
      , cast(quantity as integer) as quantity
      , cast(unit_price as numeric) as unit_price
  FROM `fact_sales_order_line__rename_column`
)

, fact_sales_order_line__add_gross_amount AS (
  SELECT 
      sales_order_line_key
      , product_key
      , quantity
      , unit_price
      , quantity * unit_price as gross_amount
  FROM `fact_sales_order_line__cast_type`     
)





SELECT 
    sales_order_line_key
    , product_key
    , quantity
    , unit_price
    , gross_amount
FROM `fact_sales_order_line__add_gross_amount`




