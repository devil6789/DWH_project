WITH dim_sales_customer_category__source AS (
  SELECT * FROM `vit-lam-data.wide_world_importers.sales__customer_categories`
)

, dim_sales_customer_category__rename AS (
  SELECT customer_category_id as customer_category_key
         , customer_category_name
  FROM `dim_sales_customer_category__source`
)

, dim_sales_customer_category__cast_type AS (
  SELECT cast(customer_category_key as int) AS customer_category_key
        , cast(customer_category_name as string) AS customer_category_name
  FROM `dim_sales_customer_category__rename`
)

SELECT customer_category_key
       , customer_category_name
From `dim_sales_customer_category__cast_type`
