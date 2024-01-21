with dim_customer__source AS (
  SELECT 
  *
FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename AS (
  SELECT customer_id AS	customer_key
        ,customer_name AS	customer_name
        , customer_category_id as customer_category_key
        , buying_group_id AS buying_group_key
  FROM `dim_customer__source`       
)

, dim_customer__cast AS (
  select cast(customer_key  AS int) as customer_key
        , cast(customer_name as string) as customer_name
        , cast(customer_category_key as int) as customer_category_key
        , cast(buying_group_key as int) as buying_group_key 
  FROM `dim_customer__rename`
)
SELECT dim_customer.customer_key
      , dim_customer.customer_name
      , dim_customer.customer_category_key
      , dim_customer.buying_group_key
 from `dim_customer__cast` as dim_customer
