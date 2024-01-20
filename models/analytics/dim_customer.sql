with dim_customer__source AS (
  SELECT 
  *
FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename AS (
  SELECT customer_id AS	customer_key
        ,customer_name AS	customer_name
  FROM `dim_customer__source`       
)

, dim_customer__cast AS (
  select cast(customer_key  AS int) as customer_key
        , cast(customer_name as string) as customer_name
  FROM `dim_customer__rename`
)
SELECT customer_key
      , customer_name
 from `dim_customer__cast`
