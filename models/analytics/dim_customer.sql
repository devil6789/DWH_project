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
        , is_on_credit_hold as is_on_credit_hold_boolean
  FROM `dim_customer__source`       
)

, dim_customer__cast AS (
  select cast(customer_key  AS int) as customer_key
        , cast(customer_name as string) as customer_name
        , cast(customer_category_key as int) as customer_category_key
        , cast(buying_group_key as int) as buying_group_key
        , cast(is_on_credit_hold_boolean AS BOOLEAN) as is_on_credit_hold_boolean
  FROM `dim_customer__rename`
)

, dim_customer__handle_boolean AS (
  SELECT *
        , CASE
            WHEN is_on_credit_hold_boolean is TRUE then 'On Creadit Hold '
            WHEN is_on_credit_hold_boolean is FALSE then 'Not On Creadit Hold '
            WHEN is_on_credit_hold_boolean is NULL then 'Undefined'
            else 'Invalid'
          END as is_on_credit_hold
  FROM `dim_customer__cast`
)

SELECT dim_customer.customer_key
      , dim_customer.customer_name
      , dim_customer.customer_category_key
      , coalesce(stg_dim_category.customer_category_name, 'Undefined') AS customer_category_name 
      , dim_customer.buying_group_key
      , coalesce(stg_dim_buying_group.buying_group_name, 'Undefined') AS buying_group_name 
      , dim_customer.is_on_credit_hold
 from `dim_customer__handle_boolean` as dim_customer
  LEFT JOIN {{ ref('stg_dim_customer_category') }} as stg_dim_category
    ON dim_customer.customer_category_key = stg_dim_category.customer_category_key
  LEFT JOIN {{ ref("stg_dim_buying_group") }} as stg_dim_buying_group
    ON dim_customer.buying_group_key = stg_dim_buying_group.buying_group_key

