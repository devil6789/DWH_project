WITH dim_sales_buying_group__source AS (
  SELECT * FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, dim_sales_buying_group__rename AS (
  SELECT buying_group_id as buying_group_key
        ,buying_group_name
  FROM `dim_sales_buying_group__source`
)

, dim_sales_buying_group__cast_type AS (
  SELECT cast(buying_group_key as int) as buying_group_key
        ,cast(buying_group_name as string) as buying_group_name
  FROM `dim_sales_buying_group__rename`
)

, stg_dim_buying_group__add_undefined_invalid_value AS (
select buying_group_key
      ,buying_group_name
FROM `dim_sales_buying_group__cast_type`
UNION ALL 
select 0 as buying_group_key, 'Undefined' AS buying_group_name
UNION ALL 
select -1 as buying_group_key, 'Invalid' AS buying_group_name
)

SELECT buying_group_key
      ,buying_group_name
FROM `stg_dim_buying_group__add_undefined_invalid_value`