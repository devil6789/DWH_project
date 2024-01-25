with dim_supplier__source AS (
  SELECT * FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename AS (
  SELECT supplier_id AS	supplier_key
         , supplier_name
  FROM `dim_supplier__source`       
)

, dim_supplier__cast_type AS (
  SELECT
    cast(supplier_key AS int) as supplier_key
    , cast(supplier_name as string) AS supplier_name
  FROM `dim_supplier__rename`  
)

, dim_supplier__add_undefined_invalid_value AS (
SELECT supplier_key
       , supplier_name
FROM `dim_supplier__cast_type`
UNION ALL 
select 0 as supplier_key, 'Undefined' AS supplier_name
UNION ALL 
select -1 as supplier_key, 'Invalid' AS supplier_name
)
SELECT supplier_key
       , supplier_name 
FROM `dim_supplier__add_undefined_invalid_value`
