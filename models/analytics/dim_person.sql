WITH dim_person__source AS (
  SELECT 
  *
FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename AS (
  SELECT person_id AS	person_key
         , full_name
  FROM `dim_person__source`
)

, dim_person__cast_type AS (
  SELECT cast(person_key as int) as person_key
         , cast(full_name as STRING) AS full_name
  FROM `dim_person__rename`
)

SELECT  
    person_key
    , full_name
FROM `dim_person__cast_type`

