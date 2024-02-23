WITH dim_external_salesperson_target__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, dim_external_salesperson_target__rename AS (
    SELECT
        year_month
        , salesperson_person_id AS salesperson_person_key
        , target_revenue
    FROM `dim_external_salesperson_target__source`
)

, dim_external_salesperson_target__cast_type AS (
    SELECT
        CAST(year_month AS DATE) AS year_month         
        , CAST(salesperson_person_key AS INT) AS salesperson_person_key 
        , CAST(target_revenue AS NUMERIC) AS target_revenue 
    FROM `dim_external_salesperson_target__rename`
)

    SELECT
        year_month
        , salesperson_person_key
        , target_revenue
    FROM `dim_external_salesperson_target__cast_type`