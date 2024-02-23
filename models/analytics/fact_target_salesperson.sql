WITH fact_target_salesperson__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_target_salesperson__rename AS (
    SELECT
        year_month
        , salesperson_person_id AS salesperson_person_key
        , target_revenue
    FROM `fact_target_salesperson__source`
)

, fact_target_salesperson__cast_type AS (
    SELECT
        CAST(year_month AS DATE) AS year_month         
        , CAST(salesperson_person_key AS INT) AS salesperson_person_key 
        , CAST(target_revenue AS NUMERIC) AS target_revenue 
    FROM `fact_target_salesperson__rename`
)

    SELECT
        year_month
        , salesperson_person_key
        , target_revenue
    FROM `fact_target_salesperson__cast_type`