WITH stg_dim_country__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__countries`
)

, stg_dim_country__rename AS (
    SELECT
        country_id AS country_key
        , country_name
        , formal_name
        , continent
        , region
        , subregion
    FROM `stg_dim_country__source`
)

, stg_dim_country__cast_type AS (
    SELECT
        CAST(country_key AS INT) AS country_key         
        , CAST(country_name AS STRING) AS country_name 
        , CAST(formal_name AS STRING) AS formal_name 
        , CAST(continent AS STRING) AS continent 
        , CAST(region AS STRING) AS region
        , CAST(subregion AS STRING) AS subregion         
    FROM `stg_dim_country__rename`
)

    SELECT
        country_key
        , country_name
        , formal_name
        , continent
        , region
        , subregion
    FROM `stg_dim_country__cast_type`