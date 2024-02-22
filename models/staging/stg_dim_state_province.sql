WITH stg_dim_state_province__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, stg_dim_state_province__rename AS (
    SELECT
        state_province_id AS state_province_key
        , state_province_name
        , sales_territory
        , country_id AS country_key
    FROM `stg_dim_state_province__source`
)

, stg_dim_state_province__cast_type AS (
    SELECT
        CAST(state_province_key AS INT) AS state_province_key         
        , CAST(state_province_name AS STRING) AS state_province_name 
        , CAST(sales_territory AS STRING) AS sales_territory  
        , CAST(country_key AS INT) AS country_key         
    FROM `stg_dim_state_province__rename`
)

, stg_dim_state_province__handle_null AS (
    SELECT
        state_province_key
        , COALESCE(state_province_name, 'Undefined') AS state_province_name 
        , COALESCE(sales_territory, 'Undefined') AS sales_territory 
        , COALESCE(country_key, 0) AS country_key 
    FROM `stg_dim_state_province__cast_type`
)

, stg_dim_state_province__join AS (
    SELECT
        dim_state_province.state_province_key
        , dim_state_province.state_province_name
        , dim_state_province.sales_territory
        , dim_state_province.country_key
        , COALESCE(dim_country.country_name, 'Invalid') AS country_name
        , COALESCE(dim_country.formal_name, 'Invalid') AS formal_name
        , COALESCE(dim_country.continent, 'Invalid') AS continent
        , COALESCE(dim_country.region, 'Invalid') AS region
        , COALESCE(dim_country.subregion, 'Invalid') AS subregion
    FROM `stg_dim_state_province__handle_null` AS dim_state_province
      LEFT JOIN {{ ref("stg_dim_country") }} AS dim_country
        ON dim_state_province.country_key = dim_country.country_key
)

    SELECT
        state_province_key
        , state_province_name
        , sales_territory
        , country_key
        , country_name
        , formal_name
        , continent
        , region
        , subregion
    FROM `stg_dim_state_province__join`


    