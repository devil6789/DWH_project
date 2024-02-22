WITH stg_dim_city__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__cities`
)

, stg_dim_city__rename AS (
    SELECT
        city_id AS city_key
        , city_name
        , state_province_id AS state_province_key      
    FROM `stg_dim_city__source`
)

, stg_dim_city__cast_type AS (
    SELECT
        CAST(city_key AS INT) AS city_key
        , CAST(city_name AS STRING) AS city_name
        , CAST(state_province_key AS INT) AS state_province_key
    FROM `stg_dim_city__rename`
)

, stg_dim_city__handle_null AS (
    SELECT
        dim_city.city_key
        , dim_city.city_name
        , dim_city.state_province_key
        , COALESCE(dim_state_province.state_province_name, 'Invalid') AS state_province_name          
        , COALESCE(dim_state_province.sales_territory, 'Invalid') AS sales_territory  
        , COALESCE(dim_state_province.country_key, -1) AS country_key  
        , COALESCE(dim_state_province.country_name, 'Invalid') AS country_name  
        , COALESCE(dim_state_province.formal_name, 'Invalid') AS formal_name  
        , COALESCE(dim_state_province.continent, 'Invalid') AS continent  
        , COALESCE(dim_state_province.region, 'Invalid') AS region  
        , COALESCE(dim_state_province.subregion, 'Invalid') AS subregion         
    FROM `stg_dim_city__cast_type` AS dim_city
      LEFT JOIN {{ ref("stg_dim_state_province") }} AS dim_state_province
        ON dim_city.state_province_key = dim_state_province.state_province_key
)

    SELECT
        city_key
        , city_name
        , state_province_key
        , state_province_name
        , sales_territory
        , country_key
        , country_name
        , formal_name
        , continent
        , region
        , subregion
    FROM `stg_dim_city__handle_null`

    