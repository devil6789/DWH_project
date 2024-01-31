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

    SELECT
        dim_city.city_key
        , dim_city.city_name
        , dim_city.state_province_key
        , dim_state_province.state_province_name         
        , dim_state_province.sales_territory 
        , dim_state_province.country_key 
        , dim_state_province.country_name 
        , dim_state_province.formal_name 
        , dim_state_province.continent 
        , dim_state_province.region 
        , dim_state_province.subregion        
    FROM `stg_dim_city__cast_type` AS dim_city
      LEFT JOIN {{ ref("stg_dim_state_province") }} AS dim_state_province
        ON dim_city.state_province_key = dim_state_province.state_province_key