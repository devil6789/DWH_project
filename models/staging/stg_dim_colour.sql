WITH dim_colour__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)

, dim_colour__rename AS (
    SELECT
        color_id AS colour_key
        , color_name AS colour_name
    FROM `dim_colour__source`
)

, dim_colour__cast_type AS (
    SELECT
        CAST(colour_key AS INT) AS colour_key
        , CAST(colour_name AS STRING) AS colour_name
    FROM `dim_colour__rename`
)

    SELECT 
        colour_key
        , colour_name
    FROM `dim_colour__cast_type`