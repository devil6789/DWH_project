WITH dim_package_type__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, dim_package_type__rename AS (
    SELECT 
        package_type_id AS package_type_key
        , package_type_name
    FROM `dim_package_type__source`  
)

, dim_package_type__cast_type AS (
    SELECT
        CAST(package_type_key AS INT) AS package_type_key
        , CAST(package_type_name AS STRING) AS package_type_name
    FROM `dim_package_type__rename`
)

, dim_package_type__add_undefined_invalid AS (
    SELECT
        package_type_key
        , package_type_name
    FROM `dim_package_type__cast_type`

    UNION ALL
    SELECT 
        0
        , 'Undefined'

    UNION ALL
    SELECT 
        -1
        , 'Invalid'
)

    SELECT
        package_type_key
        , COALESCE(package_type_name, 'Undefined') AS package_type_name
    FROM `dim_package_type__add_undefined_invalid`

    