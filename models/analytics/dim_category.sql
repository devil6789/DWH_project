WITH dim_category AS (
    SELECT *
    FROM {{ ref("stg_dim_category") }}
)

    SELECT
        *
        , category_key AS category_level_1_key
        , category_name AS category_level_1_name
        , parent_category_key AS parent_category_level_2_key
        , parent_category_name AS category_level_2_name
        , 0 AS category_level_3_key
        , 'Undefined' AS category_level_3_name
        , 0 AS category_level_4_key
        , 'Undefined' AS category_level_4_name
    FROM `dim_category` AS level_1
    WHERE category_level = 1

    UNION ALL
    SELECT
        *
        , parent_category_key AS category_level_1_key
        , parent_category_name AS category_level_1_name
        , category_key AS parent_category_level_2_key
        , category_name AS category_level_2_name
        , 0 AS category_level_3_key
        , 'Undefined' AS category_level_3_name
        , 0 AS category_level_4_key
        , 'Undefined' AS category_level_4_name
    FROM `dim_category` AS level_2
    WHERE category_level = 2

    UNION ALL
    SELECT
        level_3.*
        , source.parent_category_key AS category_level_1_key
        , source.parent_category_name AS category_level_1_name
        , level_3.parent_category_key AS parent_category_level_2_key
        , level_3.parent_category_name AS category_level_2_name
        , level_3.category_key AS category_level_3_key
        , level_3.category_name AS category_level_3_name
        , 0 AS category_level_4_key
        , 'Undefined' AS category_level_4_name
    FROM `dim_category` AS level_3
    LEFT JOIN `dim_category` AS source
    ON level_3.parent_category_key = source.category_key
    WHERE level_3.category_level = 3

    UNION ALL
    SELECT
        level_4.*
        , level_2.parent_category_key AS category_level_1_key
        , level_2.parent_category_name AS category_level_1_name
        , level_3.parent_category_key AS parent_category_level_2_key
        , level_3.parent_category_name AS category_level_2_name
        , level_4.parent_category_key AS category_level_3_key
        , level_4.parent_category_name AS category_level_3_name
        , level_4.category_key AS category_level_4_key
        , level_4.category_name AS category_level_4_name       
    FROM `dim_category` AS level_4

    LEFT JOIN `dim_category` AS level_3
      ON level_4.parent_category_key = level_3.category_key

    LEFT JOIN `dim_category` AS level_2
      ON level_3.parent_category_key = level_2.category_key 
    WHERE level_4.category_level = 4


