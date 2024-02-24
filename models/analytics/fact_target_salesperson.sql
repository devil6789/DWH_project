WITH fact_target_salesperson__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, fact_target_salesperson__rename AS (
    SELECT
        year_month
        , salesperson_person_id AS salesperson_person_key
        , target_revenue AS target_net_sales
    FROM `fact_target_salesperson__source`
)

, fact_target_salesperson__cast_type AS (
    SELECT
        CAST(year_month AS DATE) AS year_month     
        , CAST(salesperson_person_key AS INT) AS salesperson_person_key 
        , CAST(target_net_sales AS NUMERIC) AS target_net_sales 
    FROM `fact_target_salesperson__rename`
)

, fact_target_salesperson__handle_null AS (
    SELECT
        year_month
        , salesperson_person_key
        , COALESCE(target_net_sales, 0) AS target_net_sales
    FROM `fact_target_salesperson__cast_type`
)

, fact_target_salesperson__join AS (
    SELECT
        year_month
        , salesperson_person_key
        , fact_target_salesperson.target_net_sales
        , COALESCE(stg_fact_target_salesperson.net_sales, -1) AS net_sales
    FROM `fact_target_salesperson__handle_null` AS fact_target_salesperson

    FULL OUTER JOIN {{ ref("stg_fact_target_salesperson") }} AS stg_fact_target_salesperson
    USING (year_month, salesperson_person_key)
)

, fact_target_salesperson__calculated_measure AS (
    SELECT
        *
        , net_sales / NULLIF(target_net_sales, 0) AS achieved_percentage
    FROM `fact_target_salesperson__join`
)

, fact_target_salesperson__define_achieve AS (
    SELECT
        *
        , CASE 
            WHEN achieved_percentage < 0.95 THEN 'Not Achieve'
            WHEN achieved_percentage >= 0.95 THEN 'Not Achieve'
            WHEN target_net_sales IS NULL THEN 'No Target'
            ELSE 'Invalid'
          END AS achieved_status
    FROM `fact_target_salesperson__calculated_measure`
)

    SELECT
        year_month
    	  , salesperson_person_key
      	, target_net_sales	
        , net_sales	
        , achieved_percentage	
        , achieved_status
    FROM `fact_target_salesperson__define_achieve`

    
      