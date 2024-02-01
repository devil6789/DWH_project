WITH fact_sales_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename AS (
    SELECT
        order_id AS sales_order_key
        , customer_id AS customer_key
        , salesperson_person_id AS sales_person_key
        , picked_by_person_id AS picked_by_person_key
        , contact_person_id AS contact_person_key
        , backorder_order_id AS backorder_order_key
        , order_date
        , expected_delivery_date AS order_expected_delivery_date
        , picking_completed_when AS order_picking_completed_when
        , is_undersupply_backordered AS is_undersupply_backordered_boolean
        , customer_purchase_order_number
    FROM `fact_sales_order__source`
)

, fact_sales_order__cast_type AS (
    SELECT
        CAST(sales_order_key AS INT) AS sales_order_key 
        , CAST(customer_key AS INT) AS customer_key
        , CAST(sales_person_key AS INT) AS sales_person_key 
        , CAST(picked_by_person_key AS INT) AS picked_by_person_key 
        , CAST(contact_person_key AS INT) AS contact_person_key 
        , CAST(backorder_order_key AS INT) AS backorder_order_key 
        , CAST(order_date AS DATE) AS order_date
        , CAST(order_expected_delivery_date AS DATE) AS order_expected_delivery_date 
        , CAST(order_picking_completed_when AS DATE) AS order_picking_completed_when 
        , CAST(is_undersupply_backordered_boolean AS BOOLEAN) AS is_undersupply_backordered_boolean 
        , CAST(customer_purchase_order_number AS STRING) AS customer_purchase_order_number      
    FROM `fact_sales_order__rename`
)

, fact_sales_order__handle_boolean AS (
    SELECT * EXCEPT(is_undersupply_backordered_boolean)
        , CASE
            WHEN is_undersupply_backordered_boolean IS TRUE THEN 'Undersupply Backordered' 
            WHEN is_undersupply_backordered_boolean IS FALSE THEN 'Not Undersupply Backordered'  
            WHEN is_undersupply_backordered_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_undersupply_backordered
    FROM `fact_sales_order__cast_type`
)
    SELECT
        sales_order_key
        , customer_key
        , sales_person_key
        , picked_by_person_key
        , contact_person_key
        , backorder_order_key
        , order_date
        , order_expected_delivery_date
        , order_picking_completed_when
        , is_undersupply_backordered
        , customer_purchase_order_number
    FROM `fact_sales_order__handle_boolean`