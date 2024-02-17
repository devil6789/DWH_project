WITH fact_purchase_order__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)

, fact_purchase_order__rename AS (
    SELECT       
        purchase_order_id AS purchase_order_key
        , supplier_id AS supplier_key
        , delivery_method_id AS delivery_method_key
        , contact_person_id AS contact_person_key
        , expected_delivery_date
        , is_order_finalized AS is_order_finalized_boolean
        , comments AS order_comments
        , internal_comments AS order_internal_comments
    FROM `fact_purchase_order__source`
)

, fact_purchase_order__cast_type AS (
    SELECT
        CAST(purchase_order_key AS INT) AS purchase_order_key 
        , CAST(supplier_key AS INT) AS supplier_key 
        , CAST(delivery_method_key AS INT) AS delivery_method_key 
        , CAST(contact_person_key AS INT) AS contact_person_key 
        , CAST(expected_delivery_date AS DATE) AS expected_delivery_date 
        , CAST(is_order_finalized_boolean AS BOOLEAN) AS is_order_finalized_boolean 
        , CAST(order_comments AS STRING) AS order_comments 
        , CAST(order_internal_comments AS STRING) AS order_internal_comments         
    FROM `fact_purchase_order__rename`
)

    SELECT *
    FROM `fact_purchase_order__cast_type`