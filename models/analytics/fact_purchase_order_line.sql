WITH fact_purchase_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.purchasing__purchase_order_lines`
)

, fact_purchase_order_line__rename AS (
    SELECT
        purchase_order_line_id AS purchase_order_line_key
        , description
        , purchase_order_id AS purchase_order_key
        , stock_item_id AS product_key
        , package_type_id AS package_type_key
        , last_receipt_date
        , is_order_line_finalized AS is_order_line_finalized_boolean
        , ordered_outers
        , received_outers
        , expected_unit_price_per_outer
    FROM `fact_purchase_order_line__source`
)

, fact_purchase_order_line__cast_type AS (
    SELECT
        CAST(purchase_order_line_key AS INT) AS purchase_order_line_key 
        , CAST(description AS STRING) AS description 
        , CAST(purchase_order_key AS INT) AS purchase_order_key 
        , CAST(product_key AS INT) AS product_key 
        , CAST(package_type_key AS INT) AS package_type_key 
        , CAST(last_receipt_date AS DATE) AS last_receipt_date 
        , CAST(is_order_line_finalized_boolean AS BOOLEAN) AS is_order_line_finalized_boolean 
        , CAST(ordered_outers AS INT) AS ordered_outers 
        , CAST(received_outers AS INT) AS received_outers 
        , CAST(expected_unit_price_per_outer AS NUMERIC) AS expected_unit_price_per_outer         
    FROM `fact_purchase_order_line__rename`
)

, fact_purchase_order_line__handle_boolean AS (
    SELECT
        *
        , CASE 
            WHEN is_order_line_finalized_boolean IS TRUE THEN 'Order Line Finalized'
            WHEN is_order_line_finalized_boolean IS FALSE THEN 'Not Order Line Finalized'
            WHEN is_order_line_finalized_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_order_line_finalized
    FROM `fact_purchase_order_line__cast_type`
)

, fact_purchase_order_line__handle_null AS (
    SELECT
        COALESCE(purchase_order_line_key, 0) AS purchase_order_line_key 
        , COALESCE(description, 'Undefined') AS description
        , COALESCE(purchase_order_key, 0) AS purchase_order_key 
        , COALESCE(product_key, 0) AS product_key 
        , COALESCE(package_type_key, 0) AS package_type_key 
        , COALESCE(last_receipt_date, '2012-01-01') AS last_receipt_date
        , is_order_line_finalized_boolean
        , COALESCE(is_order_line_finalized, 'Undefined') AS is_order_line_finalized
        , COALESCE(ordered_outers, 0) AS ordered_outers 
        , COALESCE(received_outers, 0) AS received_outers 
        , COALESCE(expected_unit_price_per_outer, 0) AS expected_unit_price_per_outer 
    FROM `fact_purchase_order_line__handle_boolean`
)

, fact_purchase_order_line__join AS (
    SELECT
        purchase_order_line.purchase_order_line_key
        , purchase_order_line.description
        , purchase_order_line.purchase_order_key
        , purchase_order.supplier_key
        , purchase_order.delivery_method_key
        , purchase_order.contact_person_key
        , purchase_order_line.product_key
        , purchase_order_line.package_type_key
        , purchase_order.order_date
        , purchase_order.expected_delivery_date AS order_expected_delivery_date
        , purchase_order_line.last_receipt_date
        , purchase_order.is_order_finalized_boolean
        , purchase_order.is_order_finalized
        , purchase_order_line.is_order_line_finalized_boolean
        , purchase_order_line.is_order_line_finalized
        , purchase_order_line.ordered_outers
        , purchase_order_line.received_outers
        , purchase_order_line.expected_unit_price_per_outer
        , purchase_order.order_comments
        , purchase_order.order_internal_comments
    FROM `fact_purchase_order_line__handle_null` AS purchase_order_line
      LEFT JOIN {{ ref("stg_fact_purchase_order") }} AS purchase_order
        ON purchase_order.purchase_order_key = purchase_order_line.purchase_order_key
)

    SELECT
        purchase_order_line_key
        , description
        , purchase_order_key
        , supplier_key
        , delivery_method_key
        , contact_person_key
        , product_key
        , package_type_key
        , order_date
        , order_expected_delivery_date
        , last_receipt_date
        , is_order_finalized_boolean
        , is_order_finalized
        , is_order_line_finalized_boolean
        , is_order_line_finalized
        , ordered_outers
        , received_outers
        , expected_unit_price_per_outer
        , order_comments
        , order_internal_comments
    FROM `fact_purchase_order_line__join`
    

