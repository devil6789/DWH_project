WITH fact_sales_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename AS (
    SELECT
        order_line_id AS sales_order_line_key
        , description
        , order_id AS sales_order_key
        , stock_item_id AS product_key
        , package_type_id AS package_type_key
        , picking_completed_when AS line_picking_completed_when
        , quantity
        , unit_price
        , tax_rate
    FROM `fact_sales_order_line__source`
)

, fact_sales_order_line__cast_type AS (
    SELECT
        CAST(sales_order_line_key AS INT) AS sales_order_line_key 
        , CAST(description AS STRING) AS description 
        , CAST(sales_order_key AS INT) AS sales_order_key 
        , CAST(product_key AS INT) AS product_key 
        , CAST(package_type_key AS INT) AS package_type_key 
        , CAST(line_picking_completed_when AS DATE) AS line_picking_completed_when 
        , CAST(quantity AS INT) AS quantity 
        , CAST(unit_price AS NUMERIC) AS unit_price 
        , CAST(tax_rate AS NUMERIC) AS tax_rate 
    FROM `fact_sales_order_line__rename`
)

, fact_sales_order_line__join AS (
    SELECT
        fact_sales_order_line.sales_order_line_key
        , fact_sales_order_line.description
        , fact_sales_order_line.sales_order_key
        , fact_sales_order_line.product_key
        , fact_sales_order_line.package_type_key
        , fact_sales_order.customer_key
        , fact_sales_order.sales_person_key
        , fact_sales_order.picked_by_person_key
        , fact_sales_order.contact_person_key
        , fact_sales_order.backorder_order_key
        , fact_sales_order.order_date        
        , fact_sales_order.order_expected_delivery_date
        , fact_sales_order.order_picking_completed_when

        , fact_sales_order_line.line_picking_completed_when
        , fact_sales_order.is_undersupply_backordered
        , fact_sales_order.customer_purchase_order_number
        , fact_sales_order_line.quantity
        , fact_sales_order_line.unit_price
        , fact_sales_order_line.tax_rate
    FROM `fact_sales_order_line__cast_type` AS fact_sales_order_line
      LEFT JOIN {{ ref("stg_fact_sales_order") }} AS fact_sales_order
        ON fact_sales_order_line.sales_order_key = fact_sales_order.sales_order_key
)

, fact_sales_order_line__calculated_measure AS (
    SELECT *
        , quantity * unit_price AS net_sales
        , quantity * unit_price * tax_rate / 100 AS net_tax
        , quantity * unit_price * (1 - tax_rate /100) AS net_sales_real
    FROM `fact_sales_order_line__join`
)

    SELECT
        COALESCE(sales_order_line_key, 0) AS sales_order_line_key
        , COALESCE(description, 'Undefined') AS description
        , CASE
            WHEN sales_order_key > 0 THEN  sales_order_key
            WHEN sales_order_key IS NULL THEN 0
            WHEN sales_order_key <= 0 THEN -1            
          END AS sales_order_key
        , CASE
            WHEN product_key > 0 THEN  product_key
            WHEN product_key IS NULL THEN 0
            WHEN product_key <= 0 THEN -1
          END AS product_key
        , CASE
            WHEN package_type_key > 0 THEN  package_type_key
            WHEN package_type_key IS NULL THEN 0
            WHEN package_type_key <= 0 THEN -1
          END AS package_type_key
        , CASE
            WHEN customer_key > 0 THEN  customer_key
            WHEN customer_key IS NULL THEN 0
            WHEN customer_key <= 0 THEN -1
          END AS customer_key
        , CASE
            WHEN sales_person_key > 0 THEN  sales_person_key
            WHEN sales_person_key IS NULL THEN 0
            WHEN sales_person_key <= 0 THEN -1
          END AS sales_person_key
        , CASE
            WHEN picked_by_person_key > 0 THEN  picked_by_person_key
            WHEN picked_by_person_key IS NULL THEN 0
            WHEN picked_by_person_key <= 0 THEN -1
          END AS picked_by_person_key
        , CASE
            WHEN contact_person_key > 0 THEN  contact_person_key
            WHEN contact_person_key IS NULL THEN 0
            WHEN contact_person_key <= 0 THEN -1
          END AS contact_person_key 
        , CASE
            WHEN backorder_order_key > 0 THEN  backorder_order_key
            WHEN backorder_order_key IS NULL THEN 0
            WHEN backorder_order_key <= 0 THEN -1
          END AS backorder_order_key 
        , COALESCE(order_date, '2012-01-01') AS order_date
        , COALESCE(order_expected_delivery_date, '2012-01-01') AS order_expected_delivery_date
        , COALESCE(order_picking_completed_when, '2012-01-01') AS order_picking_completed_when
        , COALESCE(line_picking_completed_when, '2012-01-01') AS line_picking_completed_when
        , COALESCE(is_undersupply_backordered, 'Undefined') AS is_undersupply_backordered
        , COALESCE(customer_purchase_order_number, 'Undefined') AS customer_purchase_order_number
        , COALESCE(quantity, 0) AS quantity
        , COALESCE(unit_price, 0) AS unit_price
        , COALESCE(tax_rate, 0) AS tax_rate
        , COALESCE(net_sales, 0) AS net_sales
        , COALESCE(net_tax, 0) AS net_tax
        , COALESCE(net_sales_real, 0) AS net_sales_real
    FROM `fact_sales_order_line__calculated_measure`