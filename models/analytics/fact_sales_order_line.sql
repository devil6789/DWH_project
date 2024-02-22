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

, fact_sales_order_line__handle_null AS (
    SELECT
        sales_order_line_key
        , description
        , COALESCE(sales_order_key, 0) AS sales_order_key 
        , COALESCE(product_key, 0) AS product_key 
        , COALESCE(package_type_key, 0) AS package_type_key 
        , line_picking_completed_when
        , quantity
        , unit_price
        , tax_rate
    FROM `fact_sales_order_line__cast_type`
)

, fact_sales_order_line__join AS (
    SELECT
        fact_sales_order_line.sales_order_line_key
        , fact_sales_order_line.description
        , fact_sales_order_line.sales_order_key
        , fact_sales_order_line.product_key
        , fact_sales_order_line.package_type_key
        , COALESCE(fact_sales_order.customer_key, -1) AS customer_key
        , COALESCE(fact_sales_order.sales_person_key, -1) AS sales_person_key
        , COALESCE(fact_sales_order.picked_by_person_key, -1) AS picked_by_person_key
        , COALESCE(fact_sales_order.contact_person_key, -1) AS contact_person_key
        , COALESCE(fact_sales_order.backorder_order_key, -1) AS backorder_order_key
        , COALESCE(fact_sales_order.order_date, '2012-01-01') AS order_date        
        , COALESCE(fact_sales_order.order_expected_delivery_date, '2012-01-01') AS order_expected_delivery_date
        , COALESCE(fact_sales_order.order_picking_completed_when, '2012-01-01') AS order_picking_completed_when
        , fact_sales_order_line.line_picking_completed_when
        , COALESCE(fact_sales_order.is_undersupply_backordered, 'Invalid') AS is_undersupply_backordered
        , COALESCE(fact_sales_order.customer_purchase_order_number, 'Invalid') AS customer_purchase_order_number
        , fact_sales_order_line.quantity
        , fact_sales_order_line.unit_price
        , fact_sales_order_line.tax_rate
    FROM `fact_sales_order_line__handle_null` AS fact_sales_order_line
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
        sales_order_line_key
        , description
        , sales_order_key
        , product_key       
        , customer_key
        , sales_person_key
        , picked_by_person_key
        , contact_person_key
        , backorder_order_key
        
        , FARM_FINGERPRINT(CONCAT(package_type_key, ',', is_undersupply_backordered)) AS sales_order_line_indicator_key
        , order_date
        , order_expected_delivery_date
        , order_picking_completed_when
        , line_picking_completed_when       
        , customer_purchase_order_number
        , is_undersupply_backordered
        , quantity
        , unit_price
        , tax_rate
        , net_sales
        , net_tax
        , net_sales_real       
    FROM `fact_sales_order_line__calculated_measure`
    