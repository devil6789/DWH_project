WITH fact_sales_order_line__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename AS (
    SELECT
        order_line_id AS order_line_key
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
        CAST(order_line_key AS INT) AS order_line_key 
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
        fact_sales_order_line.order_line_key
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
        order_line_key
        , description
        , sales_order_key
        , product_key
        , package_type_key
        , customer_key
        , sales_person_key
        , picked_by_person_key
        , contact_person_key
        , backorder_order_key
        , order_date
        , order_expected_delivery_date
        , order_picking_completed_when
        , line_picking_completed_when
        , is_undersupply_backordered
        , customer_purchase_order_number
        , quantity
        , unit_price
        , tax_rate
        , net_sales
        , net_tax
        , net_sales_real
    FROM `fact_sales_order_line__calculated_measure`