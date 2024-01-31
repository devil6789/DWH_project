WITH dim_customer__source AS (
    SELECT *
    FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename AS (
    SELECT
        customer_id AS customer_key
        , customer_name
        , is_statement_sent AS is_statement_sent_boolean
        , is_on_credit_hold AS is_on_credit_hold_boolean
        , standard_discount_percentage
        , payment_days AS customer_payment_days
        , credit_limit
        , account_opened_date
        , customer_category_id AS customer_category_key
        , buying_group_id AS buying_group_key
        , delivery_method_id AS delivery_method_key
    FROM `dim_customer__source`
)

, dim_customer__cast_type AS (
    SELECT
        CAST(customer_key AS INT) AS customer_key
        , CAST(customer_name AS STRING) AS customer_name
        , CAST(is_statement_sent_boolean AS BOOLEAN) AS is_statement_sent_boolean
        , CAST(is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
        , CAST(standard_discount_percentage AS NUMERIC) AS standard_discount_percentage
        , CAST(customer_payment_days AS INT) AS customer_payment_days 
        , CAST(credit_limit AS NUMERIC) AS credit_limit
        , CAST(account_opened_date AS DATE) AS account_opened_date
        , CAST(customer_category_key AS INT) AS customer_category_key
        , CAST(buying_group_key AS INT) AS buying_group_key
        , CAST(delivery_method_key AS INT) AS delivery_method_key     
    FROM `dim_customer__rename`
)

, dim_customer__handle_boolean AS (
    SELECT
        customer_key
        , customer_name
        , CASE
            WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
            WHEN is_statement_sent_boolean IS FALSE THEN 'Not Statement Sent'
            WHEN is_statement_sent_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_statement_sent
        , CASE
            WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
            WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
            WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
            ELSE 'Invalid'
          END AS is_on_credit_hold 
        , standard_discount_percentage
        , customer_payment_days
        , credit_limit
        , account_opened_date
        , customer_category_key
        , buying_group_key
        , delivery_method_key
    FROM `dim_customer__cast_type`
)

    SELECT
        dim_customer.customer_key
        , dim_customer.customer_name
        , dim_customer.is_statement_sent
        , dim_customer.is_on_credit_hold
        , dim_customer.standard_discount_percentage
        , dim_customer.customer_payment_days
        , dim_customer.credit_limit
        , dim_customer.account_opened_date
        , dim_customer.customer_category_key
        , dim_customer_category.customer_category_name
        , dim_customer.buying_group_key
        , dim_customer_buying_group.buying_group_name
        , dim_customer.delivery_method_key
        , dim_delivery_method.delivery_method_name
    FROM `dim_customer__handle_boolean` AS dim_customer
      LEFT JOIN {{ ref("stg_dim_customer_category") }} AS dim_customer_category
        ON dim_customer.customer_category_key = dim_customer_category.customer_category_key

      LEFT JOIN {{ ref("stg_dim_customer_buying_group") }} AS dim_customer_buying_group
        ON dim_customer.buying_group_key = dim_customer_buying_group.buying_group_key

      LEFT JOIN {{ ref("stg_dim_delivery_method") }} AS dim_delivery_method
        ON dim_customer.delivery_method_key = dim_delivery_method.delivery_method_key