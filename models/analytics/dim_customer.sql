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
    FROM `dim_customer__cast_type`
)

    SELECT *
    FROM `dim_customer__handle_boolean`