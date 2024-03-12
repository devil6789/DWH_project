WITH dim_customer_attribute__source AS (
    SELECT *
    FROM {{ ref("fact_sales_order_line") }}
)

, dim_customer_attribute__group_by AS (
    SELECT 
        customer_key
        , MAX(order_date) AS last_active_date
        , DATE_TRUNC(MIN(order_date), MONTH) AS start_month
        , DATE_TRUNC(MAX(order_date), MONTH) AS end_month
        , SUM(net_sales) AS lifetime_monetary
        , SUM(CASE
                WHEN order_date BETWEEN (DATE_TRUNC('2016-05-31', MONTH) - INTERVAL 12 MONTH) AND '2016-05-31' THEN net_sales
                ELSE 0
              END) AS last_12_months_monetary
        , COUNT(DISTINCT order_date) AS lifetime_frequency
        , COUNT(DISTINCT CASE
                WHEN order_date BETWEEN (DATE_TRUNC('2016-05-31', MONTH) - INTERVAL 12 MONTH) AND '2016-05-31' THEN order_date
              END) AS last_12_months_frequency
    FROM `dim_customer_attribute__source`
    GROUP BY 1
)

, dim_customer_attribute__calculate_recency AS (
    SELECT *
        , DATE_DIFF('2016-05-31', last_active_date, day) AS lifetime_recency
        , CASE 
            WHEN DATE_DIFF('2016-05-31', last_active_date, day) <= 365 THEN 'Does Buy'
            ELSE 'Does Not Buy'
          END AS does_buy_in_last_12_months
    FROM `dim_customer_attribute__group_by`
)

, dim_customer_attribute__percent_rank AS (
    SELECT *
        , PERCENT_RANK() OVER (ORDER BY lifetime_recency) AS lifetime_recency_percent
        , PERCENT_RANK() OVER (ORDER BY lifetime_frequency) AS lifetime_frequency_percent
        , PERCENT_RANK() OVER (ORDER BY lifetime_monetary) AS lifetime_monetary_percent       
    FROM `dim_customer_attribute__calculate_recency`
)

, dim_customer_attribute__percentile AS (
    SELECT *
        , CASE 
            WHEN lifetime_recency_percent >= 0.8 THEN 1
            WHEN lifetime_recency_percent >= 0.6 THEN 2
            WHEN lifetime_recency_percent >= 0.4 THEN 3
            WHEN lifetime_recency_percent >= 0.2 THEN 4
            WHEN lifetime_recency_percent >= 0 THEN 5
            ELSE -1
          END AS lifetime_recency_percentile
        , CASE 
            WHEN lifetime_frequency_percent >= 0.8 THEN 5
            WHEN lifetime_frequency_percent >= 0.6 THEN 4
            WHEN lifetime_frequency_percent >= 0.4 THEN 3
            WHEN lifetime_frequency_percent >= 0.2 THEN 2
            WHEN lifetime_frequency_percent >= 0 THEN 1
            ELSE -1
          END AS lifetime_frequency_percentile
        , CASE 
            WHEN lifetime_monetary_percent >= 0.8 THEN 5
            WHEN lifetime_monetary_percent >= 0.6 THEN 4
            WHEN lifetime_monetary_percent >= 0.4 THEN 3
            WHEN lifetime_monetary_percent >= 0.2 THEN 2
            WHEN lifetime_monetary_percent >= 0 THEN 1
            ELSE -1
          END AS lifetime_monetary_percentile
    FROM `dim_customer_attribute__percent_rank`
)

, dim_customer_attribute__add_RFM_score AS (
    SELECT *
        , CAST(CONCAT(lifetime_recency_percentile
                 , lifetime_frequency_percentile
                 , lifetime_monetary_percentile) AS INT) AS RFM_score
    FROM `dim_customer_attribute__percentile`
)

, dim_customer_attribute__add_customer_segment AS (
    SELECT *
        , CASE 
            WHEN RFM_score IN (555, 554, 544, 545, 454, 455, 445) THEN 'Champions'
            WHEN RFM_score IN (543, 444, 435, 355, 354, 345, 344, 335) THEN 'Loyal'
            WHEN RFM_score IN (553, 551, 552, 541, 542, 533, 532, 531, 452, 451, 442
                              , 441, 431, 453, 433, 432, 423, 353, 352, 351, 342, 341, 333, 323) THEN 'Potential Loyalist'
            WHEN RFM_score IN (525, 524, 523, 522, 521, 515, 514, 513, 425,424, 413,414,415, 315, 314, 313) THEN 'Promising'
            WHEN RFM_score IN (512, 511, 422, 421, 412, 411, 311) THEN 'New Customers'
            WHEN RFM_score IN (535, 534, 443, 434, 343, 334, 325, 324) THEN 'Need Attention'
            WHEN RFM_score IN (331, 321, 312, 221, 213, 231, 241, 251) THEN 'About To Sleep'
            WHEN RFM_score IN (255, 254, 245, 244, 253, 252, 243, 242, 235, 234
                              , 225, 224, 153, 152, 145, 143, 142, 135, 134, 133, 125, 124) THEN 'At Risk'
            WHEN RFM_score IN (155, 154, 144, 214,215,115, 114, 113) THEN 'Cannot Lose Them'
            WHEN RFM_score IN (332, 322, 233, 232, 223, 222, 132, 123, 122, 212, 211) THEN 'Hibernating customers'
            WHEN RFM_score IN (111, 112, 121, 131,141,151) THEN 'Lost customers'
            -- ELSE 'Invalid'
          END AS customer_segment
    FROM `dim_customer_attribute__add_RFM_score`
)

, dim_customer_attribute__add_values_undefined_invalid AS (
    SELECT
        customer_key
        , last_active_date
        , start_month
        , end_month
        , does_buy_in_last_12_months
        , lifetime_recency
        , lifetime_monetary
        , last_12_months_monetary
        , lifetime_frequency
        , last_12_months_frequency
        , lifetime_recency_percent
        , lifetime_frequency_percent
        , lifetime_monetary_percent
        , lifetime_recency_percentile
        , lifetime_frequency_percentile
        , lifetime_monetary_percentile
        , RFM_score
        , customer_segment
    FROM `dim_customer_attribute__add_customer_segment`

    UNION ALL
    SELECT
        0 AS customer_key
        , '2012-01-01' AS last_active_date
        , '2012-01-01' AS start_month
        , '2012-01-01' AS end_month
        , 'Undefined' AS does_buy_in_last_12_months
        , 0 AS lifetime_recency
        , 0 AS lifetime_monetary
        , 0 AS last_12_months_monetary
        , 0 AS lifetime_frequency
        , 0 AS last_12_months_frequency
        , 0 AS lifetime_recency_percent
        , 0 AS lifetime_frequency_percent
        , 0 AS lifetime_monetary_percent
        , 0 AS lifetime_recency_percentile
        , 0 AS lifetime_frequency_percentile
        , 0 AS lifetime_monetary_percentile
        , 0 AS RFM_score
        , 'Undefined' AS customer_segment

    UNION ALL
    SELECT
        -1 AS customer_key
        , '2012-01-01' AS last_active_date
        , '2012-01-01' AS start_month
        , '2012-01-01' AS end_month
        , 'Invalid' AS does_buy_in_last_12_months
        , -1 AS lifetime_recency
        , -1 AS lifetime_monetary
        , -1 AS last_12_months_monetary
        , -1 AS lifetime_frequency
        , -1 AS last_12_months_frequency
        , -1 AS lifetime_recency_percent
        , -1 AS lifetime_frequency_percent
        , -1 AS lifetime_monetary_percent
        , -1 AS lifetime_recency_percentile
        , -1 AS lifetime_frequency_percentile
        , -1 AS lifetime_monetary_percentile
        , -1 AS RFM_score
        , 'Invalid' AS customer_segment
)

    SELECT
        customer_key
        , last_active_date
        , start_month
        , end_month
        , does_buy_in_last_12_months
        , lifetime_recency
        , lifetime_monetary
        , last_12_months_monetary
        , lifetime_frequency
        , last_12_months_frequency
        , lifetime_recency_percent
        , lifetime_frequency_percent
        , lifetime_monetary_percent
        , lifetime_recency_percentile
        , lifetime_frequency_percentile
        , lifetime_monetary_percentile
        , RFM_score
        , customer_segment
    FROM `dim_customer_attribute__add_values_undefined_invalid`

    