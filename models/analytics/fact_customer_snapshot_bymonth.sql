WITH fact_customer_snapshot_bymonth__source AS (
    SELECT *
    FROM {{ ref("fact_sales_order_line") }}
)

, fact_customer_snapshot_bymonth__summarize AS (
    SELECT
        customer_key
        , DATE_TRUNC(order_date, MONTH) AS year_month
        , MAX(order_date) AS last_active_day
        , DATE_DIFF('2016-05-31', MAX(order_date), day) AS lifetime_recency 
        , COUNT(DISTINCT order_date) AS frequency
        , SUM(net_sales) AS monetary
        -- , CONCAT(product_key, ",") AS product_key
    FROM `fact_customer_snapshot_bymonth__source`
    GROUP BY 1,2
    
)

, fact_customer_snapshot_bymonth__add_stg_year_month AS (
    SELECT
        *
        , LEAD(year_month,1) OVER (PARTITION BY customer_key ORDER BY year_month) AS year_month_1_following
    FROM `fact_customer_snapshot_bymonth__summarize`
)  

, fact_customer_snapshot_bymonth__add_recency AS (
    SELECT
        *
        , DATE_DIFF(year_month_1_following, last_active_day, day) AS recency
         
       
    FROM `fact_customer_snapshot_bymonth__add_stg_year_month`
)

, fact_customer_snapshot_bymonth__add_lifetime_values AS (
    SELECT *
        , SUM(frequency) OVER (PARTITION BY customer_key ORDER BY year_month) AS lifetime_frequency
        , SUM(monetary) OVER (PARTITION BY customer_key ORDER BY year_month) AS lifetime_monetary
    FROM `fact_customer_snapshot_bymonth__add_recency`
)

, fact_customer_snapshot_bymonth__calculate_percent AS (
    SELECT *
        , PERCENT_RANK() OVER (ORDER BY recency) AS recency_percent
        , PERCENT_RANK() OVER (ORDER BY frequency) AS frequency_percent
        , PERCENT_RANK() OVER (ORDER BY monetary) AS monetary_percent
        , PERCENT_RANK() OVER (ORDER BY lifetime_recency) AS lifetime_recency_percent
        , PERCENT_RANK() OVER (ORDER BY lifetime_frequency) AS lifetime_frequency_percent
        , PERCENT_RANK() OVER (ORDER BY lifetime_monetary) AS lifetime_monetary_percent
    FROM `fact_customer_snapshot_bymonth__add_lifetime_values`
)

, fact_customer_snapshot_bymonth__calculate_percentile AS (
    SELECT
        *
        , CASE 
            WHEN recency_percent >= 0.8 THEN 1
            WHEN recency_percent >= 0.6 THEN 2
            WHEN recency_percent >= 0.4 THEN 3
            WHEN recency_percent >= 0.2 THEN 4
            WHEN recency_percent >= 0 THEN 5
            ELSE -1
          END AS recency_score
        , CASE 
            WHEN lifetime_recency_percent >= 0.8 THEN 1
            WHEN lifetime_recency_percent >= 0.6 THEN 2
            WHEN lifetime_recency_percent >= 0.4 THEN 3
            WHEN lifetime_recency_percent >= 0.2 THEN 4
            WHEN lifetime_recency_percent >= 0 THEN 5
            ELSE -1
          END AS lifetime_recency_score
        , CASE 
            WHEN frequency_percent >= 0.8 THEN 5
            WHEN frequency_percent >= 0.6 THEN 4
            WHEN frequency_percent >= 0.4 THEN 3
            WHEN frequency_percent >= 0.2 THEN 2
            WHEN frequency_percent >= 0 THEN 1
            ELSE -1
          END AS frequency_score
        , CASE 
            WHEN lifetime_frequency_percent >= 0.8 THEN 5
            WHEN lifetime_frequency_percent >= 0.6 THEN 4
            WHEN lifetime_frequency_percent >= 0.4 THEN 3
            WHEN lifetime_frequency_percent >= 0.2 THEN 2
            WHEN lifetime_frequency_percent >= 0 THEN 1
            ELSE -1
          END AS lifetime_frequency_score
        , CASE 
            WHEN monetary_percent >= 0.8 THEN 5
            WHEN monetary_percent >= 0.6 THEN 4
            WHEN monetary_percent >= 0.4 THEN 3
            WHEN monetary_percent >= 0.2 THEN 2
            WHEN monetary_percent >= 0 THEN 1
            ELSE -1
          END AS monetary_score
        , CASE 
            WHEN lifetime_monetary_percent >= 0.8 THEN 5
            WHEN lifetime_monetary_percent >= 0.6 THEN 4
            WHEN lifetime_monetary_percent >= 0.4 THEN 3
            WHEN lifetime_monetary_percent >= 0.2 THEN 2
            WHEN lifetime_monetary_percent >= 0 THEN 1
            ELSE -1
          END AS lifetime_monetary_score
        
    FROM `fact_customer_snapshot_bymonth__calculate_percent`
)

, fact_customer_snapshot_bymonth__calculate_RFM_score AS (
    SELECT
        *
        , CAST(CONCAT(recency_score
                 , frequency_score
                 , monetary_score) AS INT) AS RFM_score
        , CAST(CONCAT(lifetime_recency_score
                 , lifetime_frequency_score
                 , lifetime_monetary_score) AS INT) AS lifetime_RFM_score
        
    FROM `fact_customer_snapshot_bymonth__calculate_percentile`
)

, fact_customer_snapshot_bymonth__add_customer_segment AS (
    SELECT
        *
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
        , CASE 
            WHEN lifetime_RFM_score IN (555, 554, 544, 545, 454, 455, 445) THEN 'Champions'
            WHEN lifetime_RFM_score IN (543, 444, 435, 355, 354, 345, 344, 335) THEN 'Loyal'
            WHEN lifetime_RFM_score IN (553, 551, 552, 541, 542, 533, 532, 531, 452, 451, 442
                              , 441, 431, 453, 433, 432, 423, 353, 352, 351, 342, 341, 333, 323) THEN 'Potential Loyalist'
            WHEN lifetime_RFM_score IN (525, 524, 523, 522, 521, 515, 514, 513, 425,424, 413,414,415, 315, 314, 313) THEN 'Promising'
            WHEN lifetime_RFM_score IN (512, 511, 422, 421, 412, 411, 311) THEN 'New Customers'
            WHEN lifetime_RFM_score IN (535, 534, 443, 434, 343, 334, 325, 324) THEN 'Need Attention'
            WHEN lifetime_RFM_score IN (331, 321, 312, 221, 213, 231, 241, 251) THEN 'About To Sleep'
            WHEN lifetime_RFM_score IN (255, 254, 245, 244, 253, 252, 243, 242, 235, 234
                              , 225, 224, 153, 152, 145, 143, 142, 135, 134, 133, 125, 124) THEN 'At Risk'
            WHEN lifetime_RFM_score IN (155, 154, 144, 214,215,115, 114, 113) THEN 'Cannot Lose Them'
            WHEN lifetime_RFM_score IN (332, 322, 233, 232, 223, 222, 132, 123, 122, 212, 211) THEN 'Hibernating customers'
            WHEN lifetime_RFM_score IN (111, 112, 121, 131,141,151) THEN 'Lost customers'
            -- ELSE 'Invalid'
          END AS lifetime_customer_segment
    FROM `fact_customer_snapshot_bymonth__calculate_RFM_score`
)

    SELECT 
        customer_key
        , year_month
        , last_active_day
        , year_month_1_following
        , recency
        , lifetime_recency
        , frequency
        , lifetime_frequency
        , monetary
        , lifetime_monetary
        , recency_score
        , frequency_score
        , monetary_score
        , RFM_score
        , customer_segment
        , lifetime_recency_score
        , lifetime_frequency_score
        , lifetime_monetary_score
        , lifetime_RFM_score
        , lifetime_customer_segment
    FROM `fact_customer_snapshot_bymonth__add_customer_segment`