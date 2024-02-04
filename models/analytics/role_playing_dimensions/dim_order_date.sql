with dim_order_date__source AS (
  SELECT *
  FROM unnest(generate_date_array('2010-01-01', '2030-01-01', interval 1 DAY)) as order_date
)

, dim_order_date__enrich AS (
  SELECT * 
         , format_date('%A', order_date) as day_of_week
         , format_date('%a', order_date) as day_of_week_short
         , date_trunc(order_date, month) as year_month
         , format_date('%B', order_date) as month
         , date_trunc(order_date, year) as year
         , extract(year from order_date) as year_number
  FROM `dim_order_date__source`
)

, dim_order_date__add_column AS (
  SELECT *
      , case 
          WHEN day_of_week_short = 'Sun' OR day_of_week_short = 'Sat' then 'Weekend'
          else 'Weekday'
         END as is_weekday_or_weekend
  FROM `dim_order_date__enrich`       
)

SELECT 
      order_date
    , day_of_week	
    , day_of_week_short
    , is_weekday_or_weekend
    , year_month
    , month	
    , year	
    , year_number	
    
FROM `dim_order_date__add_column`




