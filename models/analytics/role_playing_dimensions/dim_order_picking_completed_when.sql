with dim_order_picking_completed_when__source AS (
  SELECT *
  FROM unnest(generate_date_array('2010-01-01', '2030-01-01', interval 1 DAY)) as order_picking_completed_when
)

, dim_order_picking_completed_when__enrich AS (
  SELECT * 
         , format_date('%A', order_picking_completed_when) as day_of_week
         , format_date('%a', order_picking_completed_when) as day_of_week_short
         , date_trunc(order_picking_completed_when, month) as year_month
         , format_date('%B', order_picking_completed_when) as month
         , date_trunc(order_picking_completed_when, year) as year
         , extract(year from order_picking_completed_when) as year_number
  FROM `dim_order_picking_completed_when__source`
)

, dim_order_picking_completed_when__add_column AS (
  SELECT *
      , case 
          WHEN day_of_week_short = 'Sun' OR day_of_week_short = 'Sat' then 'Weekend'
          else 'Weekday'
         END as is_weekday_or_weekend
  FROM `dim_order_picking_completed_when__enrich`       
)

SELECT 
      order_picking_completed_when
    , day_of_week	
    , day_of_week_short
    , is_weekday_or_weekend
    , year_month
    , month	
    , year	
    , year_number	
    
FROM `dim_order_picking_completed_when__add_column`








