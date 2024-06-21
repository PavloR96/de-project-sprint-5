INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
SELECT
   res.restaurant_id
  ,res.restaurant_name
  ,tms.date as settlement_date
  ,count(distinct ord.id) as orders_count
  ,sum(prs.total_sum) as orders_total_sum
  ,sum(prs.bonus_payment) as orders_bonus_payment_sum
  ,sum(prs.bonus_grant) as orders_bonus_granted_sum
  ,sum(prs.total_sum)*0.25 as order_processing_fee
  ,sum(prs.total_sum) - sum(prs.bonus_payment) - sum(prs.total_sum)*0.25 as restaurant_reward_sum

  FROM dds.fct_product_sales as prs
  INNER JOIN dds.dm_orders as ord on ord.id = prs.order_id
  INNER JOIN dds.dm_timestamps as tms on tms.id = ord.timestamp_id
  INNER JOIN dds.dm_restaurants as res on res.id = ord.restaurant_id

group by
   res.restaurant_id
  ,res.restaurant_name
  ,tms.date

ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
restaurant_name= EXCLUDED.restaurant_name,
orders_count= EXCLUDED.orders_count,
orders_total_sum= EXCLUDED.orders_total_sum,
orders_bonus_payment_sum= EXCLUDED.orders_bonus_payment_sum,
orders_bonus_granted_sum= EXCLUDED.orders_bonus_granted_sum,
restaurant_reward_sum= EXCLUDED.restaurant_reward_sum;
