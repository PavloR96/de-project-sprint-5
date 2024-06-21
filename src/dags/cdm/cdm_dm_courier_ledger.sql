delete from cdm.dm_courier_ledger
where 1=1
  and settlement_year = EXTRACT(YEAR FROM '{{ds}}'::date)
  and settlement_month = EXTRACT(MONTH FROM '{{ds}}'::date);

insert into cdm.dm_courier_ledger (
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum
)

select
	dco.courier_id,
	dco.courier_name,
	dtt.year  as settlement_year,
	dtt.month as settlement_month,
	count(order_id) as orders_count,
	sum(total_sum) as orders_total_sum,
	avg(rate) as rate_avg,
	sum(total_sum) * 0.25 as order_processing_fee,
	case
		when avg(rate) < 4   then sum(greatest(0.05 * total_sum, 100))
		when avg(rate) < 4.5 then sum(greatest(0.07 * total_sum, 150))
		when avg(rate) < 4.9 then sum(greatest(0.08 * total_sum, 175))
		else sum(greatest(0.1 * total_sum , 200))
	end as courier_order_sum,
	
	sum(tip_sum) as courier_tips_sum,
	
	case
		when avg(rate) < 4   then sum(greatest(0.05 * total_sum, 100))
		when avg(rate) < 4.5 then sum(greatest(0.07 * total_sum, 150))
		when avg(rate) < 4.9 then sum(greatest(0.08 * total_sum, 175))
		else sum(greatest(0.1 * total_sum, 200))
	end + sum(tip_sum)*0.95 as courier_reward_sum

from       dds.fct_deliveries as fct
inner join dds.dm_couriers    as dco on dco.id = fct.courier_id
inner join dds.dm_orders      as ord on ord.id = fct.order_id 
inner join dds.dm_timestamps  as dtt on dtt.id = ord.timestamp_id
where 1=1
  and dtt.year = EXTRACT(YEAR FROM '{{ds}}'::date)
  and dtt.month =  EXTRACT(MONTH FROM '{{ds}}'::date)
group by
    dco.courier_id
   ,dco.courier_name
   ,dtt.year
   ,dtt.month