CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_restaurants(
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pk PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS dds.dm_products (
	id serial4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) DEFAULT 0 NOT NULL,
	restaurant_id int4 NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pk PRIMARY KEY (id),
	CONSTRAINT dm_products_price_check CHECK ((product_price >= (0)::numeric)),
    CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);


CREATE TABLE IF NOT EXISTS dds.dm_timestamps(
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pk PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

CREATE TABLE IF NOT EXISTS dds.dm_users(
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dds.dm_orders(
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_pk PRIMARY KEY (id),
    CONSTRAINT dm_orders_restaurant_id_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_orders_timestamp_id_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
    CONSTRAINT dm_orders_user_id_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id)
);


CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 DEFAULT 0 NOT NULL,
	price numeric(14, 2) DEFAULT 0 NOT NULL,
	total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_payment numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_grant numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pk PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric)),
    CONSTRAINT dm_fct_product_sales_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT dm_fct_product_sales_product_id_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id)
);


CREATE TABLE IF NOT EXISTS dds.dm_couriers(
    id serial4 NOT NULL,
    courier_id varchar NOT NULL UNIQUE,
    courier_name varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries(
    id serial4 NOT NULL,
	delivery_id varchar UNIQUE NOT NULL,
    delivery_ts timestamp NOT NULL,
    address varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries(
    id serial4 NOT NULL,  
    order_id int NOT NULL,
	delivery_id	int NOT NULL,
	courier_id int NOT NULL,
    rate int NOT NULL DEFAULT 0 CHECK(rate >= 0 AND rate <= 5),
	tip_sum numeric (14, 2) NOT NULL DEFAULT 0 CHECK (tip_sum >= 0),
	total_sum numeric (14, 2) NOT NULL DEFAULT 0 CHECK (total_sum >= 0),
	CONSTRAINT fct_deliveries_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT fct_deliveries_delivery_id_fk FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id),
    CONSTRAINT fct_deliveries_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL,
    settlement_year int NOT NULL CHECK(settlement_year >= 2020 AND settlement_year < 2500),
    settlement_month int NOT NULL CHECK(settlement_month >= 0 AND settlement_month <= 12),
    orders_count int NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    rate_avg numeric(19, 5),
    order_processing_fee numeric(19, 5) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_tips_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    courier_reward_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0)
);