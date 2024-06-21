import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator

from dds.load_data_to_dds_dag.users_loader import UserLoader
from dds.load_data_to_dds_dag.restaurants_loader import RestaurantLoader
from dds.load_data_to_dds_dag.timestamps_loader import TimestampLoader
from dds.load_data_to_dds_dag.products_loader import ProductLoader
from dds.load_data_to_dds_dag.orders_loader import OrderLoader
from dds.load_data_to_dds_dag.courier_loader import CourierLoader
from dds.load_data_to_dds_dag.delivery_loader import DeliveryLoader
from dds.load_data_to_dds_dag.fct_products_loader import FctProductsLoader
from dds.load_data_to_dds_dag.fct_deliveriy_loader import FctDelyveriesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'stg', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_load_data_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    #origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    
    # Объявляем таски, которые загружают данные.
    @task(task_id="start_dag")
    def begin():
        DummyOperator(task_id="start_dag")

    # Объявляем таски, которые загружают данные.
    @task(task_id="middle_dummy_task")
    def middle_dummy():
        DummyOperator(task_id="middle_dummy_task")

    @task(task_id="load_dm_users")
    def load_dm_users():
        # создаем экземпляр класса, в котором реализована логика.
        user_loader = UserLoader(dwh_pg_connect, log)
        user_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_dm_restaurants")
    def load_dm_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        restaurant_loader = RestaurantLoader(dwh_pg_connect, log)
        restaurant_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_dm_timestamps")
    def load_dm_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        timestamp_loader = TimestampLoader(dwh_pg_connect, log)
        timestamp_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_dm_products")
    def load_dm_products():
        # создаем экземпляр класса, в котором реализована логика.
        product_loader = ProductLoader(dwh_pg_connect, log)
        product_loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_dm_orders")
    def load_dm_orders():
        # создаем экземпляр класса, в котором реализована логика.
        order_loader = OrderLoader(dwh_pg_connect, log)
        order_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_dm_couriers")
    def load_dm_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_dm_deliveries")
    def load_dm_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        delivery_loader = DeliveryLoader(dwh_pg_connect, log)
        delivery_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.


    @task(task_id="load_fct_product_sales")
    def load_fct_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        fct_product_sales = FctProductsLoader(dwh_pg_connect, log)
        fct_product_sales.load_fct_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_fct_deliveries")
    def load_fct_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        fct_deliveries = FctDelyveriesLoader(dwh_pg_connect, log)
        fct_deliveries.load_fct_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    start_dag = begin()

    users_dict = load_dm_users()
    restaurants_dict = load_dm_restaurants()
    timestamps_dict = load_dm_timestamps()
    
    couriers_dict = load_dm_couriers()
    deliveries_dict = load_dm_deliveries()

    middle_dummy_task = middle_dummy()
    
    products_dict = load_dm_products()
    orders_dict = load_dm_orders()
    
    fct_products_dict = load_fct_product_sales()
    fct_deliveries_dict = load_fct_deliveries()
    

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    start_dag >> [users_dict, restaurants_dict, timestamps_dict, couriers_dict, deliveries_dict] >> middle_dummy_task >> [products_dict, orders_dict] >> fct_products_dict >> fct_deliveries_dict

stg_bonus_system_ranks_dag = dds_load_data_dag()
