import logging

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.dummy_operator import DummyOperator
from config_const import ConfigConst
from lib.pg_connect import ConnectionBuilder
from stg.delivery_system_dag.loader import Loader
from stg.delivery_system_dag.pg_savers import PgSaver

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
)
def stg_delivery_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    api_url = Variable.get(ConfigConst.API_URL)
    headers = {'X-Nickname': Variable.get(ConfigConst.X_NICKNAME),
               'X-Cohort': Variable.get(ConfigConst.X_COHORT),
               'X-API-KEY': Variable.get(ConfigConst.X_API_KEY)}

    # Объявляем таски, которые загружают данные.
    @task(task_id="start_dag")
    def begin():
        DummyOperator(task_id="start_dag")

    @task()
    def load_couriers():
        couriers_pg_saver = PgSaver(dwh_pg_connect)
        couriers_loader = Loader(api_url, headers, couriers_pg_saver, log)
        couriers_loader.load_object('couriers', '_id', 'asc', 50)

    @task()
    def load_deliveries():
        deliveries_pg_saver = PgSaver(dwh_pg_connect)
        deliveries_loader = Loader(api_url, headers, deliveries_pg_saver, log)
        deliveries_loader.load_object('deliveries', 'delivery_id', 'asc', 50)


    start_dag = begin()
    courier_loader = load_couriers()
    delivery_loader = load_deliveries()

    start_dag >> [courier_loader, delivery_loader]

delivery_stg_dag = stg_delivery_system()  # noqa
