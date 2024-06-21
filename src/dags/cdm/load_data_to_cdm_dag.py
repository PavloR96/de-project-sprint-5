import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from config_const import ConfigConst

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def cdm_load_data_dag():
    
    # Объявляем таски, которые загружают данные.
    update_cdm_settlement_report = PostgresOperator(
        task_id='update_cdm_settlement_report',
        postgres_conn_id=ConfigConst.PG_WAREHOUSE_CONNECTION,
        sql="cdm_dm_settlement_report.sql"
    )

    # Объявляем таски, которые загружают данные.
    update_cdm_courier_ledger = PostgresOperator(
        task_id='update_cdm_courier_ledger',
        postgres_conn_id=ConfigConst.PG_WAREHOUSE_CONNECTION,
        sql="cdm_dm_courier_ledger.sql"
    )

    
    update_cdm_settlement_report
    update_cdm_courier_ledger

stg_bonus_system_ranks_dag = cdm_load_data_dag()
