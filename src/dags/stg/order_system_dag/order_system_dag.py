import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from stg.order_system_dag.pg_savers import RestaurantPgSaver, UserPgSaver, OrderPgSaver
from stg.order_system_dag.loaders import RestaurantLoader, UserLoader, OrderLoader
from stg.order_system_dag.readers import RestaurantReader, UserReader, OrderReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_order_system():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_res = RestaurantPgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect_res = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader_res = RestaurantReader(mongo_connect_res)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader_res = RestaurantLoader(collection_reader_res, dwh_pg_connect, pg_saver_res, log)

        # Запускаем копирование данных.
        loader_res.run_copy()

    @task()
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_us = UserPgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader_us = UserReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader_us = UserLoader(collection_reader_us, dwh_pg_connect, pg_saver_us, log)

        # Запускаем копирование данных.
        loader_us.run_copy()

    @task()
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver_ord = OrderPgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader_ord = OrderReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader_ord = OrderLoader(collection_reader_ord, dwh_pg_connect, pg_saver_ord, log)

        # Запускаем копирование данных.
        loader_ord.run_copy()

    restaurant_loader = load_restaurants()
    user_loader = load_users()
    order_loader = load_orders()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurant_loader >> user_loader >> order_loader  # type: ignore


order_stg_dag = stg_order_system()  # noqa
