from logging import Logger
from typing import List, Optional
import json

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from dds.load_data_to_dds_dag.restaurants_loader import RestaurantDDSRepository
from dds.load_data_to_dds_dag.users_loader import UserDDSRepository
from dds.load_data_to_dds_dag.timestamps_loader import TimestampDDSRepository

from datetime import datetime, timezone, date, time


class OrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str 
    restaurant_id: int
    timestamp_id: int
    user_id: int


class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class OrderSTGRepository:
    def load_orders(self, conn: Connection, threshold: int) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                """,
                {"threshold": threshold},
            )
            objs = cur.fetchall()
        objs.sort(key=lambda x: x.id)

        return objs


class OrderDDSRepository:

    def insert_orders(self, conn: Connection, orders: List[OrderObj]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s);
                """,
                {
                    "user_id": orders.user_id,
                    "restaurant_id": orders.restaurant_id,
                    "timestamp_id": orders.timestamp_id,
                    "order_key": orders.order_key,
                    "order_status": orders.order_status
                },
            )

    def list_orders(self, conn: Connection) -> List[OrderObj]:
        with conn.cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT user_id, restaurant_id, timestamp_id, order_key, order_status
                    FROM dds.dm_orders;
                """
            )
            obj = cur.fetchall()
        return obj
    
    def get_order(self, conn: Connection, order_id: str) -> Optional[OrderObj]:
        with conn.cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_key,
                        restaurant_id,
                        timestamp_id,
                        user_id,
                        order_status
                    FROM dds.dm_orders
                    WHERE order_key = %(order_id)s;
                """,
                {"order_id": order_id},
            )
            obj = cur.fetchone()
        return obj


class OrderLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 100  # Пользователей много, увеличиваем пачку

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.dds_rest = RestaurantDDSRepository()
        self.dds_user = UserDDSRepository()
        self.dds_ts = TimestampDDSRepository()
        self.dds_ord = OrderDDSRepository()
        self.stg_ord = OrderSTGRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    order_key: str
    order_status: str 
    restaurant_id: int
    timestamp_id: int
    user_id: int

    def parse_orders(self, order_raw: OrderJsonObj, restaurant_id: int, user_id: int, timestamp_id: int) -> List[OrderObj]:

        order_json = json.loads(order_raw.object_value)
        t = OrderObj(id=0,
                            order_key = order_json['_id'],
                            order_status = order_json['final_status'],
                            restaurant_id = restaurant_id,
                            user_id = user_id,
                            timestamp_id = timestamp_id
                            )

        return t

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.stg_ord.load_orders(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)

            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                order_json = json.loads(order.object_value)

                restaurant = self.dds_rest.get_restaurant(conn, order_json['restaurant']['id'])
                if not restaurant:
                    break
                
                dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
                timestamp = self.dds_ts.get_timestamp(conn, dt)
                if not timestamp:
                    break

                user  = self.dds_user.get_user(conn, order_json['user']['id'])
                if not user:
                    break

                order_to_load = self.parse_orders(order, restaurant.id, user.id, timestamp.id)
                self.dds_ord.insert_orders(conn, order_to_load)


                # Сохраняем прогресс.
                # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
                # либо откатятся все изменения целиком.
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = order.id
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
