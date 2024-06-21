from logging import Logger
from typing import List
import json

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from dds.load_data_to_dds_dag.restaurants_loader import (RestaurantDDSRepository, RestaurantJsonObj,
                                   RestaurantSTGRepository)

from datetime import datetime, timezone, date, time


class ProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str 
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int


class ProductDDSRepository:

    def insert_product(self, conn: Connection, products: List[ProductObj]) -> None:
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                        VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s);
                    """,
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "product_price": product.product_price,
                        "active_from": product.active_from,
                        "active_to": product.active_to,
                        "restaurant_id": product.restaurant_id
                    },
                )

    def list_products(self, conn: Connection) -> List[ProductObj]:
        with conn.cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj

class ProductLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 100  # Пользователей много, увеличиваем пачку

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.stg_rest = RestaurantSTGRepository()
        self.dds = ProductDDSRepository()
        self.dds_rest = RestaurantDDSRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def parse_restaurants_menu(self, restaurant_raw: RestaurantJsonObj, restaurant_version_id: int) -> List[ProductObj]:
        res = []
        rest_json = json.loads(restaurant_raw.object_value)
        for prod_json in rest_json['menu']:
            t = ProductObj(id=0,
                              product_id=prod_json['_id'],
                              product_name=prod_json['name'],
                              product_price=prod_json['price'],
                              active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                              active_to=datetime(year=2099, month=12, day=31),
                              restaurant_id=restaurant_version_id
                              )

            res.append(t)
        return res

    def load_products(self):
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

            load_queue = self.stg_rest.load_raw_restaurants(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)

            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            products = self.dds.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p

            for restaurant in load_queue:
                restaurant_version = self.dds_rest.get_restaurant(conn, restaurant.object_id)
                if not restaurant_version:
                    return

                products_to_load = self.parse_restaurants_menu(restaurant, restaurant_version.id)
                products_to_load = [p for p in products_to_load if p.product_id not in prod_dict]
                self.dds.insert_product(conn, products_to_load)


            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
