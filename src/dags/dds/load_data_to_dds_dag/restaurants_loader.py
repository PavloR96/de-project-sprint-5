from logging import Logger
from typing import List, Optional
import json

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime


class RestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str 
    active_from: datetime
    active_to: datetime


class RestaurantJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class RestaurantSTGRepository:
    def load_raw_restaurants(self, conn: Connection, threshold: int) -> List[RestaurantJsonObj]:
        with conn.cursor(row_factory=class_row(RestaurantJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                """,
                {"threshold": threshold},
            )
            objs = cur.fetchall()
        return objs


class RestaurantDDSRepository:

    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s);
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[RestaurantObj]:
        with conn.cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        restaurant_id,
                        restaurant_name,
                        active_from,
                        active_to
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj


class RestaurantLoader:
    WF_KEY = "restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 100  # Ресторанов мало, пачка может быть небольшая

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.stg_rest = RestaurantSTGRepository()
        self.dds = RestaurantDDSRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.object_value)
            t = RestaurantObj(id=r.id,
                                 restaurant_id=rest_json['_id'],
                                 restaurant_name=rest_json['name'],
                                 active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                                 active_to=datetime(year=2099, month=12, day=31)
                                 )

            res.append(t)
        return res

    def load_restaurants(self):
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

            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            restaurants_to_load = self.parse_restaurants(load_queue)
            for rest in restaurants_to_load:
                existing = self.dds.get_restaurant(conn, rest.restaurant_id)
                if not existing:
                    self.dds.insert_restaurant(conn, rest)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
