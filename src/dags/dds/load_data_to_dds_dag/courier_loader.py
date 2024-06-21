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


class CourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class CourierJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class CourierSTGRepository:
    def load_couriers(self, conn: Connection, threshold: int) -> List[CourierJsonObj]:
        with conn.cursor(row_factory=class_row(CourierJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.deliverysystem_couriers
                    WHERE id > %(threshold)s
                    ORDER BY id ASC;
                """,
                {"threshold": threshold},
            )
            objs = cur.fetchall()
        return objs


class CourierDDSRepository:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s);
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                },
            )

    def get_courier(self, conn: Connection, courier_id: str) -> Optional[CourierObj]:
        with conn.cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        courier_id,
                        courier_name
                    FROM dds.dm_couriers
                    WHERE courier_id = %(courier_id)s;
                """,
                {"courier_id": courier_id},
            )
            obj = cur.fetchone()
        return obj


class CourierLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 100  # Ресторанов мало, пачка может быть небольшая

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.stg_rest = CourierSTGRepository()
        self.dds = CourierDDSRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def parse_couriers(self, raws: List[CourierJsonObj]) -> List[CourierObj]:
        res = []
        for r in raws:
            courier_json = json.loads(r.object_value)
            t = CourierObj(id=r.id,
                           courier_id=courier_json['_id'],
                           courier_name=courier_json['name']
                        )

            res.append(t)
        return res

    def load_couriers(self):
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
            
            load_queue = self.stg_rest.load_couriers(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)

            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            couriers_to_load = self.parse_couriers(load_queue)
            for rest in couriers_to_load:
                existing = self.dds.get_courier(conn, rest.courier_id)
                if not existing:
                    self.dds.insert_courier(conn, rest)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
