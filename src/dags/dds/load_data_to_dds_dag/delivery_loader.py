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


class DeliveryObj(BaseModel):
    id: int
    delivery_id: Optional[str]
    delivery_ts: Optional[datetime]
    address: Optional[str]


class DeliveryJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class DeliverySTGRepository:
    def load_delivery(self, conn: Connection, threshold: int) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC;
                """,
                {"threshold": threshold},
            )
            objs = cur.fetchall()
        return objs


class DeliveryDDSRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, delivery_ts, address)
                    VALUES (%(delivery_id)s, %(delivery_ts)s, %(address)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET delivery_ts = EXCLUDED.delivery_ts,
                        address = EXCLUDED.address;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "delivery_ts": delivery.delivery_ts,
                    "address": delivery.address
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str) -> Optional[DeliveryObj]:
        with conn.cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        delivery_id
                    FROM dds.dm_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {
                    "delivery_id": delivery_id
                },
            )
            obj = cur.fetchone()
        return obj


class DeliveryLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 100  # Ресторанов мало, пачка может быть небольшая

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.stg_rest = DeliverySTGRepository()
        self.dds = DeliveryDDSRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def parse_deliveries(self, raws: List[DeliveryJsonObj]) -> List[DeliveryObj]:
        res = []
        for r in raws:
            delivery_json = json.loads(r.object_value)
            
            try:
                delivery_ts = datetime.strptime(delivery_json['delivery_ts'], "%Y-%m-%d %H:%M:%S.%f")
            except:
                delivery_ts = datetime.strptime(delivery_json['delivery_ts'], "%Y-%m-%d %H:%M:%S")
                
            t = DeliveryObj(
                id=0,
                delivery_id=delivery_json['delivery_id'],
                delivery_ts=delivery_ts,
                address=delivery_json['address']
            )
            
            res.append(t)
        return res

    def load_deliveries(self):
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
            
            load_queue = self.stg_rest.load_delivery(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)

            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            
            deliveries_to_load = self.parse_deliveries(load_queue)

            for delivery in deliveries_to_load:
                existing = self.dds.get_delivery(conn, delivery.delivery_id)
                if not existing:
                    self.dds.insert_delivery(conn, delivery)

                # Сохраняем прогресс.
                # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
                # либо откатятся все изменения целиком.
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
