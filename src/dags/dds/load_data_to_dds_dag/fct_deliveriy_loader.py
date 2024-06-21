from logging import Logger
from typing import List, Dict, Tuple
import json

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from dds.load_data_to_dds_dag.orders_loader import OrderDDSRepository
from dds.load_data_to_dds_dag.courier_loader import CourierDDSRepository
from dds.load_data_to_dds_dag.delivery_loader import DeliveryDDSRepository, DeliverySTGRepository, DeliveryJsonObj

from datetime import datetime, timezone, date, time


class FctDeliveryObj(BaseModel):
    order_id: int
    delivery_id: int
    courier_id: int
    rate: float
    tip_sum: float
    total_sum: float



class FctDeliveryDDSRepository:

    def insert_facts(self, conn: Connection, facts: List[FctDeliveryObj]) -> None:
        with conn.cursor() as cur:
            for fact in facts:
                cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(
                        order_id,
                        delivery_id,
                        courier_id,
                        rate,
                        tip_sum,
                        total_sum
                    )
                    VALUES (
                        %(order_id)s,
                        %(delivery_id)s,
                        %(courier_id)s,
                        %(rate)s,
                        %(tip_sum)s,
                        %(total_sum)s
                    );
                """,
                {
                    "order_id": facts.order_id,
                    "delivery_id": facts.delivery_id,
                    "courier_id": facts.courier_id,
                    "rate": facts.rate,
                    "tip_sum": facts.tip_sum,
                    "total_sum": facts.total_sum
                },
            )



class FctDelyveriesLoader:
    WF_KEY = "fct_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.stg_del = DeliverySTGRepository()
        self.dds_ord = OrderDDSRepository()
        self.dds_cour = CourierDDSRepository()
        self.dds_del = DeliveryDDSRepository()
        self.dds_fct = FctDeliveryDDSRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def parse_delivery(self
                             ,delivery: DeliveryJsonObj
                             ,order_id: int
                             ,delivery_id: int
                             ,courier_id: int
                             ) -> FctDeliveryObj:
        
        delivery_json = json.loads(delivery.object_value)
            
        t = FctDeliveryObj(
            order_id=order_id,
            delivery_id=delivery_id,
            courier_id=courier_id,
            rate=delivery_json['rate'],
            tip_sum=delivery_json['tip_sum'],
            total_sum=delivery_json['sum']
        )
        return t

    def load_fct_deliveries(self):
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

            load_queue = self.stg_del.load_delivery(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)

            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery_obj in load_queue:
                delivery_json = json.loads(delivery_obj.object_value)

                delivery = self.dds_del.get_delivery(conn, delivery_json['delivery_id'])
                if not delivery:
                    break

                order = self.dds_ord.get_order(conn, delivery_json['order_id'])
                if not order:
                    break

                courier = self.dds_cour.get_courier(conn, delivery_json['courier_id'])
                if not courier:
                    break


                delivery_to_load = self.parse_delivery(delivery_obj, order.id, courier.id, delivery.id)
                self.dds_fct.insert_facts(conn, delivery_to_load)

                                
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([delivery_obj.id for delivery_obj in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
