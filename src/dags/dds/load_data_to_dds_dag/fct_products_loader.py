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
from dds.load_data_to_dds_dag.products_loader import ProductDDSRepository, ProductObj

from datetime import datetime, timezone, date, time


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str

class FctProductObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class ProductPaymentJsonObj:
    def __init__(self, d: Dict) -> None:
        self.product_id: str = d["product_id"]
        self.product_name: str = d["product_name"]
        self.price: float = d["price"]
        self.quantity: int = d["quantity"]
        self.product_cost: float = d["product_cost"]
        self.bonus_payment: float = d["bonus_payment"]
        self.bonus_grant: float = d["bonus_grant"]


class BonusEventSTGRepository:

    def load_raw_events(self, conn: Connection, event_type: str, threshold: int) -> List[EventObj]:
        with conn.cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = %(event_type)s
                      AND id > %(threshold)s
                    ORDER BY id ASC;
                """,
                {
                    "event_type": event_type,
                    "threshold": threshold
                }
            )
            objs = cur.fetchall()
        return objs


class BonusPaymentJsonObj:
    EVENT_TYPE = "bonus_transaction"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.order_id: str = d["order_id"]
        self.order_date: datetime = datetime.strptime(d["order_date"], "%Y-%m-%d %H:%M:%S")
        self.product_payments = [ProductPaymentJsonObj(pp) for pp in d["product_payments"]]


class FctProductDDSRepository:

    def insert_facts(self, conn: Connection, facts: List[FctProductObj]) -> None:
        with conn.cursor() as cur:
            for fact in facts:
                cur.execute(
                    """
                        INSERT INTO dds.fct_product_sales(
                            order_id,
                            product_id,
                            count,
                            price,
                            total_sum,
                            bonus_payment,
                            bonus_grant
                        )
                        VALUES (
                            %(order_id)s,
                            %(product_id)s,
                            %(count)s,
                            %(price)s,
                            %(total_sum)s,
                            %(bonus_payment)s,
                            %(bonus_grant)s
                        )
                        /*ON CONFLICT (order_id, product_id) DO UPDATE
                        SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant
                            */
                        ;
                    """,
                    {
                        "product_id": fact.product_id,
                        "order_id": fact.order_id,
                        "count": fact.count,
                        "price": fact.price,
                        "total_sum": fact.total_sum,
                        "bonus_payment": fact.bonus_payment,
                        "bonus_grant": fact.bonus_grant
                    },
                )



class FctProductsLoader:
    EVENT_TYPE = "bonus_transaction"
    WF_KEY = "fct_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    _LOG_THRESHOLD = 100  # Пользователей много, увеличиваем пачку

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.stg_ev = BonusEventSTGRepository()
        self.dds_ord = OrderDDSRepository()
        self.dds_prod = ProductDDSRepository()
        self.dds_fct = FctProductDDSRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def parse_order_products(self
                             ,order_raw: BonusPaymentJsonObj
                             ,order_id: int
                             ,products: Dict[str, ProductObj]
                             ) -> Tuple[bool, List[FctProductObj]]:
        res = []

        for prod_json in order_raw.product_payments:
            if prod_json.product_id not in products:
                return(False, [])
            
            t = FctProductObj(id=0,
                                 order_id=order_id,
                                 product_id=products[prod_json.product_id].id,
                                 count=prod_json.quantity,
                                 price=prod_json.price,
                                 total_sum=prod_json.product_cost,
                                 bonus_grant=prod_json.bonus_grant,
                                 bonus_payment=prod_json.bonus_payment
                                 )
            res.append(t)

        return (True, res)

    def load_fct_products(self):
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

            load_queue = self.stg_ev.load_raw_events(conn, self.EVENT_TYPE, last_loaded)
            load_queue.sort(key=lambda x: x.id)

            self.log.info(f"Found {len(load_queue)} events to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            products = self.dds_prod.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p


            proc_cnt = 0
            for payment_raw in load_queue:
                payment_obj = BonusPaymentJsonObj(json.loads(payment_raw.event_value))
                order = self.dds_ord.get_order(conn, payment_obj.order_id)
                if not order:
                    self.log.info(f"Not found order {payment_obj.order_id}. Finishing.")
                    continue

                (success, facts_to_load) = self.parse_order_products(payment_obj, order.id, prod_dict)
                if not success:
                    self.log.info(f"Could not parse object for order {order.id}. Finishing.")
                    continue

                self.dds_fct.insert_facts(conn, facts_to_load)

                proc_cnt += 1
                if proc_cnt % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processing events {proc_cnt} out of {len(load_queue)}.")
                                  

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
