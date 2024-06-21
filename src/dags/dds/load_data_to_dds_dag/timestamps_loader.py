from logging import Logger
from typing import List, Optional
from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime, timezone, date, time



class TimestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date 
    time: time


class TimestampsSTGRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamp_threshold: int, limit: int) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                SELECT
                    id
                    ,object_value::json ->> 'date' as ts
                    ,DATE_PART('year', (object_value::json ->> 'date')::timestamp) as year
                    ,DATE_PART('month', (object_value::json ->> 'date')::timestamp) as month
                    ,DATE_PART('day', (object_value::json ->> 'date')::timestamp) as day
                    ,(object_value::json ->> 'date')::date as date
                    ,(object_value::json ->> 'date')::time as time

                FROM stg.ordersystem_orders
                WHERE 1=1
                  and object_value::json ->> 'final_status' in ('CLOSED','CANCELLED')
                  and id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamp_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestampDDSRepository:

    def insert_timestamp(self, conn: Connection, timestamp: TimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s);
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "date": timestamp.date,
                    "time": timestamp.time
                },
            )

    def get_timestamp(self, conn: Connection, dt: datetime) -> Optional[TimestampObj]:
        with conn.cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(dt)s;
                """,
                {"dt": dt},
            )
            obj = cur.fetchone()
        return obj

class TimestampLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Заказов много, увеличиваем пачку

    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dwh
        self.stg = TimestampsSTGRepository(pg_dwh)
        self.dds = TimestampDDSRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
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
            load_queue = self.stg.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for timestamp in load_queue:
                self.dds.insert_timestamp(conn, timestamp)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
