import json
from datetime import datetime

from lib.pg_connect import PgConnect
from lib.dict_util import to_dict

class PgSaver:
    def __init__(self, connect: PgConnect) -> None:
        self.conn = connect.client()

    def insert_value(self, object_name: str, id: str, update_ts: datetime, val: str):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_{object_name}(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value;
                """.format(object_name=object_name),
                {
                    "id": id,
                    "val": val,
                    "update_ts": update_ts
                }
            )
            self.conn.commit()

    def save_object(self, object_name: str, id: str, update_ts: datetime, val) -> None:
        str_val = json.dumps(to_dict(val))
        self.insert_value(object_name, id, update_ts, str_val)

    def delete_table(self, object_name: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                    DELETE FROM stg.deliverysystem_{object_name};
                """.format(object_name=object_name)
            )
            self.conn.commit()

