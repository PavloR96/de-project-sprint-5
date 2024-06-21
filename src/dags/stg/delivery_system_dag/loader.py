from logging import Logger
from datetime import datetime, timedelta
from bson.objectid import ObjectId

from stg.delivery_system_dag.pg_savers import PgSaver
from lib.dict_util import to_dict

from typing import Dict, List
import requests


class Loader:
    _LOG_THRESHOLD = 100

    def __init__(self, api_url, headers, pg_saver: PgSaver, logger: Logger) -> None:
        self.api_url = api_url
        self.headers = headers
        self.pg_saver = pg_saver
        self.log = logger


    def get_data_from_api(self, object_name: str, sort_field: str, sort_direction: str, limit: int) -> List[Dict]:
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        date_to = (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        offset = 0
        
        docs = []
        docs_offset = requests.get(
            url = self.api_url + f'/{object_name}?from={date_from}&to={date_to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
            headers=self.headers).json()
        
        while docs_offset:
            docs += docs_offset
            offset += 50
            docs_offset = requests.get(
                url = self.api_url + f'/{object_name}?from={date_from}&to={date_to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}',
                headers=self.headers).json()
        return docs


    def load_object(self, object_name: str, sort_field: str, sort_direction: str, limit: int) -> int:
        data = self.get_data_from_api(object_name, sort_field, sort_direction, limit)
        self.log.info(f"Found {len(data)} objects to sync from {object_name}.")

        #Очищаем, т.к. грузим только за послдение 7 дней
        self.pg_saver.delete_table(object_name)

        for d in data:
            d_parsed_ids = to_dict(d)
            self.pg_saver.save_object(object_name, str(d[sort_field]), datetime.now(), d_parsed_ids)

        self.log.info(f"Finishing work. {len(data)} objects of {object_name} have been uploaded")

        return len(data)