import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class DKruglovLocationsOperator(BaseOperator):
    template_fields = ('top_count'),
    def __init__(self, top_count, **kwargs):
        super().__init__(**kwargs)
        self.top_count = top_count

    def execute(self, context):
        top_cnt = self.top_count
        api_url = 'https://rickandmortyapi.com/api/location/'
        r = requests.get(api_url)
        if r.status_code == 200:
            top_cnt = 3
            top_locations = []
            for record in r.json()['results']:
                location = (record['id'], record['name'], record['type'], record['dimension'], len(record['residents']))
                top_locations.append(location)
            top_locations.sort(key=lambda x: x[-1], reverse=True)

            logging.info(f'Top {top_cnt} locations:')
            logging.info(top_locations[:top_cnt])

            return top_locations[:top_cnt]
        else:
            logging.warning("HTTP STATUS CODE: {}".format(r.status_code))
            raise AirflowException('Error in load api_url')