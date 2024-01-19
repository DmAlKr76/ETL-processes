from airflow import DAG
from airflow.utils.dates import days_ago
import requests
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

from d_kruglov_plugins.d_kruglov_top_locations_operator import DKruglovLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-kruglov',
    'poke_interval': 600
}

with DAG('d-kruglov-lesson-5',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-kruglov_lesson_5']) as dag:

    start = DummyOperator(task_id='start')

    def insert_locations_to_gp(**kwargs):
        data = kwargs['templates_dict']['top_locations']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        creating_table = "CREATE TABLE IF NOT EXISTS d_kruglov_ram_location (id int UNIQUE, name varchar, type varchar, dimension varchar, resident_cnt int)"
        truncating_table = "TRUNCATE TABLE d_kruglov_ram_location"
        inserting_values = f"INSERT INTO d_kruglov_ram_location VALUES {','.join(data)}"

        try:
            pg_hook.run(creating_table, False)
            pg_hook.run(truncating_table, False)
            pg_hook.run(inserting_values, False)
        except AirflowException as e:
            logging.warning("ERROR WHILE RUN PG_HOOK : {}".format(e))


    get_top_locations = DKruglovLocationsOperator(
        task_id='get_top_locations',
        top_count=3
    )

    insert_locations_to_gp = PythonOperator(
        task_id='insert_locations_to_gp',
        python_callable=insert_locations_to_gp,
        templates_dict={'top_locations': '{{ti.xcom_pull(task_ids="get_top_locations")}}'},
        provide_context=True
    )

    start >> get_top_locations >> insert_locations_to_gp
