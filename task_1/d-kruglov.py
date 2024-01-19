from datetime import datetime
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook



DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-kruglov',
    'poke_interval': 600
}

with DAG('d-kruglov',
         schedule_interval='0 6 * * MON-SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-kruglov']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_task = BashOperator(
        task_id='echo_task',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def show_date_now():
        logging.info(f"Current datetime: {datetime.now()}")


    show_date = PythonOperator(
        task_id='show_date_now',
        python_callable=show_date_now,
        dag=dag
    )

    def load_data(**kwargs):
        day_of_week = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
        result = cursor.fetchone()[0]
        logging.info(f'WEEKDAY: {day_of_week}')
        logging.info('Result:')
        logging.info(result)

        #query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение

    load_data_from_gp = PythonOperator(
        task_id='load_data_from_gp',
        python_callable=load_data,
        dag=dag
    )

    dummy >> [echo_task, show_date] >> load_data_from_gp