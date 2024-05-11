# DAG which insert in CDM from DDS

from datetime import datetime, timedelta
import logging
log = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook

import sql_helpers

### Vertica settings ###
VERTICA_CONN_ID = 'vertica_conn'
vertica_hook = VerticaHook(vertica_conn_id=VERTICA_CONN_ID)

def push_data_cdm(selected_date, **kargs):
    test_connection_query = "select 1;"
    # Write to Vertica
    with vertica_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                if result[0] == 1:
                    log.info(f"Right connection to the Vertica. Continuing ...")
                    cur.execute(sql_helpers.query_paste_to_vertica_cdm(selected_date))
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()


default_args = {
    'owner': 'Airflow',
    'retries': 1,  # the number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=1),  # delay between retries
    'depends_on_past': False,
}

with DAG(
        'ETL_vertica_cdm',  # name
        default_args=default_args,  # connect args
        schedule_interval='@daily',  # interval
        start_date=datetime(2022, 10, 1),  # start calc
        catchup=True,  # used in  the first launch, from date in the past until now. Usually = off
        tags=['final', 'project'],
) as dag:
    # create DAG logic (sequence/order)
    start = DummyOperator(task_id="start")
    insert_in_cdm = PythonOperator(
        task_id="push_data_cdm", python_callable=push_data_cdm,
        op_kwargs={'selected_date': '{{ ds }}'},
        provide_context=True, dag=dag
    )
    end = DummyOperator(task_id="end")

    start >> insert_in_cdm >> end
