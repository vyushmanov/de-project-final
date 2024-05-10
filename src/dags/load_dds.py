# DAG which uploads from STG to DDS in Vertica

from datetime import datetime, timedelta
import logging
log = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.contrib.hooks.vertica_hook import VerticaHook

### Vertica settings ###
VERTICA_CONN_ID = 'vertica_conn'
vertica_hook = VerticaHook(vertica_conn_id=VERTICA_CONN_ID)


def currencies_from_stg_to_dds(selected_date, **kargs):
    query_paste_to_vertica_currencies_dds = f"""
    INSERT INTO STV2024031256__DWH.currencies
    SELECT id, date_update, currency_code, currency_code_with, currency_with_div
    FROM (
        SELECT * 
            ,ROW_NUMBER() OVER (PARTITION BY currency_code, currency_code_with, date_update ) as seq 
        FROM STV2024031256__STAGING.currencies
    ) a where a.seq = 1 
        AND date_update::date = '{selected_date}'
        AND id IS NOT NULL
        AND date_update IS NOT NULL
        AND currency_code IS NOT NULL
        AND currency_code_with IS NOT NULL
        AND currency_with_div IS NOT NULL;
    """
    test_connection_query = "select 1;"
    # Write to Vertica
    with vertica_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                if result[0] == 1:
                    log.info(f"Right connection to the Vertica. Continuing ...")
                    cur.execute(query_paste_to_vertica_currencies_dds)
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()


def deduplicate_transactions(selected_date, **kargs):
    query_paste_to_vertica_transactions_dds = f"""
    INSERT INTO STV2024031256__DWH.operations
    SELECT operation_id, account_number_from, account_number_to, currency_code,
            country, status, transaction_type, amount, transaction_dt 
    FROM (
        SELECT * 
            ,ROW_NUMBER() OVER (PARTITION BY operation_id, transaction_dt ) as seq 
        FROM STV2024031256__STAGING.transactions
    ) a
    WHERE a.seq = 1 
            AND transaction_dt::date = '{selected_date}'
            AND account_number_from > 0
            AND operation_id IS NOT NULL
            AND account_number_from IS NOT NULL
            AND account_number_to IS NOT NULL
            AND currency_code IS NOT NULL
            AND country IS NOT NULL
            AND status IS NOT NULL
            AND transaction_type IS NOT NULL
            AND amount IS NOT NULL
            AND transaction_dt IS NOT NULL;
    """
    test_connection_query = "select 1;"
    # Write to Vertica
    with vertica_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                if result[0] == 1:
                    log.info(f"Right connection to the Vertica. Continuing ...")
                    cur.execute(query_paste_to_vertica_transactions_dds)
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
        'ETL_vertica_dds',  # name
        default_args=default_args,  # connect args
        schedule_interval='@daily',  # interval
        start_date=datetime(2022, 10, 1),  # start calc
        catchup=True,  # used in  the first launch, from date in the past until now. Usually = off
        tags=['final', 'project'],
) as dag:
    # create DAG logic (sequence/order)
    start = DummyOperator(task_id="start")
    with TaskGroup("load_DDS_tables") as load_tables:
        dedup_curr = PythonOperator(
            task_id="load_currencies_dds", python_callable=currencies_from_stg_to_dds,
            op_kwargs={'selected_date': '{{ ds }}'},
            provide_context=True
        )
        dedup_trans = PythonOperator(
            task_id="load_transactions_dds", python_callable=deduplicate_transactions,
            op_kwargs={'selected_date': '{{ ds }}'},
            provide_context=True
        )

    end = DummyOperator(task_id="end")

    start >> load_tables >> end