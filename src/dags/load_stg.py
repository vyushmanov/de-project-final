# DAG which downloads from Postgres and Uploading to STG Vertica

import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Set

log = logging.getLogger(__name__)

from airflow import DAG
from airflow.models.variable import Variable
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

### PostgreSQL settings ###
pg_conn = PostgresHook('postgres_conn').get_conn()
table_list = [{'table': 'currencies', 'dt_column': 'date_update'}
              ,{'table': 'transactions', 'dt_column': 'transaction_dt'}]

### Vertica settings ###
vertica_user = Variable.get("VERTICA_USER")
vertica_hook = VerticaHook(vertica_conn_id='vertica_conn')

default_args = {
    'owner': 'Airflow',
    'retries': 1,  # the number of retries that should be performed before failing the task
    'retry_delay': timedelta(minutes=1),  # delay between retries
    'depends_on_past': False,
}

def extract_table(table_dict:Dict, selected_date:datetime, path_temp_csv:str, **kwargs) -> None:
    path_to_temp = f"{path_temp_csv}/{table_dict['table']}_{selected_date}.csv"
    query_fetch_from_postgres_currencies = f"""
        SELECT *
        FROM public.{table_dict['table']}
        WHERE {table_dict['dt_column']}::date = '{selected_date}';"""
    test_connection_query = "SELECT 1;"
    with pg_conn as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                log.info(f'Result = {result}')
                if result[0] == 1:
                    log.info(f"Right connection to the {table_dict['table']}. Continuing ...")
                    cur.execute(query_fetch_from_postgres_currencies)
                    df = pd.DataFrame(cur.fetchall())
                    # log.info(currencies_df.values)
                    log.info(f"{len(df)} lines were received from {table_dict['table']} table for {selected_date}")
                    ## Save output from postgres to Vertica
                    write_df = df.to_csv(
                        path_or_buf=path_to_temp,
                        sep=';',
                        mode="w",
                        header=False,
                        encoding='utf-8',
                        index=False
                    )
                    log.info(f"{path_to_temp} recorded")
                else: log.info("Something went wrong")
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()


def push_to_vertica(table_schema:str, table_dict:Dict, selected_date:datetime, path_temp_csv:str, **kwargs) -> None:
    path_transactions = f"{path_temp_csv}/{table_dict['table']}_{selected_date}.csv"
    query_get_column_list = f"""
            SELECT ordinal_position,
                   column_name
            FROM v_catalog.columns
            WHERE table_name = '{table_dict['table']}'
                and table_schema = '{table_schema}__STAGING'
                and column_name != 'id'
            order by 1            
            """
    test_connection_query = "select 1;"
    with vertica_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(test_connection_query)
                result = cur.fetchone()
                log.info(f'Result = {result}')
                if result[0] == 1:
                    log.info(f"Right connection to the {table_dict['table']}. Continuing ...")
                    cur.execute(query_get_column_list)
                    df = pd.DataFrame(cur.fetchall())
                    table_col = ','.join([i for i in df[1]])
                    query_paste_to_vertica = f"""
                        COPY {table_schema}__STAGING.{table_dict['table']} 
                            ({table_col})
                        FROM LOCAL '{path_transactions}'
                        DELIMITER ';'
                        REJECTED DATA AS TABLE {table_schema}__STAGING.{table_dict['table']}_rej;
                        """
                    log.info(query_paste_to_vertica)
                    cur.execute(query_paste_to_vertica)
            except:
                raise ValueError("Errors detected. Stopping...")
        cur.close()
        conn.commit()


def remove_temp_files(table_list:list, selected_date:datetime, path_temp_csv:str, **kargs) -> None:
    for table_dict in table_list:
        path = f"{path_temp_csv}/{table_dict['table']}_{selected_date}.csv"
        os.remove(path)
        log.info(f"{path} has been removed")

with DAG(
        'ETL_vertica_stg',  # name
        default_args=default_args,  # connect args
        schedule_interval='@daily',  # interval
        start_date=datetime(2022, 10, 1),  # start calc
        catchup=True,  # used in  the first launch, from date in the past until now. Usually = off
        tags=['final', 'project'],
) as dag:
    # create DAG logic (sequence/order)
    start = DummyOperator(task_id="start")
    list_of_read_task = []
    list_write_task = []
    for table_dict in table_list:
        extract_vertica = PythonOperator(
            task_id=f"extract_{table_dict['table']}_src", python_callable=extract_table,
            op_kwargs={'table_dict': table_dict, 'selected_date': '{{ ds }}', 'path_temp_csv': '/lessons/temp'},
            dag=dag)
        list_of_read_task.append(extract_vertica)
        load_vertica = PythonOperator(
            task_id=f"load_{table_dict['table']}_stg", python_callable=push_to_vertica,
            op_kwargs={'table_schema': vertica_user, 'table_dict': table_dict,
                       'selected_date': '{{ ds }}', 'path_temp_csv': '/lessons/temp'},
            dag=dag)
        list_write_task.append(load_vertica)
    remove_files = PythonOperator(
        task_id="remove_temp_files", python_callable=remove_temp_files,
        op_kwargs={'table_list': table_list, 'selected_date': '{{ ds }}', 'path_temp_csv': '/lessons/temp'},
        dag=dag)
    end = DummyOperator(task_id="end")

    chain(start, list_of_read_task, list_write_task, remove_files, end)
