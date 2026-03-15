import pymysql
pymysql.install_as_MySQLdb()
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import duckdb
import json


def detect_new_calls():
    last_time = Variable.get("last_loaded_call_time", default_var='2000-01-01 00:00:00')
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    sql = f"SELECT call_id FROM calls WHERE call_time > '{last_time}'"
    df = mysql_hook.get_pandas_df(sql)

    print(f"Number of new record in MySQL: {len(df)}")

    return df['call_id'].tolist()



def load_telephony_details(ti):
    ids_from_mysql = ti.xcom_pull(task_ids='detect_new_calls')
    final_list = []
    for call_id in ids_from_mysql:
        path = f"/Users/gorodok/airflow_hw/telephony_data/{call_id}.json"
        with open(path, "r") as file:
            data_from_json = json.load(file)
            final_list.append(data_from_json)

    print(f"Number of loaded JSONs: {len(final_list)}")

    return final_list



def transform_and_load_duckdb(ti):
    telephony_data = ti.xcom_pull(task_ids='load_telephony_details')
    if not telephony_data:
        return

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    sql = """
        SELECT c.call_id, c.call_time, c.phone, c.direction, c.status,
               CONCAT(e.name, ' ', e.surname) as full_name
        FROM calls c
        JOIN employees e ON c.employee_id = e.employee_id
    """
    db_df = mysql_hook.get_pandas_df(sql)
    json_df = pd.DataFrame(telephony_data)
    final_df = db_df.merge(json_df, on='call_id')

    final_df = final_df[final_df['duration_sec'] >= 0]
    final_df = final_df.drop_duplicates(subset=['call_id'])

    conn_db = duckdb.connect("/Users/gorodok/airflow_hw/calls_analytics.db")
    conn_db.execute("""
        CREATE TABLE IF NOT EXISTS support_call_enriched (
            call_id INTEGER PRIMARY KEY, 
            call_time TIMESTAMP, 
            phone VARCHAR,
            direction VARCHAR, 
            status VARCHAR, 
            full_name VARCHAR, 
            duration_sec INTEGER, 
            short_description TEXT
        );
    """)

    for i, row in final_df.iterrows():
        row_values = row.tolist()
        conn_db.execute("INSERT OR REPLACE INTO support_call_enriched VALUES (?,?,?,?,?,?,?,?)", row_values)

    max_time = final_df['call_time'].max()
    Variable.set("last_loaded_call_time", str(max_time))

    print(f"Number of loaded rows: {len(final_df)}")

    conn_db.close()



default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "catchup": False
}

with DAG(
        "pipeline",
        default_args=default_args,
        schedule="@hourly",
        catchup=False,
) as dag:
    t1 = PythonOperator(task_id="detect_new_calls", python_callable=detect_new_calls)
    t2 = PythonOperator(task_id="load_telephony_details", python_callable=load_telephony_details)
    t3 = PythonOperator(task_id="transform_and_load_duckdb", python_callable=transform_and_load_duckdb)

    t1 >> t2 >> t3