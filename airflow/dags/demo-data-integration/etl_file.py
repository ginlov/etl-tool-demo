import os
import sys
import time
import pendulum

sys.path.append(os.path.realpath(
                    os.path.join(__file__, os.pardir)
                ))

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from operators.file_etl import setup_connection, to_mysql
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

requirements=[
    "pandas",
    "pymysql"
]

args={
    "owner": "Giang Vu Long",
    "retries": 10,
    "start_date": days_ago(1)
}

with DAG(
    dag_id="etl_file",
    catchup=False,
    tags=["demo"],
    default_args=args,
    schedule_interval="@daily"
) as dag:

    create_database = MySqlOperator(task_id="create_database_mysql",\
                                    sql=r"""
                                    CREATE DATABASE IF NOT EXISTS etl_db;
                                    USE etl_db;
                                    """,\
                                    mysql_conn_id="mysql")

    create_table = MySqlOperator(task_id="create_table_mysql",\
                                sql=r"""
                                USE etl_db;
                                CREATE TABLE IF NOT EXISTS data_file (
                                   brand varchar(255),
                                   category varchar(255),
                                   mall varchar(255),
                                   product_name varchar(255),
                                   views integer,
                                   price integer,
                                   rate1star integer,
                                   rate2star integer,
                                   rate3star integer,
                                   rate4star integer,
                                   rate5star integer,
                                   rating float,
                                   shop_name varchar(200),
                                   shope_rating integer,
                                   response_rate varchar(200),
                                   ship_ontime varchar(200)
                                )
                                """,
                                mysql_conn_id="mysql")

    insert_data = PythonVirtualenvOperator(
        task_id="insert_data_to_database",
        requirements=requirements,
        python_callable=to_mysql
    )

    create_database >> create_table >> insert_data
