import os
import sys
import pendulum

sys.path.append(os.path.realpath(
                    os.path.join(__file__, os.pardir)
                ))

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from operators.api_etl import crawl_job, transform_data, to_mysql
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

requirements=[
    "pymysql",
    "pandas"
]

args={
    "owner": "Giang Vu Long",
    "retries": 10,
    "start_date": days_ago(1)
}

with DAG(
    dag_id="etl_api",
    catchup=False,
    tags=["demo"],
    default_args=args,
    schedule_interval="@daily"
) as dag:
    crawl_task = PythonVirtualenvOperator(
        task_id="crawl",
        requirements=requirements,
        python_callable=crawl_job
    )

    transform_task = PythonVirtualenvOperator(
        task_id="transform",
        requirements=requirements,
        python_callable=transform_data
    )
    load_to_mysql = PythonVirtualenvOperator(
        task_id="load_to_mysql",
        requirements=requirements,
        python_callable=to_mysql
    )
    crawl_task >> transform_task >> load_to_mysql
