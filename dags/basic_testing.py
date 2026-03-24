from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id = 'tes_connection',
    start_date = datetime(2026,1,1),
    schedule_interval = None,
    catchup = False
) as dag:
    cek_python = BashOperator(
        task_id = 'cek_versi_python',
        bash_command = 'python --version'
    )
    cek_folder_python = BashOperator(
        task_id = 'cek_folder_scrapper',
        bash_command = "ls /opt/airflow/scrapper"
    )

cek_python >> cek_folder_python