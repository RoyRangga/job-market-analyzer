from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def simulate_scrape():
    file_path = "/opt/airflow/scrapper/hasil_scrapping.csv"
    print(f"data berhasil disimpan di : {file_path}")

    return file_path

with DAG(
    dag_id = "simulasi_pipeline_analyzer",
    start_date = datetime(2026, 3, 1),
    schedule_interval = None,
    catchup = False
) as dag:
    task_scrape = PythonOperator(
        task_id = 'scrape_data',
        python_callable = simulate_scrape
    )

    task_cek_file = BashOperator(
        task_id = 'cek_file_result',
        bash_command = "ls -l {{ ti.xcom_pull(task_ids='scrape_data') }}"
    )

    task_scrape >> task_cek_file