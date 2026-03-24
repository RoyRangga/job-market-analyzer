from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'Roy',
    'start_date':datetime(2026,3,23),
    'retries':1,
    'retry_delay':timedelta(minutes=1) 
}

with DAG(
    dag_id = 'job_market_orchestration',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dag :
    task_scrape = BashOperator(
        task_id = 'run_main_scrapper',
        bash_command = 'python /opt/airflow/scrapper/main_scrapper.py'
    )
    task_spark_kalibrr = BashOperator(
        task_id = 'run_spark_kalibrr', 
        bash_command = 'python /opt/airflow/scrapper/pyspark_transformation.py --source kalibrr'
    )
    task_spark_jobstreet = BashOperator(
        task_id = 'run_spark_jobstreet', 
        bash_command = 'python /opt/airflow/scrapper/pyspark_transformation.py --source jobstreet'
    )
    task_enrichment = BashOperator(
        task_id = 'run_ai_enrinchment',
        bash_command = 'python /opt/airflow/scrapper/scrappingFunction.py'
    )
    task_dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_transformation && dbt run --profiles-dir .'
    )

    task_scrape >> [task_spark_jobstreet, task_spark_kalibrr] >> task_enrichment >> task_dbt_run