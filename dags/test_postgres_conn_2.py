from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_conn_socket(host, port):
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"✅ koneksi ke {host}:{port} berhasil")
    except:
        print(f"❌ koneksi ke {host}:{port} gagal")

with DAG(
    dag_id = 'tes_koneksi_db',
    start_date = datetime(2026,1,1),
    schedule_interval = None,
    catchup = False
) as dag:
    # test koneksi ke postgres
    cek_psql = PythonOperator(
        task_id = 'cek_koneksi_postgres',
        python_callable = test_conn_socket,
        op_kwargs = {'host':'postgres_db', 'port':'5432'}
    )
    # test koneksi ke sql sever
    cek_sql_server = PythonOperator(
        task_id = 'cek_koneksi_mssql',
        python_callable = test_conn_socket,
        op_kwargs = {'host':'sql-server', 'port':'1433'}
    )

    cek_psql >> cek_sql_server