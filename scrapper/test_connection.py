import sqlalchemy as sa
import boto3
from botocore.client import Config
import urllib

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=master;"
    "UID=sa;"
    "PWD=YourStrongPassword123!;"
)

engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
try:
    with engine.connect() as conn:
        print("connection berhasil")
except Exception as e:
    print(f"connection failed : {e}")

#konfigurasi minIO
s3 = boto3.resource(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id = 'minioadmin',
    aws_secret_access_key = 'minioadmin',
    config = Config(signature_version='s3v4'),
    region_name = 'us-east-1'
) 

try:
    for bucket in s3.buckets.all():
        print(f"dapat terhubung dengan bucket: {bucket.name}")
except Exception as e:
    print(f"gagal terhubung ke s3 bucket: {e}")