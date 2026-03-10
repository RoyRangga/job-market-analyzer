from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8 '
    'pyspark-shell'
)

def transform_job_data():
    spark = SparkSession.builder \
        .appName("JobMarketTransformation") \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8")\
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    # read raw data from MinIO
    # raw_kalibrr = spark.read.parquet("s3a://job-market-raw/*.parquet")
    raw_jobstreet = spark.read.parquet("s3a://job-market-jobstreet-raw/*.parquet")

    # transformasi (pemindahan data dari MinIO ke sal server).
    cleaned_df = raw_jobstreet.select(
        F.col("site2").alias("site2"),
        F.col("link2").alias("link2"),
        F.upper(F.col("site")).alias("site"),
        F.col("role").alias("role"),
        # F.col("location").alias("location"),
        F.coalesce(
            F.col("location").alias("location") if "location" in raw_jobstreet.columns else F.lit(None),
            F.col("alamat").alias("alamat") if "alamat" in raw_jobstreet.columns else F.lit(None)
        ).alias("location"),
        F.col("company").alias("company"),
        F.col("job_type").alias("job_type"),
        F.concat_ws("\n", F.col("job_detail_classification")).alias("job_detail_classification"),
        F.concat_ws("\n", F.col("minimum_qualifications")).alias("minimum_qualifications"),
        F.concat_ws("\n", F.col("all_description")).alias("job_description"),
        # F.expr("date_sub(current_date(), cast(F.regexp_extract(F.col('date_Posted_string'), r'(\d+)', 1) as int)).alias(date_posted)"),
        F.expr("date_sub(current_date(), cast(regexp_extract('date_posted_string', '(\\d+)', 1) as int))").alias("date_posted"),
        F.current_timestamp().alias("processed at")
    )

    # Simpan ke SQL Server
    cleaned_df.write \
              .format("jdbc") \
              .option("url", "jdbc:sqlserver://sql-server:1433;databaseName=Job_analyzer;encript=true;trustServerCertificate=true;") \
              .option("dbtable", "stg_job_listings_raw") \
              .option("user", "sa") \
              .option("password", "YourPassword123") \
              .mode("append") \
              .save()


transform_job_data()