from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
import pandas as pd
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8 '
    'pyspark-shell'
)

def write_to_sql_server(cleaned_df, table_name):
    cleaned_df.write \
              .format("jdbc") \
              .option("url", "jdbc:sqlserver://sql-server:1433;databaseName=Job_analyzer;encrypt=true;trustServerCertificate=true;") \
              .option("dbtable", table_name) \
              .option("user", "sa") \
              .option("password", "YourStrongPassword123!") \
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .mode("append") \
              .save()
    print(f"table {table_name} successfully loaded intu sql server")

def kalibrr_transform(spark):
    print("processing kalibrr...")
    df = spark.read.option("mergeSchema", "true").parquet("s3a://job-market-kalibrr-raw/*.parquet")
    cleaned_df = df.select(
        F.col("link"),
        F.col("site2").alias("source_site"),
        F.col("location"),
        F.col("role"),
        F.col("job_type"),
        F.concat_ws("\n", F.col("description")).alias("job_description"),
        F.concat_ws("\n", F.col("minimum_qualifications")).alias("minimum_qualifications"),
        F.col("position"),
        F.col("spesialisasi").alias("specialization"),
        F.col("syarat_pendidikan").alias("minimum_educcation"),
        F.col("industry"),
        F.col("is_business_verified").cast(LongType()),
        F.to_date(F.col("date_posted")).alias("date_posted"),
        # F.current_timestamp().alias("processed_at")
    ).dropDuplicates(["link"])
    write_to_sql_server(cleaned_df, "stg_kalibrr_raw")

def jobstreet_transform(spark):
    df = spark.read.option("mergeSchema", "true").parquet("s3a://job-market-jobstreet-raw/*.parquet")
    cleaned_df = df.select(
        F.col("link"),
        F.col("site2").alias("source_site"),
        F.col("location"),
        F.col("role"),
        F.col("company"),
        F.col("job_type"),
        F.col("job_detail_classification"),
        F.concat_ws("\n", F.col("minimum_qualifications")).alias("minimum_qualifications"),
        F.col("all_description").alias("job_description"),
        F.expr("date_sub(current_date(), cast(regexp_extract(date_posted_string, '(\\d+)', 1) as int))").alias("date_posted"),
        # F.current_timestamp().alias("processed_at")
    ).dropDuplicates(["link"])
    write_to_sql_server(cleaned_df, "stg_jobstreet_raw")