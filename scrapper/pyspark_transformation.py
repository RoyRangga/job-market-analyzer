from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from datetime import datetime
import pandas as pd
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8 '
    'pyspark-shell'
)

def write_to_sql_server(spark, cleaned_df, table_name):
    jdbc_url = "jdbc:sqlserver://sql-server:1433;databaseName=Job_analyzer;encrypt=true;trustServerCertificate=true;"
    connection_properties = {
        "user":"sa",
        "password":"YourStrongPassword123!",
        "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    try:
        existing_data = spark.read \
                .format("jdbc") \
                .option("url",jdbc_url) \
                .option("dbtable", f"(SELECT link FROM {table_name}) as subquery") \
                .option("user", connection_properties["user"]) \
                .option("password", connection_properties["password"]) \
                .option("driver", connection_properties["driver"]) \
                .load()
                # .mode("append") \
                # .save()
        print(f"table {table_name} successfully loaded intu sql server")
        new_data = cleaned_df.join(existing_data, on='link', how='left_anti')

        new_count = new_data.count()
        if new_count > 0:
            new_data.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", connection_properties["user"]) \
                    .option("password", connection_properties["password"]) \
                    .option("driver", connection_properties["driver"]) \
                    .mode("append") \
                    .save()
            print(f"✨berhasil menambahkan {new_count} data baru ke table {table_name}")
        else:
            print(f"ℹ️ Tidak ada data baru untuk {table_name}. Semua link sudah ada di SQL Server.")
    except Exception as e:
        if "Invalid object name" in str(e) or "does not exist" in str(e):
            print(f"table {table_name} masih belum ada di database, melakukan inisiasi awal")
            cleaned_df.write \
                      .format("jdbc") \
                      .option("url", jdbc_url) \
                      .option("dbtable", table_name) \
                      .option("user", connection_properties["user"]) \
                      .option("password", connection_properties["password"]) \
                      .option("driver", connection_properties["driver"]) \
                      .mode("overwrite") \
                      .save()
        else:
            print(f"❌ Error saat menulis ke SQL Server: {e}")

def kalibrr_transform(spark):
    print("processing kalibrr...")
    today = datetime.today().strftime("%Y-%m-%d")
    df = spark.read.option("mergeSchema", "true").parquet(f"s3a://job-market-kalibrr-raw/{today}/*.parquet")
    print(f"reading data from : {today}")
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
        F.to_date(F.col("date_posted")).alias("date_posted")
    ).dropDuplicates(["link"])
    write_to_sql_server(spark, cleaned_df, "stg_kalibrr_raw")

def jobstreet_transform(spark):
    today = datetime.today().strftime("%Y-%m-%d")
    df = spark.read.option("mergeSchema", "true").parquet(f"s3a://job-market-jobstreet-raw/{today}/*.parquet")
    extract_digit = F.coalesce(
        F.regexp_extract(F.col("date_posted_raw"), r"(\d+)", 1).cast("int"),
        F.lit(0)
    )
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
        F.col("date_posted_raw").alias("date_posted_raw"),
        F.when(
            F.col("date_posted_raw").rlike("jam|menit|detik|hour|minute|second"),
            F.current_date()
        ).when(
            F.col("date_posted_raw").rlike("day|hari"),
            F.date_sub(F.current_date(), extract_digit)
        ).when(
            F.col("date_posted_raw").rlike("bulan|month"),
            F.date_sub(F.current_date(), extract_digit * 30)
        ).otherwise(
            F.current_date()
        ).alias("date_posted")
    ).dropDuplicates(["link"])
    write_to_sql_server(spark, cleaned_df, "stg_jobstreet_raw")