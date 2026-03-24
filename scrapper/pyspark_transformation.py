from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import argparse
url_kaliber = "https://www.kalibrr.id/id-ID/home/te/data"
url_jobstreet = "https://id.jobstreet.com/id/Data-jobs"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
    'com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8 '
    'pyspark-shell'
)

# spark = SparkSession.builder \
#     .appName("JobMarketTransformation") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()


def write_to_sql_server(spark, cleaned_df, table_name):
    jdbc_url = "jdbc:sqlserver://sql-server:1433;databaseName=Job_analyzer;encrypt=true;trustServerCertificate=true;"
    connection_properties = {
        "user":"sa",
        "password":"<YOUR_PASSWORD>",
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
        F.lower(F.col("location")).alias("location"),
        F.lower(F.col("role")).alias("role"),
        F.lower(F.col("salary")).alias("salary"),
        F.lower(F.col("company")).alias("company"),
        F.lower(F.col("job_type")).alias("job_type"),
        F.concat_ws("\n", F.col("description")).alias("job_description"),
        F.concat_ws("\n", F.col("minimum_qualifications")).alias("minimum_qualifications"),
        F.lower(F.col("position")).alias("position"),
        F.lower(F.col("spesialisasi")).alias("specialization"),
        F.col("syarat_pendidikan").alias("minimum_educcation"),
        F.lower(F.col("industry")).alias("industry"),
        # F.col("is_business_verified").cast(IntegerType()).alias("is_business_verified"),
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
        F.lower(F.col("site2")).alias("source_site"),
        F.lower(F.col("location")).alias("location"),
        F.lower(F.col("role")).alias("role"),
        F.col("salary"),
        F.lower(F.col("company")).alias("company"),
        F.lower(F.col("job_type")).alias("job_type"),
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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run Pyspark Transformation for Kalibrr or Jobstreet")
    parser.add_argument(
        '--source',
        type=str,
        required=True,
        choices=['kalibrr', 'jobstreet'],
        help="pilih sumber data: 'kalibrr' atau 'jobstreet' "
    )
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("JobMarketTransformation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    if args.source == 'kalibrr':
        kalibrr_transform(spark)
    elif args.source == 'jobstreet':
        jobstreet_transform(spark)

    spark.stop()