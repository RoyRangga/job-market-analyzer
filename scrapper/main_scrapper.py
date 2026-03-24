import scrappingFunction
# import pyspark_transformation
from datetime import datetime, timedelta
import time
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

# spark = SparkSession.builder \
#     .appName("JobMarketTransformation") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

scrapping_kaliber = scrappingFunction.scrape_kaliber
scrapping_jobstreet = scrappingFunction.scrap_jobstreet
upload_minIO = scrappingFunction.upload_to_minio
# kalibrr_load = pyspark_transformation.kalibrr_transform
# jobstreet_load = pyspark_transformation.jobstreet_transform

url_kaliber = "https://www.kalibrr.id/id-ID/home/te/data"
url_jobstreet = "https://id.jobstreet.com/id/Data-jobs"

try:
    data_kaliber = scrapping_kaliber(main_url=url_kaliber)
    current_date = datetime.now().strftime("%Y-%m-%d")
    path_minIO = f"s3a://job-market-kalibrr-raw/{current_date}/data.parquet"
    bucket_name_klbr = 'job-market-kalibrr-raw'
    file_name = f"{current_date}/kalibrr_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    upload_minIO(data_kaliber, bucket_name_klbr, file_name)

    data_jobstreet = scrapping_jobstreet(main_url=url_jobstreet)
    path_minIO = f"s3a://job-market-jobstreet-raw/{current_date}/data.parquet"
    file_name = f"{current_date}/jobstreet_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    # upload_minIO(data_jobstreet, 'job-market-jobstreet-raw', file_name)
    bucket_name_jbstr = 'job-market-jobstreet-raw'
    upload_minIO(data_jobstreet, bucket_name_jbstr, file_name)

    # 3. Print hasil (Opsional, hanya untuk cek)
    print(f"Berhasil mengambil {len(data_kaliber)} data dari Kalibrr")
    print(f"Berhasil mengambil {len(data_jobstreet)} data dari Jobstreet")

    # kalibrr_load(spark)
    # jobstreet_load(spark)

    print("data raw berhasil masuk ke sql sever")
except Exception as e:
    print(f"error message: {e}")