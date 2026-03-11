from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
import time
import pandas as pd
import re
import urllib
from sqlalchemy import create_engine
import boto3
import io
# import pyspark

def get_sql_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=Job_Analyzer;"
        "UID=sa;"
        "PWD=YourStrongPassword123!;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def upload_to_minio(data, bucket_name, object_name):
    #konfigurasi koneksi
    if not data or len(data) == 0:
        print(f"gagal upload ke Minio karena data {object_name} kosong")
        return

    s3_client = boto3.client(
        's3',
        # endpoint_url = 'http://localhost:9000',#ini untuk koneksi dari komputer ke 9000
        endpoint_url = 'http://minio:9000',#ini untuk koneksi dari kontainer minio ke port 9000
        aws_access_key_id = 'minioadmin',
        aws_secret_access_key = 'minioadmin'
    )
    #ubah data-datalist menjadi string
    df = pd.DataFrame(data=data)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: "\n".join(x) if isinstance(x, list) else x)
    #upload to the minio
    try:
        parquet_buffer = df.to_parquet(index=False, engine='pyarrow')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=parquet_buffer,
            ContentType='application/octet-stream'
        )
    except Exception as e:
        print(f"storing data ke minio gagal karena: {e}")

def init_browser():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--remote-debugging-port=9222')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-extensions')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')

    options.add_argument(r'--profile-directory=Default')
    options.add_argument(r'--user-data-dir=C:\Users\royra\job-market-analyzer\chrome_profile')
    prefs = {
        "profile.default_content_setting_values.notifications":1,
        "profile.default.local_discovery":1,
        "profile.default.geolocation":1,
        "profile.default_content_setting_values.media_stream":1,

    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def scrape_kaliber(main_url):
    driver = init_browser()
    driver.get(main_url)
    time.sleep(3)

    list_jobs=[]
    try:
        job_listings = driver.find_elements(By.XPATH, "//h2[contains(@data-tooltip-id, 'job-title-tooltip-[object Object]')]/a")
        for link in job_listings:
            try:
                single_link = link.get_attribute("href")
                list_jobs.append({
                    "link": single_link,
                    "site": "Kalibrr" # Jaminan: site tidak akan NULL
                })
            except Exception as e:
                print(f"gagal tarik data: {e}")
        for item in list_jobs:
            driver.get(item['link'])
            time.sleep(3)
            try:
                #retrieve data alamat
                item['site2']="Kalibrr"
                alamat=driver.find_element(By.XPATH, "//span[contains(@itemtype, 'PostalAddress')]")
                item['location']=alamat.text
                #retrieve data role
                role=driver.find_element(By.XPATH, "//h1[contains(@itemprop, 'title')]")
                item['role']=role.text
                #tiper kerjaan
                job_type=driver.find_element(By.XPATH, "//a[contains(@href, '/home/t/')]").text
                item['job_type']=job_type
                # mengambil deskripsi pekerjaan
                description=driver.find_elements(By.XPATH, "//div[contains(@itemprop, 'description')]//*[self::li or self::p]")
                item['description']=[d.text.strip() for d in description if d.text.strip()]
                # mengambil minimum kualifikasi
                min_qualification=driver.find_elements(By.XPATH, "//div[contains(@itemprop, 'qualifications')]//*[self::li or self::p]")
                item['minimum_qualifications']=[d.text.strip() for d in min_qualification if d.text.strip()]
                #mengambil tingkatan posisi
                tingkat_posisi=driver.find_element(By.XPATH, "//dt[text()='Tingkat Posisi']/following-sibling::dd//a").text
                item['position']=tingkat_posisi
                #mengambil spesialisasi
                item['spesialisasi']=driver.find_element(By.XPATH, "//dt[text()='Spesialisasi']/following-sibling::dd//a").text
                #mengambil syarat pendidikan
                item['syarat_pendidikan']=driver.find_element(By.XPATH, "//dt[text()='Persyaratan tingkat pendidikan']/following-sibling::dd//a").text
                item['industry']=driver.find_element(By.XPATH, "//dt[text()='Industri']/following-sibling::dd//span").text
                try:
                    item['situs']=driver.find_element(By.XPATH, "//dt[text()='Situs']/following-sibling::dd//a").get_attribute('href')
                except:
                    item['situs']='N/A'
                verified=driver.find_element(By.XPATH, "//div[contains(@data-original-title, 'verified-business')]")
                if verified:
                    is_ver=1
                else:
                    is_ver=0
                item['is_business_verified']=is_ver
                #mengambil waktu upload job
                item['date_posted'] = driver.find_element(By.XPATH, "//span[contains(@itemprop, 'datePosted')]").get_attribute("textContent")
                item['date_posted'] = datetime.fromisoformat(item['date_posted']).strftime("%Y-%m-%d")
                item['valid_through']=driver.find_element(By.XPATH, "//span[contains(@itemprop, 'validThrough')]").get_attribute("textContent")
                item['valid_through'] = datetime.fromisoformat(item['valid_through']).strftime("%Y-%m-%d")
            except Exception as e:
                print(f"gagal tarik data: {e}")
    except Exception as e:
        print(f"scripping gagal: {e}")
    finally:
        driver.quit()
    return list_jobs

def scrap_jobstreet(main_url):
    driver=init_browser()
    driver.get(main_url)
    list_jobs=[]
    try:
        all_links = driver.find_elements(By.XPATH, "//div/a[contains(@data-automation, 'jobTitle')]")
        all_date_posted = driver.find_elements(By.XPATH, "//span[contains(@data-automation, 'jobListingDate')]")
        semua_link = [i.get_attribute("href") for i in all_links]
        semua_date = [i.get_attribute("textContent").strip() for i in all_date_posted]
        print(semua_link)

        for i in range(len(all_links)):
            single_link = all_links[i].get_attribute("href")
            single_date = semua_date[i]
            list_jobs.append({'link':single_link, "site":"Jobstreet", "date_posted_raw":single_date})#####

        print(list_jobs) 

        for item in list_jobs:
            driver.get(item['link'])
            time.sleep(2)
            try:
                item["site2"]="Jobstreet"
                #mendapatkan lokasi job
                try:
                    wait_element_location = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//span[contains(@data-automation, 'job-detail-location')]/a"))
                    )
                    item["location"]=wait_element_location.text
                except Exception as e:
                    print(f"gagal menarik data location, karena: {e}")
                    item["location"] = "N/A"
                #mendapatkan nama job
                try:
                    wait_element_job_type = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//span[contains(@data-automation, 'job-detail-work-type')]/a"))
                    )
                    item["job_type"]= wait_element_job_type.text

                except Exception as e:
                    print(f"gagal menarik data job_detail_work_type, karena: {e}")
                    item["job_type"] = "N/A"
                item["position_name"] = driver.find_elements(By.XPATH, "//div/a[contains(@data-automation, 'jobTitle')]")
                item["role"]=driver.find_element(By.XPATH, "//h1[contains(@data-automation, 'job-detail-title')]").text
                #mendapatkan nama perusahaan
                item["company"]=driver.find_element(By.XPATH, "//span[contains(@data-automation, 'advertiser-name')]").text
                item["job_detail_classification"]=driver.find_element(By.XPATH, "//span[contains(@data-automation, 'job-detail-classifications')]//a").text
                xpath_keywords=(
                    "//div[contains(@data-automation, 'jobAdDetails')]//p["
                    "contains(., 'Kualifikasi') or "
                    "contains(., 'Qualifications') or "
                    "contains(., 'Qualification') or "
                    "contains(., 'Requirement') or "
                    "contains(., 'Requirements')"
                    "]/following::ul[1]//*[self::li or self::p] | " #jalur 1 "ul"
                    "//div[contains(@data-automation, 'jobAdDetails')]//p["
                    "contains(., 'Kualifikasi') or "
                    "contains(., 'Qualifications') or "
                    "contains(., 'Qualification') or "
                    "contains(., 'Requirement') or "
                    "contains(., 'Requirements')"
                    "]/following::ol[1]//*[self::li or self::p]" #jalur 2 "ol"
                )
                minimum_qualifications=driver.find_elements(By.XPATH, xpath_keywords)
                item["minimum_qualifications"]=[element.text.strip() for element in minimum_qualifications if element.text.strip()]
                XPATH_DESC_2 = (
                    "//div[contains(@data-automation, 'jobAdDetails')]"
                )
                item["all_description"] = driver.find_element(By.XPATH, XPATH_DESC_2).text

            except Exception as e:
                print(f"error pada penarikan data joob street: {e}")
    except Exception as e:
        print(f"error pada penarikan data joob street: {e}")
    time.sleep(2)
    return list_jobs

#proses data scrapping

