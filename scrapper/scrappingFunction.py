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
from sqlalchemy import create_engine, text, Table, Column, String, MetaData, inspect
import boto3
import io
import requests
# import pyspark
url_kaliber = "https://www.kalibrr.id/id-ID/home/te/data"
url_jobstreet = "https://id.jobstreet.com/id/Data-jobs"
def get_sql_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=Job_Analyzer;"
        "UID=sa;"
        "PWD=<YOUR_PASSWORD>;"
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
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=parquet_buffer.getvalue(),
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
    # options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

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

    target_clicks = 2
    for i in range(target_clicks):
        try:
            load_more_btn = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//button[normalize-space()='Load more jobs']"))
            )
            driver.execute_script("arguments[0].click();", load_more_btn)
            print(f"berhasil klik button 'Load more jobs' ke-{i+1}")

            time.sleep(4)

        except Exception as e:
            print(f"tombol Load more jobs sudah di klik sebanyak {i+1} kali")
            break

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
                #salary
                try:
                    salary_list_str = driver.find_elements(By.XPATH, "//div[contains(@itemprop, 'baseSalary')]/preceding-sibling::span")
                    salary_list = [i.get_attribute('textContent').replace('\xa0', ' ') for i in salary_list_str]
                    salary_list = [i.get_attribute('textContent') for i in salary_list_str]
                    salary = "".join(salary_list)
                    if salary:
                        item['salary'] = salary
                    else:
                        item['salary']='not specified'
                except:
                    item['salary']='not specified'
                # company
                item['company'] = driver.find_element(By.XPATH, "//h1[contains(@itemprop, 'title')]/following-sibling::span/a/h2").text
                # tipe kerjaan
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
                # mengambil data posisi job
                item["position_name"] = driver.find_elements(By.XPATH, "//div/a[contains(@data-automation, 'jobTitle')]")
                # mengambil data role
                item["role"]=driver.find_element(By.XPATH, "//h1[contains(@data-automation, 'job-detail-title')]").text
                # mengambil data gaji
                try:
                    item['salary']=driver.find_element(By.XPATH, "//span[contains(@data-automation, 'job-detail-salary')]").text
                except:
                    item['salary']='not specified'
                # mendapatkan nama perusahaan
                item["company"]=driver.find_element(By.XPATH, "//span[contains(@data-automation, 'advertiser-name')]").text
                # mendapatkan detail job
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
                # mengambil minimum kualifikasi
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

def extract_hard_skill(text):
    OLLAMA_URL = "http://ollama:11434/api/generate"
    OLLAMA_MODEL = "llama3.2"
    if not text or pd.isna(text):
        return None
    system_prompt = (
    "You are a strict data cleaner. Your task is to extract ONLY the HARD SKILLS. "
    "STRICT RULES:\n"
    "1. NO majors, NO departments, NO subjects (e.g., delete 'in Computer Science', 'Akuntansi', etc.)\n"
    "2. ONLY keep: C#, Microsoft Azure, python, R, Excel, Kafka, Airflow, DBT, Microsoft Office, MongoDB, Tableau, or equivalent programming languate, Framework, Platform or Tools.\n"
    # "3. If the text says 'Bachelor degree in IT', output ONLY 'Bachelor Degree'.\n"
    "3. Output must be short. No sentences."
    "4. If you find multiple output, separate with ','."
    "5. If you find no hard skills or tools, respond with 'Not Specified'. "
    "6. Strictly prohibited: introductory text, headers, lists, or explanation. "
    "7. Do not say 'Here is' or 'Here are'."
    )
    user_prompt = f"""Task: HARD SKILLS only (Programming languate/ tools/ framework). No major, No degree.
                        Text: "Design, build, and maintain scalable ETL/ELT data pipelines to process and transform large datasets. Develop and optimize data models, data workflows, and database queries for performance and reliability. Manage and maintain data infrastructure across cloud platforms. Integrate data from multiple sources including databases, APIs, and streaming platforms. Ensure data quality, integrity, and availability across systems. Collaborate with cross-functional teams including data analysts, data scientists, and software engineers to support data initiatives. Troubleshoot and resolve data-related issues while continuously improving pipeline performance. Bachelor's degree of Computer Science or related field. 2–5 years of experience in Data Engineering or a related role. 2–5 years of experience in Data Engineering or a related role. Strong programming skills in Python and advanced SQL. Strong programming skills in Python and advanced SQL. Experience building and maintaining ETL/ELT pipelines. Experience building and maintaining ETL/ELT pipelines. Familiarity with cloud platforms such as AWS, Google Cloud Platform, or Microsoft Azure. Familiarity with cloud platforms such as AWS, Google Cloud Platform, or Microsoft Azure. Experience working with relational databases such as PostgreSQL or MySQL. Experience working with relational databases such as PostgreSQL or MySQL. Basic knowledge of big data and streaming technologies (e.g., Apache Kafka). Basic knowledge of big data and streaming technologies (e.g., Apache Kafka). Strong analytical and problem-solving skills with the ability to work collaboratively in a team environment. Strong analytical and problem-solving skills with the ability to work collaboratively in a team environment. Willing to work onsite in banking industry (Tangerang). Willing to work onsite in banking industry (Tangerang)."
                        Output: Python, SQL, ETL/ELT, AWS, Google Cloud, Microsoft Azure, PostgreSQL, MySQL, Kafka

                        Text: "Minimum Bachelor's degree Minimum Bachelor's degree Strong proficiency in Excel/data management skills Strong proficiency in Excel/data management skills Strong in stakeholders management, problem-solving, logical and analytical thinking skills Strong in stakeholders management, problem-solving, logical and analytical thinking skills Detailed-oriented and able to solve problems efficiently in a dynamic working environment. Detailed-oriented and able to solve problems efficiently in a dynamic working environment. Strong ability to analyze data and convert insights into strategic recommendations Strong ability to analyze data and convert insights into strategic recommendations Have a good eye for detail and are attentive and accurate when entering data Have a good eye for detail and are attentive and accurate when entering data Self–starter, work independently with limited guidance, solution-oriented and team player Self–starter, work independently with limited guidance, solution-oriented and team player Critical thinkers can analyze situations from multiple perspectives Critical thinkers can analyze situations from multiple perspectives	Job Description : Generate business insights through data analysis, and provide recommendations to improve business performance accordingly Work closely with Regional & Area leaders and in driving Area strategy planning by processing local insight and combine it with data analysis Develop business plan and translate it to action items, collaborate with other vast stakeholders, determine success metrics, and provide the result takeaways Go-to person for any new products/service launch Able to make key business decisions or provide solid recommendation based on data/analytical thinking  Requirement : Minimum Bachelor's degree  Strong proficiency in Excel/data management skills Strong in stakeholders management, problem-solving, logical and analytical thinking skills Detailed-oriented and able to solve problems efficiently in a dynamic working environment. Strong ability to analyze data and convert insights into strategic recommendations Have a good eye for detail and are attentive and accurate when entering data Self–starter, work independently with limited guidance, solution-oriented and team player Critical thinkers can analyze situations from multiple perspectives"
                        Output: Excel

                        Text: "What's your expected monthly basic salary? Which of the following types of qualifications do you have? How many years' experience do you have as a data analyst? How many years' experience do you have using SQL queries? Which of the following programming languages are you experienced in? Which of the following data analytics tools are you experienced with? Do you have experience in a sales role? Kualifikasi: 1. Pendidikan: Sarjana dalam bidang yang relevan, seperti Statistika, Matematika, Ilmu Komputer, atau Ekonomi. 2. Pengalaman: Pengalaman dalam analisis data, terutama dalam industri distribusi consumer goods. 3. Kemampuan Analisis: Kemampuan analisis data yang kuat, termasuk kemampuan untuk mengidentifikasi pola dan tren. 4. Kemampuan Komunikasi: Kemampuan komunikasi yang efektif untuk mempresentasikan hasil analisis kepada manajemen. 5. Kemampuan Teknis: Kemampuan menggunakan perangkat lunak analisis data, seperti Excel, SQL, Python, R, atau Tableau. 6. Bersedia bekerja dari senin - sabtu (setengah hari) 7. Penempatan Kalideres, Jakarta Barat  Keahlian: 1. Analisis Data: Keahlian dalam analisis data, termasuk kemampuan untuk mengidentifikasi pola dan tren. 2. Penggunaan Perangkat Lunak: Keahlian dalam menggunakan perangkat lunak analisis data, seperti Excel, SQL, Python, R, atau Tableau. 3. Komunikasi: Keahlian dalam komunikasi yang efektif untuk mempresentasikan hasil analisis kepada manajemen. 4. Problem-Solving: Keahlian dalam memecahkan masalah dan mengidentifikasi peluang terbaik Tugas dan Tanggung Jawab: 1. Analisis Data: Mengumpulkan, menganalisis, dan menginterpretasikan data penjualan,   distribusi, dan lain-lain untuk memahami tren dan pola. 2. Pengembangan Laporan: Membuat laporan yang akurat dan tepat waktu untuk membantu manajemen dalam pengambilan keputusan. 3. Identifikasi Masalah: Mengidentifikasi masalah dan peluang perbaikan dalam proses distribusi dan penjualan. 4. Rekomendasi: Memberikan rekomendasi kepada manajemen berdasarkan hasil analisis data. 5. Pengembangan Model: Mengembangkan model analisis data untuk memprediksi penjualan, distribusi, dan lain-lain."
                        Output: SQL, Excel, Python, R, Tableau

                        Text: "{text}"
                        Output:"""

    full_prompt = f"System: {system_prompt}\nUser: {user_prompt}"
    # prompt = f"""Extract only technical hard skills (tools, programming languages, certifications) from the text below.
    # Output: Comma-separated list only. No intro, no commentary.
    # Text: {text}"""    
    playload = {
        'model':OLLAMA_MODEL,
        'prompt':full_prompt, 
        'stream':False,
        'options':{
            'temperature':0
        }
    }
    try:
        response = requests.post(OLLAMA_URL, json=playload, timeout=300)
        if response.status_code == 200:
            result = response.json().get('response', '').replace('\n', ', ').strip('- ').strip()
            print(f"Hasil Ekstraksi: {result}")
            return result
        else:
            print(f"Ollama Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Error Ollama: {e}")
        return None

def extract_education_req(text):
    OLLAMA_URL = "http://ollama:11434/api/generate"
    OLLAMA_MODEL = "llama3.2"
    if not text or pd.isna(text):
        return None
    
    system_prompt = (
        "You are a strict data cleaner. Your task is to extract ONLY the education LEVEL. "
        "STRICT RULES:\n"
        "1. NO majors, NO departments, NO subjects (e.g., delete 'in Computer Science', 'Akuntansi', etc.)\n"
        "2. ONLY keep: S1, S2, D3, SMA, Bachelor, Master, Diploma, Degree, or equivalent.\n"
        "3. If the text says 'Bachelor degree in IT', output ONLY 'Bachelor Degree'.\n"
        "4. Output must be short. No sentences."
        "5. If you find no education level, respond with 'Not Specified'. "
        "6. Strictly prohibited: introductory text, headers, lists, or explanation. "
        "7. Do not say 'Here is' or 'The level is'."
    )
    
    # Kita berikan contoh "Input -> Output" agar AI paham pola pemotongannya
    user_prompt = f"""Task: Extract level only (S1/D3/Bachelor/etc). No major.
                        Text: "Bachelor's degree in Information Systems or Informatics Engineering"
                        Output: Bachelor's Degree

                        Text: "Diploma sederajat, D3/S1 Akuntansi atau Manajemen"
                        Output: D3, S1

                        Text: "Minimal S1 Psikologi"
                        Output: S1

                        Text: "{text}"
                        Output:"""

    full_prompt = f"System: {system_prompt}\nUser: {user_prompt}"
    playload = {
        'model':OLLAMA_MODEL,
        'prompt':full_prompt,
        'stream':False,
        'options':{
            'temperature':0,
            'stop': ["\n", "Note:", "Here is", "1.", "Here are"]
        }
    }
    try:
        response = requests.post(OLLAMA_URL, json=playload, timeout=300)
        if response.status_code == 200:
            result = response.json().get('response', '')
            print(f"hasil ekstraksi: {result}")
            return result
        else:
            print(f"Ollama Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Error Ollama: {e}")
        return None

def process_skill_enrinchment():
    # enginge for sql server
    src_engine = create_engine('mssql+pyodbc://sa:<YOUR_PASSWORD>@sql-server:1433/Job_analyzer?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes')
    # engine for postgre
    dest_engine = create_engine('postgresql://postgres2:<YOUR_PASSWORD>@postgres_db:5432/job_market_db')
    # pulling data from sql server

    print("Membaca data staging dari SQL Server")
    metadata = MetaData()
    table_name = 'clean_job_skills'
    # make sure 'link' menjadi primary key (dan otomatis bertipe Text/String)
    job_skills_table = Table(
        table_name, metadata,
        Column('link', String, primary_key=True),
        Column('hard_skills', String)
    )
    # create table if it doesn't exist
    metadata.create_all(dest_engine)
    print(f"pengecekan table {table_name} selesai...")
    query = """
            SELECT link, minimum_qualifications, job_description
            FROM stg_jobstreet_raw
            UNION ALL
            SELECT link, minimum_qualifications, job_description
            FROM stg_kalibrr_raw
            """
    df = pd.read_sql(query, src_engine)
    df['full_context'] = df[['minimum_qualifications', 'job_description']].fillna('').agg('\n'.join, axis=1)
    # read the existing links
    try:
        Existing_links = pd.read_sql(f"SELECT link from {table_name} ", dest_engine)['link'].to_list()
    except Exception:
        Existing_links = []

    df_to_process = df[~df['link'].isin(Existing_links)]
    total_process = len(df_to_process)

    print(f"total data di staging: {len(df)}")
    print(f"data yang sudah pernah diproses: {len(Existing_links)}")
    print(f"memulai extraksi AI untuk {total_process} data baru...")

    for i, (index, row) in enumerate(df_to_process.iterrows(),1):
        skills = extract_hard_skill(row['full_context'])
        # proses ekstraksi
        if skills:
            single_row_df = pd.DataFrame([{'link':row['link'], 'hard_skills':skills}])
            try:
                # simpan data ke postgres
                print("menyimpan ke Postgres")
                single_row_df.to_sql(table_name, dest_engine, if_exists='append', index=False)
                print(f"[{i}/{total_process}] ✅ Berhasil : {row['link'][:30]}...")
            except Exception as e:
                print(f"[{i}/{total_process}] ❌ Gagal simpan: {e}")

def process_education_enrinchment():
    src_engine = create_engine('mssql+pyodbc://sa:<YOUR_PASSWORD>@sql-server:1433/Job_analyzer?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes')
    dest_engine = create_engine('postgresql://postgres2:<YOUR_PASSWORD>@postgres_db:5432/job_market_db')
    print("membaca data staging dari sql server")
    metadata = MetaData()
    table_name = 'clean_education'
    education_table = Table(
        table_name, metadata,
        Column('link', String, primary_key=True),
        Column('education', String)
    )
    metadata.create_all(dest_engine)
    print(f"pengecekan table {table_name} selesai")
    query = """
            SELECT link, minimum_qualifications
            FROM stg_jobstreet_raw
            UNION ALL
            SELECT link, minimum_qualifications
            FROM stg_kalibrr_raw;
            """
    df = pd.read_sql(query, src_engine)
    try:
        existing_links = pd.read_sql(f"SELECT link FROM {table_name}", dest_engine)['link'].to_list()
    except Exception as e:
        existing_links = []
    df_to_process = df[~df['link'].isin(existing_links)]
    total_processed = len(df_to_process)
    print(f"total data di staging: {len(df)}")
    print(f"data yang sudah pernah diproses: {len(df_to_process)}")
    print(f"memulai extraksi AI untuk {total_processed} data baru...")

    for i, (index, row) in enumerate(df_to_process.iterrows(), 1):
        edu = extract_education_req(row["minimum_qualifications"])
        if edu:
            singgle_row_df = pd.DataFrame({'link':row['link'], 'education':[edu]})
            try:
                print("menyimpan ke Postgres")
                singgle_row_df.to_sql(table_name, dest_engine, if_exists='append', index=False)
                print(f"[{i}/{total_processed}] ✅ Berhasil : {row['link'][:30]}...")
            except Exception as e:
                print(f"[{i}/{total_processed}] ❌ Gagal simpan: {e}")                

def extract_major(text):
    OLLAMA_URL = "http://ollama:11434/api/generate"
    OLLAMA_MODEL = "llama3.2"
    if not text or pd.isna(text):
        return None
    system_prompt = """You are an expert and strict data cleaner. Your task is to extract ONLY the major or discipline names.
                        STRICT RULES:
                        1. NO degree levels (remove S1, S2, D3, Bachelor, Master, etc.).
                        2. NO introductory text, explanations, or notes.
                        3. If no specific major is mentioned, output 'Any Major'.
                        4. Separate multiple majors with a comma.
                        5. Output ONLY the names of the disciplines.
                        6. Do Not include sentence 'I can help you with that'.
                        7. If no major is found, output 'Not Specified'.
                        """
    user_prompt = f"""
                TASK: Extract only Major/Discipline names.
                Input: Pendidikan S-1, dengan jurusan MIPA (Matematika/ Fisika) atau IT: Teknik Komputer -> Output: MIPA, IT, Teknik Komputer

                Input: Pendidikan minimal S1 Teknik Industri atau S1 Statistika -> Output: Teknik Industri, Statistika

                Input: Bachelor's degree in Information Systems, Computer Science, or Informatics Engineering -> Output: Information Systems, Computer Science, Informatics Engineering

                Input: Minimum Bachelor's degree Strong proficiency in Excel -> Output: Any Major

                Input: Pendidikan minimal SMA/SMK sederajat / Diploma (Fresh Graduate dipersilakan melamar) -> Output: Any Major

                Input: {text} -> Output: """

    full_prompt = f"System: {system_prompt} \n User: {user_prompt}"

    playload = {
        'model': OLLAMA_MODEL,
        'prompt': full_prompt,
        'stream': False,
        'options':{
            'temperature': 0,
            'stop': ["\n", "System:", "User:", "Input:", "I can", "I'll"]
        }
    }
    response = requests.post(OLLAMA_URL, json=playload, timeout=300)
    try:
        if response.status_code == 200:
            result = response.json().get('response','')
            return result
        else:
            print(f"OLLAMA ERROR: {response.status_code} : {response.text}")
            return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def process_major_enrinchment():
    src_engine = create_engine('mssql+pyodbc://sa:<YOUR_PASSWORD>@sql-server:1433/Job_analyzer?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes')
    dest_engine = create_engine('postgresql://postgres2:<YOUR_PASSWORD>@postgres_db:5432/job_market_db')
    print("membaca data staging")
    metadata = MetaData()
    table_name = 'clean_major_tb'
    major_table = Table(
        table_name,
        metadata,
        Column('link', String, primary_key=True),
        Column('major', String)
    )
    metadata.create_all(dest_engine)
    print(f"pengecekan table {table_name} selesai")
    query = """
        SELECT link, minimum_qualifications
        FROM stg_jobstreet_raw
        UNION ALL
        SELECT link, minimum_qualifications
        FROM stg_kalibrr_raw;
        """
    df = pd.read_sql(query, src_engine)
    try:
        Existing_links = pd.read_sql(f"SELECT link from {table_name}", dest_engine).to_list()
    except:
        Existing_links = []

    df_processed = df[~df['link'].isin(Existing_links)]
    total_processed = len(df_processed)

    print(f"total data di staging: {len(df)}")
    print(f"data yang sudah pernah diproses: {len(Existing_links)}")
    print(f"memulai extraksi AI untuk {total_processed} data baru...")
    
    for i, (index, row) in enumerate(df_processed.iterrows(),1):
        major = extract_major(row['minimum_qualifications'])
        if major:
            single_row_df = pd.DataFrame({'link':row['link'], 'major':[major]})
            try:
                print("import data ke postgres")
                single_row_df.to_sql(table_name, dest_engine, if_exists='append', index=False)
                print(f"[{i}/{total_processed}] ✅ Berhasil : {row['link'][:30]}...")
            except Exception as e:
                print(f"[{i}/{total_processed}] ❌ Gagal simpan: {e}")  

def extract_exp_yr(text):
    if not text:
        return "Not Specified"
    
    pattern_range_tahun = r"\d+\s?[\-\–\—\−]\s?\d+\s?tahun"
    pattern_range_years = r"\d+\s?[\-\–\—\−]\s?\d+\s?years"
    pattern_plus_tahun = r"\d+\s?[\+]\s?tahun"
    pattern_tahun = r"\d+\s?tahun"
    pattern_years = r"\d+\s?years"
    master_pattern = f"{pattern_range_tahun}|{pattern_range_years}|{pattern_plus_tahun}|{pattern_tahun}|{pattern_years}"

    matches = re.search(master_pattern, text, re.IGNORECASE)

    if not matches:
        return "Not Specified"
    
    return matches.group()

def process_exp_yr():
    src_engine = create_engine('mssql+pyodbc://sa:<YOUR_PASSWORD>@sql-server:1433/Job_analyzer?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes')
    dest_engine = create_engine('postgresql://postgres2:<YOUR_PASSWORD>@postgres_db:5432/job_market_db')
    print("membaca data staging")
    metadata = MetaData()
    table_name = 'exp_tb_clean'
    exp_table = Table(
        table_name, metadata,
        Column('link', String, primary_key=True),
        Column('min_exp', String)
    )
    metadata.create_all(dest_engine)
    print(f"pengecekan table {table_name} selesai...")
    query = """
            SELECT link, minimum_qualifications, job_description
            FROM stg_jobstreet_raw
            UNION ALL
            SELECT link, minimum_qualifications, job_description
            FROM stg_kalibrr_raw
            """
    df = pd.read_sql(query, src_engine)
    df['full_context'] = df[['minimum_qualifications','job_description']].fillna('').agg('\n'.join, axis=1)
    try:
        Existing_links = pd.read_sql(f"SELECT link from {table_name}", dest_engine)['link'].to_list()
    except:
        Existing_links = []
    df_processed = df[~df['link'].isin(Existing_links)]
    total_processed = len(df_processed)
    print(f"total data di staging: {len(df)}")
    print(f"data yang sudah pernah diproses: {len(Existing_links)}")
    print(f"memulai extraksi AI untuk {total_processed} data baru...")

    for i, (index, row) in enumerate(df_processed.iterrows(), 1):
        exp = extract_exp_yr(row['full_context'])
        if exp:
            single_row_df = pd.DataFrame({'link':[row['link']], 'min_exp':[exp]})
            try:
                single_row_df.to_sql(table_name, dest_engine, if_exists='append', index=False)
                print(f"[{i}/{total_processed}] ✅ Berhasil : {row['link'][:30]}...")
            except Exception as e:
                print(f"[{i}/{total_processed}] ❌ Gagal simpan: {e}") 

def join_extract_sql_postgres():
    src_engine = create_engine('mssql+pyodbc://sa:<YOUR_PASSWORD>@sql-server:1433/Job_analyzer?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes')
    dest_engine = create_engine('postgresql://postgres2:<YOUR_PASSWORD>@postgres_db:5432/job_market_db')
    metadata = MetaData()
    table_name = 'remain_tb'
    remain_tb = Table(
        table_name, metadata,
        Column('link', String, primary_key=True),
        Column('source_site', String),
        Column('location', String),
        Column('role', String),
        Column('salary', String),
        Column('job_type', String),
        Column('company', String),
        Column('position', String),
        Column('specialization', String),
        Column('industry', String),
        Column('date_posted', String)        
    )
    metadata.create_all(dest_engine)
    query = """
            SELECT
            link, 
            source_site,
            location,
            role,
            salary,
            job_type,
            company,
            NULL AS position,
            NULL AS specialization,
            NULL AS industry,
            date_posted
            FROM stg_jobstreet_raw

            UNION ALL

            SELECT
            link, 
            source_site,
            location,
            role,
            salary,
            job_type,
            company,
            position,
            specialization,
            industry,
            date_posted
            FROM stg_kalibrr_raw            
            """
    df = pd.read_sql(query, src_engine)
    try:
        existing_tb = pd.read_sql(f'SELECT * from {table_name}', dest_engine)['link'].to_list()
    except:
        existing_tb = []
    processed_tb = df[~df['link'].isin(existing_tb)]
    total_processed = len(processed_tb)
    print(f"memproses total {total_processed} data...")
    print(f"total data di staging: {len(df)}")
    print(f"data yang sudah pernah diproses: {len(existing_tb)}")
    print(f"memulai extraksi AI untuk {total_processed} data baru...")

    processed_tb.to_sql(table_name, dest_engine, if_exists='append', index=False)

if __name__ == '__main__':
    process_skill_enrinchment()
    process_education_enrinchment()
    process_major_enrinchment()
    process_exp_yr()
    join_extract_sql_postgres()