from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.chrome.option import Options
# import tempfile
# import shutil
from datetime import datetime, timedelta
import time
import pandas as pd
import re


def init_browser():
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
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

def scrape_kaliber(url):
    driver = init_browser()
    driver.get(url)
    time.sleep(3)

    jobs=[]
    try:
        # job_listings = driver.find_elements(By.CLASS_NAME, "k-font-dm-sans")
        job_listings = driver.find_elements(By.XPATH, "//h2[contains(@data-tooltip-id, 'job-title-tooltip-[object Object]')]/a")
        for link in job_listings:
            try:
                single_link = link.get_attribute("href")
                jobs.append({"link":single_link})
            except Exception as e:
                print(f"gagal tarik data: {e}")   
        for item in jobs:
            driver.get(item['link'])
            time.sleep(3)            
            try:
                #retrieve data alamat
                alamat=driver.find_element(By.XPATH, "//span[contains(@itemtype, 'PostalAddress')]")
                item['alamat']=alamat.text
                #retrieve data role
                role=driver.find_element(By.XPATH, "//h1[contains(@itemprop, 'title')]")
                item['role']=role.text
                #tiper kerjaan
                job_type=driver.find_element(By.XPATH, "//a[contains(@href, '/home/t/')]").text
                item['job_type']=job_type
                #mengambil deskripsi pekerjaan
                desc=[]
                description=driver.find_elements(By.XPATH, "//div[contains(@itemprop, 'description')]//*[self::li or self::p]")
                for li in description:
                    list_desc=li.text.strip()
                    if list_desc:
                        desc.append(list_desc)
                item['description']=list_desc
                #mengambil minimum kualifikasi
                qualifications=[]
                min_qualification = driver.find_elements(By.XPATH, "//div[contains(@itemprop, 'qualifications')]//*[self::li or self::p]")
                for li in min_qualification:
                    min_qua=li.text.strip()
                    if min_qua:
                        qualifications.append(min_qua)
                item['minimum_qualifications']=qualifications
                #mengambil tingkatan posisi
                tingkat_posisi=driver.find_element(By.XPATH, "//dt[text()='Tingkat Posisi']/following-sibling::dd//a").text
                item['position']=tingkat_posisi
                #mengambil spesialisasi
                item['spesialisasi']=driver.find_element(By.XPATH, "//dt[text()='Spesialisasi']/following-sibling::dd//a").text
                #mengambil syarat pendidikan
                item['syarat_pendidikan']=driver.find_element(By.XPATH, "//dt[text()='Persyaratan tingkat pendidikan']/following-sibling::dd//a").text
                item['industry']=driver.find_element(By.XPATH, "//dt[text()='Industri']/following-sibling::dd//span").text
                item['situs']=driver.find_element(By.XPATH, "//dt[text()='Situs']/following-sibling::dd//a").get_attribute('href')
                verified=driver.find_element(By.XPATH, "//div[contains(@data-original-title, 'verified-business')]")
                if verified:
                    is_ver=1
                else:
                    is_ver=0
                item['is_business_verified']=is_ver
                #mengambil waktu upload job
                # date_posted = driver.find_element(By.XPATH, "//span[contains(@itemprop, 'datePosted')]").get_attribute("textContent")
                item['date_posted'] = driver.find_element(By.XPATH, "//span[contains(@itemprop, 'datePosted')]").get_attribute("textContent")
                # item['date_posted'] = date_posted.strftime()
                item['valid_through']=driver.find_element(By.XPATH, "//span[contains(@itemprop, 'validThrough')]").get_attribute("textContent")
                # print(f"dapat alamat: {item['alamat']}, dapat role: {item['role']}")
            except Exception as e:
                print(f"gagal tarik data: {e}")   
    except Exception as e:
        print(f"scripping gagal: {e}")
    finally:
        driver.quit()
    return jobs

def scrap_jobstreet(url):
    driver=init_browser()
    driver.get(url)
    links=[]
    try:
        #dapatkan semua link dari job yang di post
        all_links = driver.find_elements(By.XPATH, "//div[contains(@class, '_1ybl4650 _6wfnkx4x _6wfnkx4v')]/a")
        for link in all_links:
            single_link = link.get_attribute("href")
            links.append({'link':single_link})
        for item in links:
            driver.get(item['link'])    
            time.sleep(2)
            try:
                #mendapatkan nama job
                item["role"]=driver.find_element(By.XPATH, "//h1[contains(@data-automation, 'job-detail-title')]").text
                #mendapatkan lokasi job
                item["location"]=driver.find_element(By.XPATH, "//div//span[contains(@data-automation, 'job-detail-location')]//a").text
                #mendapatkan nama perusahaan
                item["company"]=driver.find_element(By.XPATH, "//span[contains(@data-automation, 'advertiser-name')]").text
                item["job_type"]=driver.find_element(By.XPATH, "//span[contains(@data-automation, 'job-detail-work-type')]//a").text
                item["job_detail_classification"]=driver.find_element(By.XPATH, "//span[contains(@data-automation, 'job-detail-classifications')]//a").text
                descs=[]
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
                descriptions=driver.find_elements(By.XPATH, xpath_keywords)
                for desc in descriptions:
                    desc2 = desc.text.strip()
                    if desc2:
                        descs.append(desc2)
                item["minimum_qualifications"]=descs
                dateposted_path = "//span[contains(., 'Diposting') or contains(., 'Posted')]"
                date_posted=driver.find_element(By.XPATH, dateposted_path).text
                item["date_Posted_string"]=date_posted
                matching = "(\d+)\s(hari|menit|jam|detik|bulan)"
                match_date = re.search(matching, item["date_Posted_string"])
                if match_date:
                    angka = int(match_date.group(1))
                    satuan = match_date.group(2)
                    item["date_posted_2"] = f"{angka} {satuan}"
                    if 'jam' in satuan or 'menit' in satuan:
                        item['date_posted'] = (datetime.now()).strftime("%Y-%m-%d")
                    elif 'hari' in satuan:
                        item['date_posted'] = (datetime.now() - timedelta(days=angka)).strftime("%Y-%m-%d")
                    elif 'bulan' in satuan:
                        item['date_posted'] = (datetime.now() - timedelta(days=angka * 30)).strftime("%Y-%m-%d")
                    else:
                        item['date_posted'] = (datetime.now()).strftime("%Y-%m-%d")
                else:
                    item["date_posted_2"] = "0 Hari"
                desc_job=[]
                XPATH_DESC_2 = (
                    "//div[contains(@data-automation, 'jobAdDetails')]"
                )
                XPATH_DESC = (
                    "//div[.//descendant::*[contains(text(), 'Deskripsi Pekerjaan') or contains(text(), 'Tanggungjawab')]]"
                    "/following-sibling::div[1]//ul[1]//li"
                    )
                description = driver.find_elements(By.XPATH, XPATH_DESC)
                for i in description:
                    list_desc = i.text.strip()
                    if list_desc and "?" not in list_desc.lower() and "pertanyaan" not in list_desc.lower():
                        desc_job.append(list_desc)
                item["description"] = desc_job
                item["all_description"] = driver.find_element(By.XPATH, XPATH_DESC_2).text

            except Exception as e:
                print(f"error pada penarikan data joob street: {e}")
    except Exception as e:
        print(f"error pada penarikan data joob street: {e}")
    time.sleep(2)

    return links

#proses data scrapping
# url = "https://www.kalibrr.id/id-ID/home"
url_kaliber = "https://www.kalibrr.id/id-ID/home/te/data"
url_jobstreet = "https://id.jobstreet.com/id/Data-jobs"

scrapping_kaliber = scrape_kaliber(url=url_kaliber)
scrapping_jobstreet = scrap_jobstreet(url=url_jobstreet)
print(scrapping_jobstreet)