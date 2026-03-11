#coba test ambil data jobstreet
import scrappingFunction as sF
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url_jobstreet = "https://id.jobstreet.com/id/Data-jobs"
driver = sF.init_browser()
driver.get(url_jobstreet)

all_job_links = []
# all_links = driver.find_elements(By.XPATH, "//a[contains(@data-automation, 'JobLink')]")
# all_links = driver.find_element(By.XPATH, "//div[contains(@class, '_17igs2j0 _36523f5f _36523f51')]").text
all_links = driver.find_elements(By.XPATH, "//div/a[contains(@data-automation, 'jobTitle')]")
list_of_job = [i.text for i in all_links]
list_of_job_links = [i.get_attribute("href") for i in all_links]
xpath = (
    "//span[contains(@data-automation, 'jobListingDate')]"
    # "//span[contains(., 'Diposting')] | "
    # "//span[contains(., 'yang lalu')]"
)
date_posted_text = driver.find_elements(By.XPATH, "//span[contains(@data-automation, 'jobListingDate')]")
wait_element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH ,xpath))
)
# date_posted_text = wait_element.get_attribute("textContent").strip()
date_posted_text = [i.get_attribute("textContent").strip() for i in date_posted_text]
all_the_desc = []
# all_the_desc.append({"link":list_of_job_links, "date_posted":date_posted_text})
# print(date_posted_text)
# print(list_of_job_links)
# print(all_the_desc)

for i in range(len(all_links)):
    single_link = all_links[i].get_attribute("href")
    single_date = date_posted_text[i]
    all_the_desc.append({'link':single_link, "site":"Jobstreet", "date_posted_raw":single_date})#####

print(all_the_desc)