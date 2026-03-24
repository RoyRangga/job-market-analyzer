#coba test ambil data jobstreet
import scrappingFunction as sF
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url_jobstreet = "https://id.jobstreet.com/id/Data-jobs"
url_kaliber = "https://www.kalibrr.id/id-ID/home/te/data"
single_url_kaliber = "https://www.kalibrr.id/id-ID/c/pt-bumi-amartha-teknologi-mandiri/jobs/264949/data-engineer-15"
driver = sF.init_browser()
driver.get(single_url_kaliber)
test = driver.find_element(By.XPATH, "//h1[contains(@itemprop, 'title')]/following-sibling::span/a/h2").text
# text = [i.get_attribute('textContent').replace('\xa0', ' ') for i in salary]
text = "".join(test)
print(test)

# print(all_the_desc)
# for i in range(len(all_links)):
#     single_link = all_links[i].get_attribute("href")
#     salary=driver.find_element(By.XPATH, "//div[contains(@itemprop, 'baseSalary')]/preceding-sibling::span")
#     print(salary)