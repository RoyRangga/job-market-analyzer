from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import pandas as pd

# --- SETUP (service & webdriver Manager) ---
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

# --- Navigasi ---
driver.get("https://quotes.toscrape.com/")
time.sleep(2)

# --- Scrapping ---
data_list = []
quotes = driver.find_elements(By.CLASS_NAME, 'quote')

for q in quotes:
    text = q.find_element(By.CLASS_NAME, 'text').text
    author = q.find_element(By.CLASS_NAME, 'author').text

    #append kedalam library:
    data_list.append(
        {
            'quote':text,
            'penulis':author
        }
    )

df = pd.DataFrame(data_list)
print(df.head())
df.to_csv("hasil_scrapping.csv", index=False)
driver.quit()