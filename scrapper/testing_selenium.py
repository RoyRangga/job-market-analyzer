from selenium import webdriver
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()

driver.get("https://www.wikipedia.org")

# Mencari kotak pencarian berdasarkan ID
search_box = driver.find_element(By.ID, "searchInput")

# Mengetik di kotak pencarian
search_box.send_keys("Python (programming language)")

# Menekan tombol enter (atau klik tombol cari)
search_box.submit()
# driver.quit()

judul = driver.find_element(By.TAG_NAME, "h1").text
print(f"judulnya adalah: {judul}")

links = driver.find_element(By.TAG_NAME, "a")

for link in links[:20]:
    print(link.get_attribute("href"))
