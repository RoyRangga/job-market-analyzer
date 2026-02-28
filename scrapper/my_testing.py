from selenium import webdriver
from selenium.webdriver.common.by import By

# driver = webdriver.Chrome()
# driver.get("https://www.python.org")
# search_box = driver.find_element(By.ID, "id-search-field")
# search_box.send_keys("Pandas")
# search_box.submit()

driver = webdriver.Chrome()
driver.get("https://quotes.toscrape.com/")
# pull_text = driver.find_element(By.CLASS_NAME, "text").text
# pull_author = driver.find_element(By.CLASS_NAME, "author").text
quotes = driver.find_elements(By.CLASS_NAME, "quote")

for quote in quotes:
    text = quote.find_element(By.CLASS_NAME,'text').text
    author = quote.find_element(By.CLASS_NAME, 'author').text 
    print(f"author: {author} : quotes :{text} ")
# print(f"quote: {pull_text}")