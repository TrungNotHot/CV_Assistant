from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import random

class BrowserManager:
    def __init__(self, headless=True):
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(options=options)

    def fetch_page(self, url, delay_range=(1, 3)):
        self.driver.get(url)
        time.sleep(random.uniform(*delay_range)) 
        return self.driver.page_source

    def close(self):
        """Đóng browser."""
        self.driver.quit()
