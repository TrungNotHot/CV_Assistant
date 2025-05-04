import re
from bs4 import BeautifulSoup
from browser_manager import BrowserManager


class pageURL:
    def __init__(self, base_url, page_num, delay=2):
        self.base_url = base_url
        self.page_num = page_num
        self.url = self.get_url()
        self.delay = delay
        self.listURL = []
        self.browser = BrowserManager()

    def get_url(self):
        return re.sub(r'page=\d+', f'page={self.page_num}', self.base_url)

    def fetch_soup(self, url):
        page_source = self.browser.fetch_page(url)
        soup = BeautifulSoup(page_source, "html.parser")
        return soup

    def get_listURL(self):
        soup=self.fetch_soup(self.url)
        jobs = soup.find_all('div', class_='job-item-search-result')
        self.listURL = [
            job.find('a')['href']
            for job in jobs
            if job.find('a') and job.find('a').has_attr('href') and "/brand/" not in job.find('a')['href']
        ]
        return self.listURL
    
