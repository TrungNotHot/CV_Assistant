
BASE_API_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"
BASE_SITE_URL = "https://www.vietnamworks.com"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/vnd.api+json",
    "Origin": BASE_SITE_URL,
    "Referer": BASE_SITE_URL,
    "User-Agent": "Mozilla/5.0"
}

JOB_FUNCTION_FILTER = [{"parentId": 5, "childrenIds": [-1]}]
JOB_DESCRIPTION_SELECTOR = "div.sc-1671001a-6.dVvinc"

HITS_PER_PAGE = 50
REQUEST_DELAY = 1.2
