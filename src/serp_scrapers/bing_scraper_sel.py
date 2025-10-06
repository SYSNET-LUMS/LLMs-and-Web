"""
Bing Search Scraper Module

Provides functions to scrape Bing results with Selenium, including
proxy rotation and User-Agent rotation. Data is flushed to CSV per page
so progress is saved incrementally. Intended for import by a
separate main.py (or other caller).

Dependencies:
    pip install selenium webdriver_manager
"""
import csv
import time
import random

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# List of User-Agent strings to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G973U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Mobile Safari/537.36",
    # add more as needed
]


def build_options(headless: bool = True, proxy: str = None) -> Options:
    """
    Prepare Chrome Options with optional headless, proxy, and random UA.
    Logs the chosen User-Agent.
    """
    options = Options()
    if headless:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    # Rotate User-Agent
    user_agent = random.choice(USER_AGENTS)
    print(f"[Log] Selected User-Agent: {user_agent}")
    options.add_argument(f"--user-agent={user_agent}")

    # Configure proxy if provided
    if proxy:
        print(f"[Log] Configuring proxy: {proxy}")
        p = Proxy()
        p.proxy_type = ProxyType.MANUAL
        p.http_proxy = proxy
        p.ssl_proxy = proxy
        options.proxy = p
    else:
        print("[Log] No proxy configured.")

    return options


def scrape_bing(query: str,
                proxy_list: list = None,
                headless: bool = True,
                output_file: str = None) -> list:
    """
    Scrape all Bing SERP pages for a query, rotating proxies and UAs.
    Immediately flushes each page's results to CSV if `output_file` is set.

    Args:
        query (str): Search query.
        proxy_list (list): List of "host:port" proxies.
        headless (bool): Run in headless mode if True.
        output_file (str): Path to CSV for incremental saving (optional).

    Returns:
        List[Dict]: Each dict has 'title', 'url', 'snippet'.
    """
    all_results = []
    page = 1

    # Prepare CSV writer if needed
    writer = None
    csv_file = None
    if output_file:
        csv_file = open(output_file, 'w', newline='', encoding='utf-8')
        writer = csv.DictWriter(csv_file, fieldnames=['title', 'url', 'snippet'])
        writer.writeheader()
        print(f"[Log] Output file '{output_file}' opened for writing.")

    print(f"[Log] Beginning scrape for query: '{query}'")
    while True:
        print(f"[Log] Starting page {page}")
        proxy = random.choice(proxy_list) if proxy_list else None
        options = build_options(headless, proxy)
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        wait = WebDriverWait(driver, 10)

        start = (page - 1) * 10 + 1
        url = (
            "https://www.bing.com/search?"
            f"q={query}&first={start}&mkt=en-US&cc=US"
        )
        print(f"[Log] Navigating to: {url}")
        driver.get(url)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li.b_algo')))
        except Exception:
            print(f"[Log] No results found or timeout on page {page}. Ending.")
            driver.quit()
            break

        items = driver.find_elements(By.CSS_SELECTOR, 'li.b_algo')
        print(f"[Log] Found {len(items)} items on page {page}")
        if not items:
            driver.quit()
            break

        page_results = []
        for item in items:
            try:
                el = item.find_element(By.CSS_SELECTOR, 'h2 a')
                title = el.text
                link = el.get_attribute('href')
            except:
                continue
            snippet = ''
            try:
                snippet = item.find_element(By.CSS_SELECTOR, 'p').text
            except:
                pass
            result = {'title': title, 'url': link, 'snippet': snippet}
            page_results.append(result)

        # Write this page's results immediately
        if writer and page_results:
            writer.writerows(page_results)
            csv_file.flush()
            print(f"[Log] Saved {len(page_results)} results from page {page} to CSV.")

        all_results.extend(page_results)
        driver.quit()
        print(f"[Log] Completed page {page}")

        # Check if this is the last page: if fewer than 10 results, end
        if len(items) < 10:
            print(f"[Log] Detected last page (only {len(items)} results). Ending.")
            break

        page += 1
        delay = random.uniform(1, 3)
        print(f"[Log] Sleeping for {delay:.2f} seconds before next page")
        time.sleep(delay)

    if csv_file:
        csv_file.close()
        print(f"[Log] CSV file '{output_file}' closed.")

    print(f"[Log] Scraping finished. Total results: {len(all_results)}")
    return all_results


def run_scraper(query: str,
                proxy_list: list = None,
                headless: bool = True,
                output_file: str = 'results.csv') -> list:
    """
    High-level function: scrape Bing for `query`, save to CSV incrementally,
    and return results.

    Args:
        query (str): Search query.
        proxy_list (list): List of "host:port" proxies.
        headless (bool): Run in headless mode if True.
        output_file (str): Path to CSV for saving.

    Returns:
        List[Dict]: Scraped results.
    """
    print(f"[Log] run_scraper called with query='{query}', output_file='{output_file}'")
    data = scrape_bing(query, proxy_list, headless, output_file)
    return data
