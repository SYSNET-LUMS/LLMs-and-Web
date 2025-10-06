from bs4 import BeautifulSoup
import json5
import re
import http
import os
import csv
import time
import random

import base64
import urllib.parse
from urllib.parse import urlparse, parse_qs, unquote, urlunparse, urlencode

# ————— Configuration ————— #
delay_range = (1, 2)           # min/max delay between requests in seconds

# A set of domains you know you want to skip
EXCLUDED_DOMAINS = {
    "www.zhihu.com",
    "zhihu.com",
    # add more if needed
}

# WebScrapingAPI credentials (you can also set this in your env)
WSA_API_KEY = os.getenv("WSA_API_KEY")
WSA_HOST    = "api.webscrapingapi.com"

## Shared
TRACKING_KEYS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "gclid","gbraid","wbraid","fbclid","msclkid","ocid","cvid","form","spm","ved","ei","oq","sxsrf","sca_esv","ntb"
}

def _drop_tracking_params(url: str) -> str:
    try:
        p = urlparse(url)
        q = parse_qs(p.query, keep_blank_values=True)
        q = {k: v for k, v in q.items() if k not in TRACKING_KEYS and not k.startswith("utm_")}
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(q, doseq=True), p.fragment))
    except Exception:
        return url

# ————— DuckDuckGo-specific helpers ————— #

def resolve_duckduckgo_redirect(href: str) -> str:
    """
    DDG often wraps result links like:
      https://duckduckgo.com/l/?kh=-1&uddg=<URLENCODED_TARGET>
    We unwrap to the direct destination and then strip tracking params.
    """
    try:
        p = urlparse(href)
        if p.netloc.endswith("duckduckgo.com") and p.path.startswith("/l/"):
            q = parse_qs(p.query)
            if "uddg" in q and q["uddg"]:
                real = unquote(q["uddg"][0])
                return _drop_tracking_params(real)
        return _drop_tracking_params(href)
    except Exception:
        return href


def _build_ddg_url(query: str, page_index: int) -> str:
    """
    Server-rendered SERP (no JS) via /html/ endpoint.
    Pagination is controlled by 's' which is a 0-based offset.
    DDG /html/ typically returns ~30 results per page.
    """
    offset = 0 if page_index <= 1 else (page_index - 1) * 30
    return (
        "https://duckduckgo.com/html/?"
        + urlencode({
            "q": query,
            **({"s": offset} if offset else {}),
            # You can add region/safe-search if desired:
            # "kl": "us-en",        # region/lang
            # "kp": "-2",           # safe search: -2 (off), 1 (moderate)
        })
    )


def get_ddg_results_from_html(html: str) -> list[tuple[str, str]]:
    """
    Parse DDG /html/ SERP. Organic results render as anchors with:
      <a class="result__a" href="...">Title</a>
    On newer layouts there may also be:
      <h2 ...><a data-testid="result-title-a" ...>Title</a></h2>
    We support both.
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Primary: classic /html/ markup
    for a in soup.select("a.result__a[href]"):
        title = a.get_text(strip=True)
        link  = resolve_duckduckgo_redirect(a["href"])

        domain = urlparse(link).netloc.lower()
        if domain in EXCLUDED_DOMAINS:
            continue

        if title and link:
            results.append((title, link))

    # Fallback: some variants use data-testid attributes
    if not results:
        for a in soup.select('h2 a[data-testid="result-title-a"][href]'):
            title = a.get_text(strip=True)
            link  = resolve_duckduckgo_redirect(a["href"])

            domain = urlparse(link).netloc.lower()
            if domain in EXCLUDED_DOMAINS:
                continue

            if title and link:
                results.append((title, link))

    return results


def fetch_duckduckgo_results(query: str, page_index: int, batch_size: int) -> list[tuple[str, str]]:
    """
    Fetch one page of *organic* DuckDuckGo results via WebScrapingAPI.
    Returns up to batch_size (title, link) tuples, skipping excluded domains.
    """
    ddg_url = _build_ddg_url(query, page_index)

    # Proxied request through WebScrapingAPI
    conn = http.client.HTTPSConnection(WSA_HOST)
    params = urlencode({
        "api_key": WSA_API_KEY,
        "url": ddg_url,
        "render_js": False,   # /html/ is server-rendered
        # Optional geo: "country": "us",
    })
    conn.request("GET", f"/v2?{params}")
    resp = conn.getresponse()
    html = resp.read().decode("utf-8")

    all_results = get_ddg_results_from_html(html=html)

    # Respect requested batch_size (even though /html/ ≈ 30/pg)
    results = all_results[:batch_size] if batch_size else all_results

    time.sleep(random.uniform(*delay_range))  # be nice
    return results


def scrape_duckduckgo_to_csv(query: str, output_file: str, max_results: int, page_size: int):
    """
    Iterate DDG pages using the 0-based 's' (offset) param (30 results/page).
    The function mirrors your Brave routine (including CSV schema).
    """
    if os.path.exists(output_file):
        os.remove(output_file)

    total_written = 0
    header_written = False
    page_index = 1

    # OVERRIDE USER (DDG /html/ returns ~30 organic per page)
    page_size = 30

    while total_written < max_results:
        try:
            batch = fetch_duckduckgo_results(query, page_index, page_size)
            if not batch:
                print(f"No more results at page {page_index}. Stopping.")
                break

            with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                if not header_written:
                    writer.writerow(["Page Title", "URL"])
                    header_written = True

                for title, link in batch:
                    writer.writerow([title, link])
                    total_written += 1
                    if total_written >= max_results:
                        break

            print(f"Fetched & saved {len(batch)} items from page {page_index} (total {total_written}).")
            page_index += 1
        except Exception as e:
            print(f"Error on page {page_index}: {e}. Retrying after delay.")
        time.sleep(random.uniform(*delay_range))

    print(f"\nDone! {total_written} total results saved to {output_file}")
