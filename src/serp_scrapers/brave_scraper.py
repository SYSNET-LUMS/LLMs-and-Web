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

# Brave official API (set this if you want to use the official API path)
# Header name is "X-Subscription-Token"
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
BRAVE_API_HOST = "api.search.brave.com"

## Brave
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

def resolve_brave_redirect(href: str) -> str:
    """
    Brave result links are typically direct (no redirect hop).
    Still, we normalize & strip tracking params like in your Bing helper.
    """
    try:
        return _drop_tracking_params(href)
    except Exception:
        return href


# ---------------------------
# OFFICIAL BRAVE API PATH
# ---------------------------

def fetch_brave_results_api(query: str, page_index: int, page_size: int):
    """
    Use Brave's official Search API.
    - Endpoint: /res/v1/web/search
    - Auth: X-Subscription-Token (BRAVE_API_KEY)
    Brave API uses result-index 'offset' (0-based), so we map:
      offset = (page_index - 1) * page_size
    """
    if not BRAVE_API_KEY:
        return None  # signal: not available

    # Map page_index (1,2,3,...) to result offset
    offset = max(0, (page_index - 1) * max(1, page_size))
    count  = max(1, page_size)

    # Build querystring
    qs = urllib.parse.urlencode({
        "q": query,
        **({"offset": page_index-1} if page_index > 1 else {}),
        "source": "web",
        # Optional knobs (uncomment/tune as needed):
        # "country": "us",
        # "safesearch": "moderate",
        # "search_lang": "en",
        # "ui_lang": "en",
        # "freshness": "month",
    })

    conn = http.client.HTTPSConnection(BRAVE_API_HOST)
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": BRAVE_API_KEY,
        # "User-Agent": "your-app-name/1.0",  # optional
    }
    conn.request("GET", f"/res/v1/web/search?{qs}", headers=headers)
    resp = conn.getresponse()
    body = resp.read()

    if resp.status != 200:
        # Common cases: 401 (bad token), 402 (no credits), 429 (rate limit)
        # Fall back to WSA scraping if available
        print(f"Brave API error HTTP {resp.status}: {body[:200]!r}")
        return None

    try:
        data = json5.loads(body.decode("utf-8"))
    except Exception as e:
        print(f"Failed to parse Brave API JSON: {e}")
        return None

    # Extract organic results from API payload
    results = []
    web = (data or {}).get("web") or {}
    for item in web.get("results", []):
        title = item.get("title")
        url   = item.get("url")
        if not title or not url:
            continue

        link = resolve_brave_redirect(url)
        domain = urlparse(link).netloc.lower()
        if domain in EXCLUDED_DOMAINS:
            continue
        results.append((title, link))

    time.sleep(random.uniform(*delay_range))
    return results


# ---------------------------
# WSA + HTML FALLBACK PATH
# ---------------------------

def fetch_brave_results_wsa(query, page_index, page_size):
    """
    Fetch one page of *organic* Brave Search results via WebScrapingAPI.
    Returns up to page_size (title, link) tuples, skipping excluded domains.

    Brave SERP markup:
      - Each organic result is a <div class="snippet" data-type="web"> … </div>
      - The clickable title is the first <a href> inside that snippet.
    """
    # Build a Brave URL (server-rendered; no JS needed)
    brave_url = (
        "https://search.brave.com/search?"
        + urllib.parse.urlencode({
            "q": query,
            **({"offset": page_index-1} if page_index > 1 else {}),  # first page: omit offset
            "source": "web"
        })
    )

    # Proxied request through WebScrapingAPI
    conn = http.client.HTTPSConnection(WSA_HOST)
    params = urllib.parse.urlencode({
        "api_key": WSA_API_KEY,
        "url": brave_url,
        "render_js": False,   # SSR HTML contains results; no need for JS
        # "country": "us",
    })
    conn.request("GET", f"/v2?{params}")
    resp = conn.getresponse()
    html = resp.read().decode("utf-8")

    results = get_result_urls_from_html(html=html)

    return results


def fetch_brave_results(query, page_index, page_size):
    """
    Unified entry point:
      - Use Brave official API if BRAVE_API_KEY is set and request succeeds.
      - Otherwise, fall back to WSA+HTML scraper.
    """
    # Try official API first
    api_results = fetch_brave_results_api(query, page_index, page_size)
    if api_results is not None:
        return api_results

    # Fallback
    return fetch_brave_results_wsa(query, page_index, page_size)


def scrape_brave_to_csv(query, output_file, max_results, page_size=20):
    """
    Iterate Brave pages:
      - Official API: uses (offset, count)
      - Fallback (HTML): uses ?offset=(page_index-1)
    """
    if os.path.exists(output_file):
        os.remove(output_file)

    total_written = 0
    header_written = False
    page_index = 1

    # If caller didn't pass a page_size, default to 20 (Brave default page size)
    page_size = 20

    while total_written < max_results:
        try:
            batch = fetch_brave_results(query, page_index, page_size)
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
            break
        time.sleep(random.uniform(*delay_range))

    print(f"\nDone! {total_written} total results saved to {output_file}")


def _extract_balanced_object(text: str, start_index: int) -> str:
    depth = 0
    i = start_index
    in_string = False
    quote = None
    escape = False

    while i < len(text):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == quote:
                in_string = False
        else:
            if ch in ('"', "'"):
                in_string = True
                quote = ch
            elif ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return text[start_index:i+1]
        i += 1

    raise ValueError("Unbalanced braces while extracting object.")

def get_result_urls_from_html(html: str) -> list[tuple[str, str]]:
    """
    Parse Brave's SSR HTML (fallback path) and return list of (title, url) tuples.
    Applies tracking/redirect cleanup and domain exclusion.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Prefer the embedded JSON ('web:' object); it's more stable than CSS selectors.
    for script in soup.find_all("script"):
        txt = script.string or script.text or ""
        idx = txt.find("web:")
        if idx == -1:
            continue

        brace_idx = txt.find("{", idx)
        if brace_idx == -1:
            continue

        web_obj_text = _extract_balanced_object(txt, brace_idx)

        # Normalize common JS-only tokens so JSON5 can parse it
        cleaned = web_obj_text
        cleaned = re.sub(r"\bvoid\s+0\b", "null", cleaned)     # void 0 -> null
        cleaned = re.sub(r"\bundefined\b", "null", cleaned)    # undefined -> null
        cleaned = re.sub(r",(\s*[}\]])", r"\1", cleaned)       # drop trailing commas

        try:
            web_obj = json5.loads(cleaned)
        except Exception as e:
            raise RuntimeError(f"Failed to parse `web` object: {e}")

        results = web_obj.get("results")
        if isinstance(results, list):
            tuples: list[tuple[str, str]] = []
            for item in results:
                if isinstance(item, dict) and isinstance(item.get("url"), str):
                    title = item.get("title") or ""
                    link  = resolve_brave_redirect(item["url"])
                    domain = urlparse(link).netloc.lower()
                    if domain in EXCLUDED_DOMAINS:
                        continue
                    tuples.append((title, link))
            return tuples

    # If we get here, nothing matched — return empty for caller to handle.
    return []
