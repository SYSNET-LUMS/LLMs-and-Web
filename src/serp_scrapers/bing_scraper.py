import os
import time
import random
import csv
import sys
import urllib.parse
import http.client
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# ————— Configuration ————— #
delay_range = (1, 2)           # min/max delay between requests in seconds

# A set of domains you know you want to skip
EXCLUDED_DOMAINS = {
    "www.zhihu.com",
    "zhihu.com",
    # add more if needed
}

# Oxylabs credentials (set these in your environment)
OXY_USERNAME = os.getenv("OXY_USERNAME")
OXY_PASSWORD = os.getenv("OXY_PASSWORD")

# WebScrapingAPI credentials (you can also set this in your env)
WSA_API_KEY = os.getenv("WSA_API_KEY")
WSA_HOST    = "api.webscrapingapi.com"


EAST_COAST_ZIPCODES = [
    # Maine
    "04032",  # Westbrook, ME
    "04101",  # Portland, ME
    "04401",  # Bangor, ME

    # New Hampshire
    "03101",  # Manchester, NH
    "03801",  # Portsmouth, NH

    # Massachusetts
    "02108",  # Boston, MA
    "02139",  # Cambridge, MA
    "02215",  # Boston (Fenway), MA
    "01002",  # Amherst, MA
    "02703",  # Fall River, MA

    # Rhode Island
    "02903",  # Providence, RI
    "02840",  # East Providence, RI

    # Connecticut
    "06103",  # Hartford, CT
    "06810",  # Greenwich, CT
    "06510",  # New Haven, CT

    # New York
    "10001",  # New York, NY
    "11201",  # Brooklyn, NY
    "10451",  # Bronx, NY
    "12207",  # Albany, NY
    "14604",  # Rochester, NY

    # New Jersey
    "07102",  # Newark, NJ
    "08002",  # Cherry Hill, NJ
    "08701",  # Toms River, NJ
    "07030",  # Hoboken, NJ

    # Pennsylvania
    "19101",  # Philadelphia, PA
    "15213",  # Pittsburgh, PA
    "17101",  # Harrisburg, PA
    "16801",  # State College, PA

    # Delaware
    "19901",  # Dover, DE
    "19711",  # Wilmington, DE

    # Maryland
    "21201",  # Baltimore, MD
    "20740",  # Laurel, MD
    "21401",  # Annapolis, MD

    # Virginia
    "22301",  # Alexandria, VA
    "23219",  # Richmond, VA
    "24060",  # Blacksburg, VA

    # North Carolina
    "27514",  # Chapel Hill, NC
    "27601",  # Raleigh, NC
    "28202",  # Charlotte, NC

    # South Carolina
    "29201",  # Columbia, SC
    "29601",  # Greenville, SC
    "29401",  # Charleston, SC

    # Georgia
    "30303",  # Atlanta, GA
    "31401",  # Savannah, GA
    "31501",  # Brunswick, GA

    # Florida
    "33101",  # Miami, FL
    "32801",  # Orlando, FL
    "32301",  # Tallahassee, FL
    "32202",  # Jacksonville, FL
    "32114",  # Daytona Beach, FL
]

#### HELPER

import base64
from urllib.parse import urlparse, parse_qs, unquote, urlunparse, urlencode

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

def _maybe_b64_decode(s: str) -> str:
    # Some Bing values are like a1<base64-no-padding>
    if s.startswith(("a0","a1","a2","a3","a")):
        s = s[2:] if s[1:].isalnum() else s[1:]  # strip a?, be tolerant
    # add padding
    pad = (-len(s)) % 4
    try:
        return base64.b64decode(s + ("=" * pad)).decode("utf-8", errors="strict")
    except Exception:
        return s

def resolve_bing_redirect(href: str) -> str:
    """
    Turn Bing/MSN redirect URLs into the final destination.
    Handles:
      - https://www.bing.com/ck/a?...&u=<base64 or encoded target>
      - https://r.msn.com/... ?ru=<target>
      - https://go.msn.com/... ?target=<target> / ?ru=<target>
    Also strips common tracking params.
    """
    try:
        # print(href)
        p = urlparse(href)
        host = p.netloc.lower()

        # If it’s already not a Bing/MSN redirector, just clean and return
        if "bing.com" not in host and "msn.com" not in host:
            return _drop_tracking_params(href)

        q = parse_qs(p.query, keep_blank_values=True)
        candidate = q.get("u", [None])[0] or q.get("ru", [None])[0] or q.get("target", [None])[0] or q.get("url", [None])[0]
        # print(candidate)

        if candidate:
            # First try URL-decoding; if it doesn't look like a URL, try base64
            cand_dec = unquote(candidate)
            if not cand_dec.startswith(("http://", "https://")):
                cand_dec = _maybe_b64_decode(candidate)
                # print("b64")
            if cand_dec.startswith(("http://", "https://")):
                # print(cand_dec)
                return _drop_tracking_params(cand_dec)

        # Fallback: sometimes the only http(s) appears percent-encoded in the query
        from re import search
        m = search(r"(https?%3A%2F%2F[^&]+)", p.query)
        if m:
            return _drop_tracking_params(unquote(m.group(1)))

        return _drop_tracking_params(href)
    except Exception:
        return href



####
# ===================== OXYLABS (requests-based) =====================
import requests

# ===================== OXYLABS (requests-based, fixed payload) =====================
import requests

def fetch_bing_results(query, start, batch_size):
    """
    Fetch one page of Bing results via Oxylabs Realtime API (no SDK).
    Returns up to batch_size (title, link) tuples, skipping excluded domains.
    """
    if not OXY_USERNAME or not OXY_PASSWORD:
        raise RuntimeError("Missing OXY_USERNAME / OXY_PASSWORD environment variables.")

    # Pick a random East-Coast ZIP code each call
    geo = random.choice(EAST_COAST_ZIPCODES)

    # Oxylabs pages are 1-based
    page_num = (start) // batch_size + 1

    url = "https://realtime.oxylabs.io/v1/queries"

    # IMPORTANT: single-object payload (not a list). 'query' must be a string.
    payload = {
        "source": "bing_search",
        "query": query,
        # "start_page": page_num,
        **({"start_page": page_num} if page_num > 1 else {}),
        "pages": 10,
        # "limit": batch_size,
        "parse": True,
        # "geo_location": geo
    }

    try:
        r = requests.post(
            url,
            auth=(OXY_USERNAME, OXY_PASSWORD),
            json=payload,
            timeout=220
        )
    except requests.RequestException as e:
        raise RuntimeError(f"Network error talking to Oxylabs: {e}") from e

    if r.status_code == 403:
        print("ERROR: Oxylabs free-trial credits exhausted (HTTP 403).")
        sys.exit(1)

    if r.status_code != 200:
        raise RuntimeError(f"Oxylabs HTTP {r.status_code}: {r.text[:300]}")

    data = r.json()

    results = []

    # Oxylabs usually returns a list under 'results'
    pages = data.get("results", [])
    # (Some responses may return a single object; normalize)
    if isinstance(pages, dict):
        pages = [pages]

    for page in pages:
        organic = (
            page.get("content", {})
                .get("results", {})
                .get("organic", [])
        )
        for item in organic:
            title = item.get("title")
            raw_link = item.get("link") or item.get("url")
            if not title or not raw_link:
                continue

            link = resolve_bing_redirect(raw_link)

            domain = urlparse(link).netloc.lower()
            if domain in EXCLUDED_DOMAINS:
                print(f"Domain Excluded: {domain}")
                continue

            results.append((title, link))

    return results


# ===================== WebScrapingAPI (kept for reference; now disabled) =====================
# def fetch_bing_results(query, start, batch_size):
#     """
#     Fetch one page of Bing results via WebScrapingAPI.
#     Returns up to batch_size (title, link) tuples, skipping excluded domains.
#     """
#     bing_url = (
#         "https://www.bing.com/search?"
#         + urllib.parse.urlencode({
#             "q": query,
#             "count": batch_size,
#             "offset": start-1,    # Bing’s “first” param is 1-based index
#         })
#     )
#
#     conn = http.client.HTTPSConnection(WSA_HOST)
#     params = urllib.parse.urlencode({
#         "api_key": WSA_API_KEY,
#         "url": bing_url,
#         "render_js": False,
#         "country": "pk",
#     })
#     conn.request("GET", f"/v2?{params}")
#     resp = conn.getresponse()
#
#     html = resp.read().decode("utf-8")
#     soup = BeautifulSoup(html, "html.parser")
#
#     results = []
#     for li in soup.select("li.b_algo")[:batch_size]:
#         h2 = li.find("h2")
#         if not h2 or not h2.a:
#             continue
#         title = h2.get_text(strip=True)
#         raw_link = h2.a.get("href", "")
#         link = resolve_bing_redirect(raw_link)
#         domain = urlparse(link).netloc.lower()
#         if domain in EXCLUDED_DOMAINS:
#             print(f"Domain Excluded: {domain}")
#             continue
#         results.append((title, link))
#     return results

# ===================== Remainder unchanged =====================

def scrape_bing_to_csv(query, output_file, max_results, batch_size):
    # Remove existing file so each run starts fresh
    if os.path.exists(output_file):
        os.remove(output_file)

    total_written = 0
    header_written = False

    for offset in range(1, max_results, batch_size):
        try:
            batch = fetch_bing_results(query, offset, batch_size)
            if not batch:
                print(f"No more results at offset {offset}. Stopping.")
                break

            # Append this batch to CSV
            with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                if not header_written:
                    writer.writerow(["Page Title", "URL"])
                    header_written = True

                for title, link in batch:
                    total_written += 1
                    writer.writerow([title, link])

            print(f"Fetched & saved {len(batch)} items from {offset}–{offset+batch_size-1} (total {total_written}).")

        except Exception as e:
            print(f"Error at offset {offset}: {e}. Retrying after delay.")
            break
        break
        # time.sleep(random.uniform(*delay_range))

    print(f"\nDone! {total_written} total results saved to {output_file}")
