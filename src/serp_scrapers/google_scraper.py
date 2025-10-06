import os
import math
import time
import csv
import base64
import json
import requests
from dotenv import load_dotenv

# —— Configuration —— #
SERPER_ENDPOINT = "https://google.serper.dev/search"
OXY_ENDPOINT    = "https://realtime.oxylabs.io/v1/queries"

# load_dotenv()  # load .env
# ———————— #



OXY_USERNAME = os.getenv("OXY_USERNAME")
OXY_PASSWORD = os.getenv("OXY_PASSWORD")

# -----------------------------
# Serper.dev (existing)
# -----------------------------
def fetch_serper_page(query, page, page_size):
    """
    Fetch one 'page' of results from Serper.dev.
    Returns list of (title, link) tuples from the 'organic' field.
    """
    headers = {
        "X-API-KEY": os.getenv("API_KEY"),
        "Content-Type": "application/json"
    }
    payload = {
        "q": query,
        "page": page,
        "num": page_size
    }

    try:
        resp = requests.post(SERPER_ENDPOINT, json=payload, headers=headers, timeout=20)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # If it's a Bad Request because num is too large, retry with num=20
        if resp is not None and resp.status_code == 400 and page_size > 20:
            retry_payload = {"q": query, "page": page, "num": 20}
            resp = requests.post(SERPER_ENDPOINT, json=retry_payload, headers=headers, timeout=20)
            resp.raise_for_status()
        else:
            raise

    data = resp.json()
    items = []
    for entry in data.get("organic", []):
        title = entry.get("title")
        link  = entry.get("link")
        if title and link:
            items.append((title, link))
    return items


# -----------------------------
# Oxylabs (multi-page in ONE call)
# -----------------------------
def _oxylabs_auth_header():
    # user = os.getenv("OXYLABS_USERNAME") or os.getenv("OXY_USERNAME")
    # pwd  = os.getenv("OXYLABS_PASSWORD") or os.getenv("OXY_PASSWORD")
    if not (OXY_USERNAME and OXY_PASSWORD   
            ):
        raise RuntimeError(
            "Missing Oxylabs credentials: set OXYLABS_USERNAME and OXYLABS_PASSWORD "
            "(or OXY_USERNAME / OXY_PASSWORD)."
        )
    token = base64.b64encode(f"{OXY_USERNAME}:{OXY_PASSWORD}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def fetch_oxylabs_pages(
    query,
    pages,                 # number of pages to fetch (we'll pass ceil(max_results/10))
    *,
    limit_per_page=10,     # Oxylabs 'limit' per page; keep 10 for consistent pagination
    domain="com",
    geo_location=None,
    device_type=None,
    locale=None,
    user_agent_type=None,
):
    """
    Fetch multiple Google result pages via a single Oxylabs Web Scraper API call.

    Retries once on any HTTP/network/JSON parsing error. On second failure,
    returns [] so the caller can skip this query gracefully.
    """
    headers = {
        "Content-Type": "application/json",
        **_oxylabs_auth_header(),
    }

    payload = {
        "source": "google_search",
        "query": query,
        "parse": True,
        "domain": domain,
        "pages": int(pages),
        "limit": int(limit_per_page),
    }
    if geo_location:
        payload["geo_location"] = geo_location
    if device_type:
        payload["device_type"] = device_type
    if locale:
        payload["locale"] = locale
    if user_agent_type:
        payload["user_agent_type"] = user_agent_type

    data = None
    for attempt in (1, 2):
        try:
            resp = requests.post(OXY_ENDPOINT, headers=headers, json=payload, timeout=220)
            resp.raise_for_status()
            try:
                data = resp.json()
            except ValueError:
                # Non-JSON body (empty, HTML error page, etc.)
                if attempt == 1:
                    time.sleep(1.5)
                    continue
                # Final failure: log brief info and skip
                print(f"[oxylabs] Non-JSON response on attempt {attempt} (status {resp.status_code}). "
                      f"First 200 chars: {resp.text[:200]!r}")
                return []
            break  # got JSON OK → exit retry loop
        except RequestException as e:
            if attempt == 1:
                time.sleep(1.5)
                continue
            print(f"[oxylabs] Request failed after retry: {e}")
            return []

    if not isinstance(data, dict) or "results" not in data:
        # Unexpected structure; skip gracefully
        print("[oxylabs] JSON payload missing 'results'; skipping this query.")
        return []

    # Aggregate items across returned pages
    items = []
    for r in data.get("results", []):
        # Prefer parsed "entities"
        entities = r.get("entities") or {}
        organic = entities.get("organic") or []

        # Fallbacks if entities are missing
        if not organic:
            content = r.get("content") or {}
            results_dict = content.get("results") or {}
            candidate_lists = []
            for key in ("organic", "main", "top_stories", "people_also_ask"):
                v = results_dict.get(key)
                if isinstance(v, list):
                    candidate_lists.append(v)
            for clist in candidate_lists:
                for entry in clist:
                    if isinstance(entry, dict) and ("title" in entry) and ("url" in entry or "link" in entry):
                        organic.append(entry)

        for entry in organic:
            title = entry.get("title")
            link = entry.get("url") or entry.get("link")
            if title and link:
                items.append((title, link))

    return items


# -----------------------------
# CSV driver with provider switch
# -----------------------------
def scrape_google_to_csv(query, max_results, page_size, output_file, *, provider="oxylabs", **oxylabs_kwargs):
    """
    Scrape Google search results to CSV.

    provider:
      - "oxylabs": ignore page_size; send one Oxylabs request with
                   pages = ceil(max_results/10), limit=10 (multi-page)
      - "serper" : use your existing Serper pagination

    oxylabs_kwargs: forwarded to fetch_oxylabs_pages, e.g.
      domain="com", geo_location="United States", device_type="desktop", locale="en"
    """
    # fresh CSV
    if os.path.exists(output_file):
        os.remove(output_file)

    total_written = 0
    header_written = False

    if provider == "oxylabs":
        # One multi-page request
        pages_needed = max(1, math.ceil(max_results / 10.0))  # ignore page_size per your requirement
        batch = fetch_oxylabs_pages(query, pages_needed, limit_per_page=10, **oxylabs_kwargs)

        if not batch:
            print("[oxylabs] No results returned.")
            print(f"\nDone! {total_written} total results saved to {output_file}")
            return

        with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            if not header_written:
                writer.writerow(["Page Title", "URL"])
                header_written = True

            for title, link in batch[:max_results]:
                writer.writerow([title, link])
                total_written += 1

        print(f"[oxylabs] Saved {total_written} items (requested pages={pages_needed}, limit=10).")

    elif provider == "serper":
        # Original paging logic for Serper
        pages_needed = (max_results + page_size - 1) // page_size
        for page in range(1, pages_needed + 1):
            try:
                current_page_size = page_size
                batch = fetch_serper_page(query, page, current_page_size)
                if not batch:
                    print(f"No results returned on page {page}. Stopping.")
                    break

                with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    if not header_written:
                        writer.writerow(["Page Title", "URL"])
                        header_written = True

                    for title, link in batch:
                        total_written += 1
                        writer.writerow([title, link])
                        if total_written >= max_results:
                            break

                start_idx = (page - 1) * current_page_size + 1
                end_idx   = start_idx + len(batch) - 1
                print(f"[serper] Page {page}: saved {len(batch)} items ({start_idx}–{end_idx}, total {total_written}).")

                if total_written >= max_results:
                    print("Reached max_results limit.")
                    break

            except Exception as e:
                print(f"Error on page {page}: {e}.")
                # Optionally backoff here
                # time.sleep(__import__("random").uniform(*delay_range))

    else:
        raise ValueError(f"Unknown provider: {provider}")

    print(f"\nDone! {total_written} total results saved to {output_file}")

# import os
# import csv
# import time
# import requests
# from dotenv import load_dotenv
# from requests.exceptions import ReadTimeout, HTTPError, RequestException

# # —— Configuration —— #
# ENDPOINT    = "https://serpapi.webscrapingapi.com/v2"
# load_dotenv()  # load API_KEY from .env
# MAX_RETRIES = 3
# DELAY_RANGE = (0.5, 1.5)
# # ———————— #

# def fetch_with_retries(params):
#     """
#     Wraps requests.get in retry logic for timeouts and transient errors.
#     """
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             # Increase read timeout to 30s
#             resp = requests.get(ENDPOINT, params=params, timeout=(10, 30))
#             resp.raise_for_status()
#             return resp.json()
#         except ReadTimeout:
#             print(f"[Attempt {attempt}] Read timed out. Retrying after delay...")
#         except HTTPError as e:
#             # For HTTP 400 due to too-large num, bubble up so caller can handle
#             if resp.status_code == 400:
#                 raise
#             print(f"[Attempt {attempt}] HTTP error {e}. Retrying after delay...")
#         except RequestException as e:
#             print(f"[Attempt {attempt}] Network error {e}. Retrying after delay...")
#         time.sleep(__import__("random").uniform(*DELAY_RANGE))
#     # If we get here, all retries failed
#     raise RuntimeError(f"Failed to fetch after {MAX_RETRIES} attempts")

# def scrape_google_to_csv(query, max_results, page_size, output_file):
#     """
#     Fetch up to max_results organic results for `query` in two pulls,
#     with retry logic on timeouts, then dump to CSV.
#     page_size is still ignored.
#     """
#     # Determine how many to pull in each half
#     half1 = max_results // 2
#     half2 = max_results - half1
#     pulls = [
#         {"start": 0,      "num": half1},
#         {"start": half1,  "num": half2}
#     ]

#     results = []
#     for pull in pulls:
#         params = {
#             "engine":  "google",
#             "api_key": os.getenv("WSAG_API_KEY"),
#             "q":       query,
#             "num":     pull["num"],
#             "start":   pull["start"]
#         }

#         data = fetch_with_retries(params)
#         for entry in data.get("organic", []):
#             title = entry.get("title")
#             link  = entry.get("link")
#             if title and link:
#                 results.append((title, link))

#     # Write CSV (same schema)
#     if os.path.exists(output_file):
#         os.remove(output_file)

#     with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerow(["Page Title", "URL"])
#         for title, link in results:
#             writer.writerow([title, link])

#     print(f"Done! {len(results)} results saved to {output_file}")
