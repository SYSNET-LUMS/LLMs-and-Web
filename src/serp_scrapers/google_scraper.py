import os
import time
import csv
import requests
from dotenv import load_dotenv

# —— Configuration —— #
ENDPOINT    = "https://google.serper.dev/search"
# delay_range = (0.5, 1.5)              # polite delay between requests (s)

load_dotenv() # take environment variables from .env.
# ———————— #

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
        resp = requests.post(ENDPOINT, json=payload, headers=headers, timeout=10)
        resp.raise_for_status()

    except requests.exceptions.HTTPError as e:
        # If it's a Bad Request because num is too large, retry with num=20
        print("Trying again")
        if resp.status_code == 400 and page_size > 20:
            print("Check")
            retry_payload = {"q": query, "page": page, "num": 20}
            resp = requests.post(ENDPOINT, json=retry_payload, headers=headers, timeout=10)
            resp.raise_for_status()
        else:
            # re-raise any other errors
            raise

    data = resp.json()
    items = []
    for entry in data.get("organic", []):
        title = entry.get("title")
        link  = entry.get("link")
        if title and link:
            items.append((title, link))
    return items

def scrape_google_to_csv(query, max_results, page_size, output_file):
    # fresh CSV
    if os.path.exists(output_file):
        os.remove(output_file)

    total_written = 0
    header_written = False
    pages_needed = (max_results + page_size - 1) // page_size

    for page in range(1, pages_needed + 1):
        try:
            batch = fetch_serper_page(query, page, page_size)
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

            start_idx = (page - 1) * page_size + 1
            end_idx   = start_idx + len(batch) - 1
            print(f"Page {page}: saved {len(batch)} items ({start_idx}–{end_idx}, total {total_written}).")

            if total_written >= max_results:
                print("Reached max_results limit.")
                break

        except Exception as e:
            print(f"Error on page {page}: {e}. Retrying after delay.")
        # time.sleep(__import__("random").uniform(*delay_range))

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
