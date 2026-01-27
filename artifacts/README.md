# Data from Last Run (September 26, 2025)

This folder contains data previously extracted by the tools in this repository.

To download the **complete dataset**, please download it from **[here](https://drive.google.com/drive/folders/1gnnH6GtNboGNyNCLz32-0VWnfixfKqkg?usp=sharing)**.

The primary files you’ll need are:

* **`aggregated_data.json`**
* **`aggregated_data_parquet/`**

We **recommend using the JSON file** when possible, as it is usually the most up-to-date and contains the most complete information. However, working with large nested JSON files can be difficult, so **Parquet files are also provided** for easier querying and analysis.

You can use **DuckDB** (or any other SQL engine that supports Parquet) to query the Parquet files efficiently.

### JSON Structure

The structure of `aggregated_data.json` is as follows:

```json
{
    "category1": {
        "har1": {
            "urls_from_prompt": [],
            "urls_cited": [],
            "search_string": [],
            "bing_urls": [
                {
                    "page_title": "",
                    "url": "",
                    "rank": "",
                    "search_string_num": 1
                }
            ],
            "google_urls": [
                {
                    "page_title": "",
                    "url": "",
                    "rank": "",
                    "search_string_num": 1
                }
            ],
            "brave_urls": [
                {
                    "page_title": "",
                    "url": "",
                    "rank": "",
                    "search_string_num": 1
                }
            ]
        }
    }
}
```

### Field Definitions

#### URL Lists

* **`urls_from_prompt`**
  URLs accessed by the answer engine during its internal reasoning process.

* **`urls_cited`**
  URLs explicitly cited in the final response text.
  *This is a subset of `urls_from_prompt`.*

#### Search Engine Results

* **`bing_urls`**
* **`google_urls`**
* **`brave_urls`**

These contain URLs scraped from the corresponding search engines.

Each HAR may include **multiple search strings**. To preserve ranking order, each search result includes a `search_string_num`.
All URLs with the same `search_string_num` come from the **same search query**, and their `rank` represents the position in the search results for that query.




### Working with the Parquet Files

If the JSON structure is difficult to work with, you can query the Parquet files directly using DuckDB:

```sql
SELECT *
FROM read_parquet('aggregated_data_parquet/serp_result.parquet')
LIMIT 10;
```

This is the recommended approach for large-scale analysis.





## Text Responses

To reconstruct the **full text responses** from the answer engine (not just cited URLs, but the complete responses for all ~1000 queries):

1. Download the HAR response files from **[here](https://drive.google.com/drive/folders/1Su_AHlv4KjHcYIS0IpizA1VGGGnM4fQR)**.
2. Use the appropriate reconstruction script:

   * `reconstruct_response_gpt.py`
   * `reconstruct_response_claude.py`

Run the script as follows:

```bash
python3 reconstruct_response_gpt.py file_name.har
```

---

If you want, I can also:

* Add a **quick-start DuckDB section**
* Add **example SQL queries**
* Split this into **README + SCHEMA.md** for clarity
