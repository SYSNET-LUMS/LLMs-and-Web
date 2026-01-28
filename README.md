# LLM Web Traffic Tracking Toolkit

Tools for tracking and studying **LLM web search behavior** by:

* running automated web-search experiments (ChatGPT / Claude),
* extracting queries + visited URLs from session logs (HAR/SSE),
* scraping search engine result pages (SERPs),
* matching/evaluating accessed URLs vs. SERP rankings,
* generating stats + visualizations.

---

## Table of Contents

1. [Repo Layout](#1-repo-layout)

2. [LLM Search Automation (`LLM_search_automation/`)](#2-llm-search-automation-llm_search_automation)
   - 2.1. [Pipeline Overview](#21-pipeline-overview)
   - 2.2. [Requirements](#22-requirements)
   - 2.3. [Data Preparation (`data/`)](#23-data-preparation-data)
   - 2.4. [GPT Automation (`GPT/`)](#24-gpt-automation-gpt)
   - 2.5. [Claude Automation (`Claude/`)](#25-claude-automation-claude)
   - 2.6. [Outputs & Conventions](#26-outputs--conventions)
   - 2.7. [Troubleshooting & Tips](#27-troubleshooting--tips)

3. [SERP Scraping and Matching (`src/`)](#3-serp-scraping-and-matching-src)
   - 3.1. [Quick Start: Using Pre-Generated Data](#31-quick-start-using-pre-generated-data)
   - 3.2. [`main.py` (HAR → SERP → URL Evaluation)](#32-mainpy-har--serp--url-evaluation)
   - 3.3. [SERP Scrapers](#33-serp-scrapers)
   - 3.4. [Environment Setup (`.env`)](#34-environment-setup-env)

4. [Generating Stats](#4-generating-stats)
   - 4.1. [Organize Results Directory](#41-organize-results-directory)
   - 4.2. [Aggregate to JSON (`a.py`)](#42-aggregate-to-json-apy)
   - 4.3. [Run Notebooks](#43-run-notebooks)

5. [Artifacts](#5-artifacts)
6. [Citation](#citation)

---

## 1. Repo Layout

```text
LLM_search_automation/     # ChatGPT/Claude web-search automation (HAR + SSE capture)
src/                       # SERP scraping, matching, evaluation, datasets, results
artifacts/                 # Scripts for parsing data from previous runs, plus links to past run data
stats.ipynb                # Analysis notebook (uses aggregated JSON)
accessed_urls_stats.ipynb  # Analysis notebook (uses aggregated JSON)
```

### 1.1 Terminology

* **Prompt**: what you send to the LLM.
* **Query**: what the LLM (or you) submits to a search engine.

### 1.2 Operational note

You are strongly encouraged to use a **VPN/proxy** when scraping SERPs to reduce the chance of IP blocking.

---

## 2. LLM Search Automation (`LLM_search_automation/`)

Run **LLM web-search experiments** and capture:

* **HAR** network logs
* **SSE** (streamed deltas) where available
* **answers** (or reconstructed answers for Claude)

Supported:

* **ChatGPT** (chatgpt.com)
* **Claude** (claude.ai)

---

### 2.1 Pipeline Overview

1. Prepare ORCAS-I per-label CSVs
2. **ChatGPT**: query with web search; capture **HAR + SSE + answers**
3. **Claude**: query (web on by default); capture **HAR + SSE**; **reconstruct** answers from SSE

Notes:

* **Claude deletion:** GUI **Select all → Delete all chats** (no script)
* **Claude answers:** reconstructed post-run from SSE because UI save isn’t reliable

---

### 2.2 Requirements

* **Node.js ≥ 18**
* **Python ≥ 3.9**
* **Chrome/Chromium**

Install deps:

```bash
npm install puppeteer-extra puppeteer-extra-plugin-stealth puppeteer-har csv-parse dotenv
pip install pandas
```

---

### 2.3 Data Preparation (`data/`)

* **Input:** `ORCAS-I-gold.tsv` (must include `label_manual`)
* **Script:** `create_csvs.py` → shuffles + splits into `data/by_label_csvs/`

```bash
cd LLM_search_automation/data
python create_csvs.py
cd ..
```

Organize dataset CSVs for a run:

```bash
mkdir -p dataset
cp data/by_label_csvs/*.csv dataset/
```

---

### 2.4 GPT Automation (`GPT/`)

#### 2.4.1 One-time sign-in

Create `LLM_search_automation/GPT/.env`:

```ini
OPENAI_EMAIL=your_email@example.com
OPENAI_PASSWORD=your_password
```

Run:

```bash
cd LLM_search_automation/GPT
node sign_in.js
```

Session persists in `./puppeteer-profile` (complete 2FA if prompted).

#### 2.4.2 Configure jobs (edit `GPT/index.js`)

```js
const modelName = "gpt-5";
const datasetRunNum = "1";
const csvJobs = [
  { file: `./dataset/ORCAS-I-gold_label_Abstain.csv`, outDir: `abstain_${modelName}_${datasetRunNum}` },
  { file: `./dataset/ORCAS-I-gold_label_Factual.csv`,  outDir: `factual_${modelName}_${datasetRunNum}` }
];
```

#### 2.4.3 Run

```bash
node index.js
```

What it does:

* enables **Web search** in UI (“+ → More → Web search”)
* sends each CSV query as a prompt
* saves per-prompt **HAR** (SSE injected) + **response**
* writes `prompts.jsonl`, `prompts.txt`, `source_meta.txt`

#### 2.4.4 Optional cleanup

```bash
node delete_chats.js
```

---

### 2.5 Claude Automation (`Claude/`)

#### 2.5.1 One-time sign-in

```bash
cd LLM_search_automation/Claude
node sign_in.js
```

Session persists in `./puppeteer-profile-claude`.

#### 2.5.2 Configure jobs (edit `Claude/index.js`)

```js
const modelName = "opus-4.1";
const datasetRunNum = "1";
const csvJobs = [
  { file: `./dataset/ORCAS-I-gold_label_Factual.csv`, outDir: `factual_${modelName}_${datasetRunNum}` }
];
```

#### 2.5.3 Run

```bash
node index.js
```

Captures per-prompt **HAR** (SSE injected) + visible answer (may be `[no answer captured]` pre-reconstruction).

#### 2.5.4 Reconstruct answers

```bash
python reconstruct_answers.py
```

Parses Claude HAR SSE deltas, concatenates final text, and replaces `[no answer captured]` in response files.

---

### 2.6 Outputs & Conventions

For each `(category, model, run)`:

```text
<category>_<model>_<run>/
  <category>_hars_<model>_<run>/       # per-prompt HAR (SSE injected)
  <category>_responses_<model>_<run>/  # response-*.txt
  prompts.jsonl
  prompts.txt
  source_meta.txt
```

---

### 2.7 Troubleshooting & Tips

* **Selectors change:** update selectors/text lookups if DOM shifts.
* **Timeouts/flakiness:** increase timeouts, add sleeps, or run smaller CSVs.
* **Concurrency:** prefer serial prompts with one visible browser.
* **Claude deletion:** GUI **Select all → Delete all chats**.

---

## 3. SERP Scraping and Matching (`src/`)

`src/` contains:

* SERP scrapers (Bing/Google/Brave)
* tooling to process HAR files
* matching/evaluation code (accessed URLs vs SERP ranks)
* `datasets/` and `results/` folders

---

### 3.1 Quick Start: Using Pre-Generated Data

If you don’t want to run automation, download pre-generated HAR files from the Google Drive link referenced in:

* `src/dataset/README.md`

After downloading:

* extract HAR files → `src/datasets/`
* extract result folders → `src/results/` organized by category (`abstain/`, `factual/`, etc.)

---

### 3.2 `main.py` (HAR → SERP → URL Evaluation)

`main.py`:

1. parses HAR files from LLM web-search sessions
2. extracts search queries
3. scrapes SERP results for those queries
4. evaluates accessed URLs against SERP results
5. writes CSV outputs + evaluation reports

#### 3.2.1 Basic usage

```bash
python3 main.py --har-files dellsupport.har beetjuice.har -s google bing -m 500 -i 50 -o results
```

#### 3.2.2 Parameters

* `--har-files` (**required**): list of `.har` files to process
* `-s, --search-engines`: default `['bing','google']` — choose from `bing`, `google`, `brave`, `ddg`
* `-m, --max-se-index`: max SERP rank index to scrape up to (default: 250)
* `-i, --index-interval`: batch size for scraping (recommend ≤ 50) (default: 50)
* `-o, --output-dir`: output directory (default: `outputs`)
* `-l, --logs-print`: enable detailed logging (default: False)

#### 3.2.3 Output structure

Example:

```text
outputs/harname_20260121_120000/
  harname_idx_engine_query.csv
  urls_to_eval_20260121_120000.txt
  evaluation_results_20260121_120000.txt
  query_meta.json
```

---

### 3.3 SERP Scrapers

Available scrapers:

1. `bing_scraper.py` — WebScrapingAPI (WSA_API_KEY) or Oxylabs (OXY_USERNAME/OXY_PASSWORD)
2. `google_scraper.py` — serper.dev (API_KEY)
3. `brave_scraper.py` — WebScrapingAPI or Brave API (BRAVE_API_KEY)

---

### 3.4 Environment Setup (`.env`)

Create a `.env` file in the repo root:

```ini
# Google (serper.dev) — REQUIRED for --search-engines google
API_KEY=your_serper_api_key_here

# Bing & Brave (WebScrapingAPI) — OPTIONAL, used if OXY credentials not set
WSA_API_KEY=your_webscrapingapi_key_here

# Bing (Oxylabs Proxy) — OPTIONAL alternative to WebScrapingAPI
OXY_USERNAME=your_oxylabs_username
OXY_PASSWORD=your_oxylabs_password

# Brave (Brave Official API) — OPTIONAL, used as fallback
BRAVE_API_KEY=your_brave_api_key_here
```

Providers:

* serper.dev: [https://serper.dev/](https://serper.dev/)
* WebScrapingAPI: [https://www.webscrapingapi.com/](https://www.webscrapingapi.com/)
* Oxylabs: [https://oxylabs.io/](https://oxylabs.io/)
* Brave API: [https://api.search.brave.com/](https://api.search.brave.com/)

---

## 4. Generating Stats

Stats generation is:

1. organize `main.py` outputs into `src/results/<category>/...`
2. run `a.py` to create `aggregated_data.json`
3. analyze in notebooks

---

### 4.1 Organize Results Directory

Place `main.py` outputs under category folders:

```text
src/results/
  abstain/
    network-logs-prompt-1_20260121_120000/
      abstain_1_bing_query.csv
      abstain_1_google_query.csv
      abstain_1_brave_query.csv
      urls_to_eval_20260121_120000.txt
      evaluation_results_20260121_120000.txt
      query_meta.json
  factual/
    network-logs-prompt-*_*/
      ...
```

#### 4.1.1 Required files per run folder

* `*.csv` — SERP results from scrapers
* `urls_to_eval_*.txt` — deduped URLs accessed during session
* `evaluation_results_*.txt` — evaluation report
* `query_meta.json` (or `*_meta.json`) — cited URLs + search strings

---

### 4.2 Aggregate to JSON (`a.py`)

Run:

```bash
cd src/results
python a.py
```

Creates:

* `src/results/aggregated_data.json`

#### 4.2.1 JSON structure (example)

```json
{
  "abstain": {
    "network-logs-prompt-1_20260121_120000": {
      "urls_from_prompt": ["https://example.com/page1"],
      "urls_cited": ["https://example.com/page1"],
      "search_string": ["query one", "query two"],
      "bing_urls": [
        { "url": "https://example.com/page1", "page_title": "Page Title", "rank": 1, "search_string_num": 1 }
      ],
      "google_urls": [],
      "brave_urls": []
    }
  }
}
```

---

### 4.3 Run Notebooks

In `stats.ipynb` and `accessed_urls_stats.ipynb`:

```python
import json

with open('aggregated_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

category = "abstain"
prompt_id = "network-logs-prompt-1_20260121_120000"
entry = data[category][prompt_id]

urls_cited = entry["urls_cited"]
search_queries = entry["search_string"]
bing_results = entry["bing_urls"]
google_results = entry["google_urls"]
brave_results = entry["brave_urls"]
```

Example analyses:

* accessed URLs vs SERP coverage (by rank)
* citation patterns by category
* search engine performance comparisons
* accessed vs cited URL overlap

---
## 5. Artifacts

The `/artifacts` directory contains helper scripts for parsing data generated by previous runs of the system, along with Google Drive links for downloading the corresponding datasets from those runs.

Detailed information about the available data; including download links, file formats, data breakdowns, and usage instructions; is provided in the `README.md` inside the `artifacts` directory, which you can find [here](artifacts/README.md).


---
## Citation

If you use this research or the associated data/tools in your work, please cite the following paper:

https://doi.org/10.1145/3774904.3792278

BibTeX Code snippet
```BibTeX
@inproceedings{sayyidali2026llm,
  author    = {Sayyid-Ali, Abdur-Rahman Ibrahim and Khan, Daanish Uddin and Bhatti, Naveed Anwar},
  title     = {Are LLM Web Search Engines Sustainable? A Web-Measurement Study of Real-Time Fetching},
  booktitle = {Proceedings of the ACM Web Conference 2026 (WWW '26)},
  year      = {2026},
  address   = {Dubai, United Arab Emirates},
  publisher = {ACM},
  doi       = {10.1145/3774904.3792278},
  url       = {https://doi.org/10.1145/3774904.3792278}
}
```