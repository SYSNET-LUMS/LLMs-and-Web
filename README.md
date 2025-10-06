## LLM Web Traffic Tracking Tool Kit

## Overview

This project contains variouse tool useful for tracking and studiying LLM Websearch. The main content of the project can be found in the src file which contain the SERP web scrapers and LLM web interface scrapers.

#### `main.py` Usage:

Use help flag for all `main.py` params, but as a guide here is a example use 
```bash
python3 main.py --har-files dellsupport.har beetjuice.har -s google bing -m 500 -i 50 -o results
``` 

- -s for selecting SE
- -m for max index to scrap till
- -i scrap batch size (dont go over 50 as results get a bit strages above that)
- -o output 

NOTE: For google scraping we use serper.dev API. Need to setup .env file to use. Check SERP Scrapers section for info


#### SERP Web Scrapers

They are two scraping tool:
1. bing_scraper.py -> Uses python requests module to simply send a get request to bings search engien.
2. google_scraper.py -> Uses serper.dev SERP API (needed to use an online servise as Googles website gardes against bots very strictly). Will have to create an account on there website to use (2500 free requests). Creat .env in root of dir with API_KEY="serper.api.from.your.account"

#### LLM Web Interface Scrapers	

#### Helpful Points
- Query = Search engine search. User prompt = what user writes to the LLM. Technially "Query" can be used for both but for sake of code understanding, we can make the definitions as such.
- An important note: very, Very, VERY much recomened to use a VPN or proxy while using SERP scrapers, as overuse can get your IP banned from the search engine.
- `old_main.py` is deprecated, only there to be viewed for reference.
- 

## LLM Search Automation

Run **LLM web-search experiments** with:
- **ChatGPT** (chatgpt.com)
- **Claude** (claude.ai)

Pipeline:
1) Prepare ORCAS-I per-label CSVs  
2) **GPT**: query with web search; capture **HAR + SSE + answers**  
3) **Claude**: query (web on by default); capture **HAR + SSE**; **reconstruct** answers from SSE

> **Claude deletion:** Use GUI **Select all → Delete all chats** (no script).  
> **Claude answers:** Reconstructed post-run from SSE because UI save isn’t reliable.

---

## Requirements

- **Node.js** ≥ 18, **Python** ≥ 3.9, **Chrome/Chromium**
```bash
npm install puppeteer-extra puppeteer-extra-plugin-stealth puppeteer-har csv-parse dotenv
pip install pandas
```

---

## Data Preparation (`data/`)

- **Input:** `ORCAS-I-gold.tsv` (`label_manual` column)  
- **Script:** `create_csvs.py` → shuffles; splits to `data/by_label_csvs/`
```bash
cd data
python create_csvs.py
cd ..
```

**Organize dataset** (copy label CSVs for a run):
```bash
mkdir -p dataset
cp data/by_label_csvs/*.csv dataset/
```
> Repeat per run as needed.

---

## GPT Automation (`GPT/`)

Use: `sign_in.js` (one-time), `index.js` (main), `delete_chats.js` (optional).

**1) One-time sign-in** — create `GPT/.env`:
```ini
OPENAI_EMAIL=your_email@example.com
OPENAI_PASSWORD=your_password
```
```bash
cd GPT
node sign_in.js
```
(Complete 2FA if prompted; session persists in `./puppeteer-profile`.)

**2) Configure CSVs** — edit `GPT/index.js`:
```js
const modelName = "gpt-5"
const datasetRunNum = "1"
const csvJobs = [
  { file: `./dataset/ORCAS-I-gold_label_Abstain.csv`, outDir: `abstain_${modelName}_${datasetRunNum}` },
  { file: `./dataset/ORCAS-I-gold_label_Factual.csv`, outDir: `abstain_${modelName}_${datasetRunNum}` }
];
```

**3) Run**
```bash
node index.js
```
Actions: enables **Web search** (“+ → More → Web search”), sends each CSV `query`, saves per-prompt **HAR** (SSE injected) + **response**, plus `prompts.jsonl`, `prompts.txt`, `source_meta.txt`.

**4) Optional cleanup**
```bash
node delete_chats.js
```

---

## Claude Automation (`Claude/`)

Use: `sign_in.js` (manual login), `index.js` (run), `reconstruct_answers.py` (SSE → final text).  
> **No deletion script** (use GUI Select all → Delete all).

**1) One-time sign-in**
```bash
cd Claude
node sign_in.js
```
(Manual login; session persists in `./puppeteer-profile-claude`.)

**2) Configure model + CSVs** — `Claude/index.js`:
```js
const modelName = "opus-4.1";
const datasetRunNum = "1";
const csvJobs = [
  { file: `./dataset/ORCAS-I-gold_label_Factual.csv`, outDir: `factual_${modelName}_${datasetRunNum}` }
];
```
*(Web access is on by default.)*

**3) Run**
```bash
node index.js
```
Captures per-prompt **HAR** (SSE injected) + visible answer (may be `[no answer captured]` pre-reconstruction).

**4) Reconstruct answers**
```bash
python reconstruct_answers.py
```
Parses Claude HAR SSE, concatenates text deltas, and replaces `[no answer captured]` in response files.

---

## Outputs & Conventions

For each `(category, model, run)`:
- `<category>_<model>_<run>/`
  - `<category>_hars_<model>_<run>/` — per-prompt **HAR** (SSE injected)
  - `<category>_responses_<model>_<run>/` — **response-*.txt**
  - `prompts.jsonl`, `prompts.txt`, `source_meta.txt`

One prompt → one HAR + one response. Add more entries to `csvJobs` for multiple categories/runs.

---

## Troubleshooting & Tips

- **Selectors change:** Update selectors/text lookups if DOM shifts.  
- **Timeouts/flakiness:** Raise timeouts, add `sleep`, or use smaller CSVs.  
- **Concurrency:** Prefer single visible browser (serial prompts).  
- **Claude deletion:** Use GUI **Select all → Delete all chats**.