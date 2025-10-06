## LLM Web Trafic Tracking Tool Kit

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

