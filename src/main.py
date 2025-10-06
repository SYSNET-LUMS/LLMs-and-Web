import argparse
import os
import json
from datetime import datetime

from serp_scrapers.bing_scraper import scrape_bing_to_csv
from serp_scrapers.google_scraper import scrape_google_to_csv  # if available
from serp_scrapers.brave_scraper import scrape_brave_to_csv
from serp_scrapers.duckduckgo_scraper import scrape_duckduckgo_to_csv
from evaluators.evaluation import check_urls  # URL evaluation helper
from chatgpt_scraper.har_parser import har_parser  # For parsing .har files

def parse_args():
    parser = argparse.ArgumentParser(
        description="Unified SERP scraper & evaluator using .har inputs"
    )
    parser.add_argument(
        '--har-files', nargs='+', required=True,
        help='List of .har files to parse'
    )
    parser.add_argument(
        '-s', '--search-engines', nargs='+', default=['bing', 'google'],
        choices=['bing', 'google', 'brave', 'ddg'],
        help='Which search engines to use'
    )
    parser.add_argument(
        '-m', '--max-se-index', type=int, default=250,
        help='Maximum index to scrape up to'
    )
    parser.add_argument(
        '-i', '--index-interval', type=int, default=50, choices=range(1, 51), metavar='[1-50]',
        help='Interval at which to scrape indexes'
    )
    parser.add_argument(
        '-o', '--output-dir', default='outputs',
        help='Directory to save query folders and results'
    )
    parser.add_argument(
        '-l', '--logs-print', default=False,
        help='Directory to save query folders and results'
    )
    parser.add_argument(
        '--gpt5', default=False,
        help='Flag if using gpt5'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    def printLog(toPrint):
        if args.logs_print:
            print(toPrint)

    # Ensure output directory
    os.makedirs(args.output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Parse HAR files
    parsed_entries = har_parser(args.har_files)
    printLog("All .har files parsed")
    # return

    import glob
    from tqdm import tqdm


    # Iterate over each HAR entry
    for entry in tqdm(parsed_entries, desc="HAR entries"):
        harname = os.path.splitext(os.path.basename(entry['harname']))[0]

        # Check for an existing folder matching pattern: harname_*
        pattern = os.path.join(args.output_dir, f"{harname}_*")
        existing_folders = glob.glob(pattern)

        if existing_folders:
            # If a folder exists, just use the first one found (or you can choose by sorting)
            folder = existing_folders[0]
        else:
            # Otherwise, create a new one with timestamp
            folder = os.path.join(args.output_dir, f"{harname}_{timestamp}")
            os.makedirs(folder, exist_ok=True)

        printLog("Running SERP on "+harname)

        # Scrape each search string
        search_strings = entry.get('search_strings', [])
        for idx, query in enumerate(tqdm(search_strings, desc=f"Queries ({harname})"), start=1):
            safe_q = query.replace(' ', '_')[:12]
            safe_q = safe_q.replace('\\', '_')[:12]
            safe_q = safe_q.replace('/', '_')[:12]
            for engine in tqdm(args.search_engines, desc="Search engines", leave=False):
                csv_path = os.path.join(folder, f"{harname}_{idx}_{engine}_{safe_q}.csv")
                if engine == 'bing':
                    printLog("Running bing for "+harname)
                    scrape_bing_to_csv(
                        query=query,
                        output_file=csv_path,
                        max_results=args.max_se_index,
                        batch_size=args.index_interval
                    )
                elif engine == 'google':
                    printLog("Running Google for "+harname)
                    scrape_google_to_csv(
                        query=query,
                        output_file=csv_path,
                        max_results=args.max_se_index,
                        page_size=args.index_interval
                    )
                elif engine == 'brave':
                    printLog("Running Brave for "+harname)
                    scrape_brave_to_csv(
                        query=query,
                        output_file=csv_path,
                        max_results=args.max_se_index,
                        page_size=args.index_interval
                    )
                elif engine == 'ddg':
                    printLog("Running DuckDuckGo for "+harname)
                    scrape_duckduckgo_to_csv(
                        query=query,
                        output_file=csv_path,
                        max_results=args.max_se_index,
                        page_size=args.index_interval
                    )
                else:
                    print(f"Engine '{engine}' not supported. Skipping.")

        # Prepare URL list file (merge and dedupe)
        urls = set(entry.get('url', []))
        urls_txt = os.path.join(folder, f"urls_to_eval_{timestamp}.txt")
        with open(urls_txt, 'w', encoding='utf-8') as f:
            for u in sorted(urls):
                f.write(u + '\n')

        # Gather all CSVs
        csv_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.csv')]
        if csv_files:
            results_txt = os.path.join(folder, f"evaluation_results_{timestamp}.txt")
            check_urls(
                csv_paths=csv_files,
                txt_path=urls_txt,
                results_pathfile=results_txt
            )
            print(f"Finished evaluation for {harname}, see {results_txt}")
        else:
            print(f"No CSVs found for {harname}, skipping evaluation.")

    print("All .har inputs processed.")


if __name__ == "__main__":
    main()
