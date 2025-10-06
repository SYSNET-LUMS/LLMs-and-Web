import argparse
import os
import json
from datetime import datetime

from serp_scrapers.bing_scraper import scrape_bing_to_csv
from serp_scrapers.google_scraper import scrape_google_to_csv  # if available
from evaluators.evaluation import check_urls  # URL evaluation helper
from chatgpt_scraper.har_parser import har_parser # For parsing .har files

# Module 1: LLM Web Interface Scraper placeholder
def run_llm_scraper(user_prompt, output_dir, timestamp, safe_query):
    """
    Placeholder for LLM-based web interface scraping.
    Should write queries for Module 2 into a JSON file and return path.
    """
    # Example: generate queries based on user_prompt
    module1_queries = [user_prompt]  # Replace with actual LLM logic
    module1_file = os.path.join(output_dir, f"{safe_query[:12]}_{timestamp}_m1_queries.json")
    with open(module1_file, 'w', encoding='utf-8') as f:
        json.dump(module1_queries, f, ensure_ascii=False, indent=2)
    print(f"Module 1: Generated queries saved to {module1_file}")
    return module1_file


def parse_args():
    parser = argparse.ArgumentParser(description="Unified SERP scraper CLI")
    parser.add_argument(
        '-q', '--queries', nargs='+', default=[],
        help='List of search queries to run (Module 2)'
    )
    parser.add_argument(
        '-f', '--queries-file', type=argparse.FileType('r'),
        help='Path to a JSON file containing a list of queries (Module 2)'
    )
    parser.add_argument(
        '--user-prompt', type=str,
        help='User prompt for LLM Web Interface Scraper (Module 1)'
    )
    parser.add_argument(
        '-m', '--max-se-index', type=int, default=250,
        help='Maximum index the search engine scraper will scrape up to'
    )
    parser.add_argument(
        '-h', '--har_files', default=0,
        help='List of .har files to parse'
    )
    parser.add_argument(
        '-i', '--index-interval', type=int, default=50, choices=range(1, 51), metavar='[1-50]',
        help='Interval at which to scrape indexes (1-50)'
    )
    parser.add_argument(
        '-s', '--search-engines', nargs='+', default=['bing', 'google'],
        choices=['bing', 'google', 'duckduckgo', 'yahoo'],
        help='Which search engines to use (choose one or more)'
    )
    parser.add_argument(
        '-o', '--output-dir', default='outputs',
        help='Directory where query folders and CSV files will be saved'
    )
    parser.add_argument(
        '-t', '--txt-eval-file', default='urls.txt',
        help='Path to the text file containing URLs to evaluate against CSV results'
    )
    parser.add_argument(
        '--module', nargs='+', type=int, choices=[1, 2, 3], required=True,
        help='Modules to run. Must be sequential (e.g., 1,2,3 or 2,3 or 1,2 or 1 or 3).'
    )
    return parser.parse_args()


def load_queries(cli_queries, file_handle):
    queries = []
    if file_handle:
        data = json.load(file_handle)
        if not isinstance(data, list):
            raise ValueError("JSON must contain a list of query strings")
        queries.extend(data)
    queries.extend(cli_queries)
    return queries


def main():
    args = parse_args()
    modules = sorted(args.module)
    # Validate sequential modules
    if modules != list(range(modules[0], modules[-1] + 1)):
        raise ValueError("Modules must be sequential (e.g., 1 2 3 or 2 3 or 1 2 or 3)")

    os.makedirs(args.output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    module1_query_file = None

    # Run Module 1
    if 1 in modules:
        if not args.user_prompt:
            raise ValueError("--user-prompt is required when running Module 1")
        safe_query = args.user_prompt.replace(' ', '_')
        module1_query_file = har_parser(args.har_files)

    # Prepare queries for Module 2
    queries = []
    if 2 in modules:
        if module1_query_file:
            with open(module1_query_file, 'r', encoding='utf-8') as f:
                queries = json.load(f)
        else:
            queries = load_queries(args.queries, args.queries_file)
        if not queries:
            raise ValueError("No queries provided for Module 2")

    # Run Module 2
    if 2 in modules:
        for query in queries:
            safe_query = query.replace(' ', '_')
            query_folder = os.path.join(args.output_dir, f"{safe_query[:12]}_{timestamp}")
            os.makedirs(query_folder, exist_ok=True)

            print(f"Starting scrape for '{query}' at {timestamp}, saving in '{query_folder}'")

            with open(os.path.join(query_folder, "query.txt"), 'w', encoding='utf-8') as f:
                f.write(query)

            for engine in args.search_engines:
                output_path = os.path.join(query_folder, f"{engine}_{safe_query[:12]}.csv")
                if engine == 'bing':
                    scrape_bing_to_csv(
                        query=query,
                        output_file=output_path,
                        max_results=args.max_se_index,
                        batch_size=args.index_interval
                    )
                elif engine == 'google':
                    scrape_google_to_csv(
                        query=query,
                        output_file=output_path,
                        max_results=args.max_se_index,
                        page_size=args.index_interval
                    )
                else:
                    print(f"Engine '{engine}' not supported yet. Skipping.")

    # Run Module 3
    if 3 in modules:
        # Determine target folders (if Module 2 ran, use its folders; otherwise scan all output_dir)
        if 2 in modules:
            safe_queries = [q.replace(' ', '_') for q in queries]
            target_folders = [os.path.join(args.output_dir, f"{sq[:12]}_{timestamp}") for sq in safe_queries]
        else:
            target_folders = [os.path.join(args.output_dir, d) for d in os.listdir(args.output_dir)
                              if os.path.isdir(os.path.join(args.output_dir, d))]

        if not target_folders:
            raise ValueError("No output folders with CSVs found in '" + args.output_dir + "'")

        for query_folder in target_folders:
            print(f"\nEvaluating URLs in '{args.txt_eval_file}' against scraped results in {query_folder}...")
            csvs = [os.path.join(query_folder, f) for f in os.listdir(query_folder) if f.endswith('.csv')]
            if not csvs:
                print(f"No CSV files found in {query_folder}, skipping.")
                continue
            results_filepath = os.path.join(query_folder, f"results_{timestamp}.txt")
            check_urls(csv_paths=csvs, txt_path=args.txt_eval_file, results_pathfile=results_filepath)
            print(f"Finished evaluation for folder '{query_folder}'\n")

    print("Selected modules completed.")


if __name__ == "__main__":
    main()
