import csv
import urllib.parse
import contextlib

# —— Helpers —— #

def normalize_url(url):
    """
    Strip scheme, www, query, fragment, trailing slash, lowercase.
    Returns (domain_core, path).
    """
    p = urllib.parse.urlparse(url.strip())
    # hostname without www
    host = (p.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    # remove port if present (unlikely in SERP)
    host = host.split(':')[0]

    # heuristic for domain core:
    parts = host.split('.')
    # common 2‑part suffix indicators
    suffix_inds = {"co","com","net","org","gov","ac","edu"}
    if len(parts) >= 3 and parts[-1].isalpha() and len(parts[-1])<=3 and parts[-2] in suffix_inds:
        core = parts[-3]
    elif len(parts) >= 2:
        core = parts[-2]
    else:
        core = parts[0]

    # path without trailing slash
    path = p.path.rstrip('/')
    return core, path

def load_csv_index(csv_path):
    """
    Returns:
      - exact_map: {url -> row_index}
      - norm_map:  {(domain_core, path) -> [(url, row_index), ...]}
    """
    exact_map = {}
    norm_map  = {}
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            url = row.get('URL','').strip()
            if not url:
                continue
            if url not in exact_map:
                exact_map[url] = idx
            # build normalized map
            core, path = normalize_url(url)
            norm_map.setdefault((core,path), []).append((url, idx))
    return exact_map, norm_map

def load_text_urls(txt_path):
    with open(txt_path, encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

# —— Main check —— #

def check_urls(csv_paths=None, txt_path='urls.txt', results_pathfile='results.txt'):
    if csv_paths is None:
        csv_paths = ['bing_results.csv', 'serper_results.csv']

    # load maps for each CSV
    csv_data = {}
    for path in csv_paths:
        exact_map, norm_map = load_csv_index(path)
        csv_data[path] = {
            "exact": exact_map,
            "norm": norm_map
        }

    text_urls = load_text_urls(txt_path)
    found = {}      # url -> list of (path, row, method)
    not_found = []

    for url in text_urls:
        matched = []
        # 1) exact
        for path, maps in csv_data.items():
            if url in maps["exact"]:
                matched.append((path, maps["exact"][url], "exact"))
        # 2) normalized
        # if not matched:
        #     core, pathp = normalize_url(url)
        #     for path, maps in csv_data.items():
        #         hits = maps["norm"].get((core, pathp), [])
        #         for hit_url, row in hits:
        #             matched.append((path, row, "normalized"))
        if matched:
            found[url] = matched
        else:
            not_found.append(url)

    # —— Reporting —— #
    with open(results_pathfile, "w", encoding="utf-8") as f, contextlib.redirect_stdout(f):
        # — your existing code starts here —
        print(f"Checked {len(text_urls)} URLs against:")
        for path in csv_paths:
            total = len(csv_data[path]["exact"])
            print(f"  - {path}: {total} entries")
        print()

        if found:
            print("✅ Found:")
            for url, hits in found.items():
                print(f"  {url}")
                for path, row, method in hits:
                    tag = "↳" if method=="normalized" else "→"
                    print(f"    {tag} {path} (row {row}) [{method}]")
        else:
            print("✅ Found: (none)")
        print()

        if not_found:
            print("❌ Not found:")
            for url in not_found:
                print(f"  {url}")
        else:
            print("❌ Not found: (none)")
        # — your existing code ends here —

    print(f"All output has been written to {results_pathfile}")