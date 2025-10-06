#!/usr/bin/env python3
"""
Create per-results-folder query metadata from HARs (GPT-o4 & GPT-5).

Matching rule (timestamp ignored):
  results/<category>/network-logs-prompt-<ID>_<ANY>/
  -> datasets/<category>/*hars*/network-logs-prompt-<ID>.har

We parse via chatgpt_scraper.har_parser.process_har_files and write:
  <results_folder>/query_meta.json

URL fields in the output are standardized to:
  - accessed : normal/accessed URLs
  - cites    : cited/given URLs
"""

import re
import json
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional

# Use your current parser helpers
from chatgpt_scraper.har_parser import har_parser

# Match result subdirs like: network-logs-prompt-249 or network-logs-prompt-249_20250814_011917
RESULTS_DIR_PATTERN = re.compile(r"^network-logs-prompt-(?P<prompt_id>\d+)(?:_.*)?$", re.I)


def find_har_for_results_dir(
    rdir: Path,
    results_root: Path,
    datasets_root: Optional[Path],
    prompt_id: str,
) -> List[Path]:
    """
    Return [<exact har path>] or [].

    Mapping (timestamp ignored):
      results/<category>/network-logs-prompt-<ID>_<ANY>/
      -> datasets/<category>/*hars*/network-logs-prompt-<ID>.har
    """
    # Determine <category> from results path
    try:
        rel = rdir.relative_to(results_root)
        category = rel.parts[0] if rel.parts else rdir.parent.name
    except Exception:
        category = rdir.parent.name

    # Search datasets/<category>/*hars*/network-logs-prompt-<ID>.har
    if datasets_root:
        cat_dir = datasets_root / category
        if cat_dir.exists():
            har_dirs = [p for p in cat_dir.iterdir() if p.is_dir() and "hars" in p.name.lower()]

            # Prefer *_gpt-5* dir when category hints GPT-5
            prefer_gpt5 = ("gpt-5" in category.lower()) or ("gpt5" in category.lower())

            def sort_key(p: Path):
                n = p.name.lower()
                # if prefer_gpt5: put non-gpt dirs later
                return (prefer_gpt5 and not ("gpt" in n), n)

            for hd in sorted(har_dirs, key=sort_key):
                hp = hd / f"network-logs-prompt-{prompt_id}.har"
                if hp.exists():
                    return [hp.resolve()]

    # Fallbacks (rare): look inside the results folder or its parent category folder
    local = rdir / f"network-logs-prompt-{prompt_id}.har"
    if local.exists():
        return [local.resolve()]

    parent_local = rdir.parent / f"network-logs-prompt-{prompt_id}.har"
    if parent_local.exists():
        return [parent_local.resolve()]

    return []


def aggregate_results(har_paths: List[Path], version: str) -> Dict[str, Any]:
    """Run your parser and normalize fields → accessed / cites."""
    if not har_paths:
        return {"hars": [], "error": "No HARs found"}

    parsed: List[Dict[str, Any]] = har_parser(
        [str(p) for p in har_paths],
        # target_url="https://chatgpt.com/backend-api/f/conversation",
    )

    all_search_strings: List[str] = []
    all_accessed: List[str] = []
    all_cites: List[str] = []
    total_accessed = 0
    total_cites = 0

    per_har: List[Dict[str, Any]] = []
    for r in parsed:
        if r.get("error"):
            per_har.append({"harname": r.get("harname"), "error": r["error"]})
            continue

        # Map to standardized names
        sstrings = r.get("search_strings", []) or []
        accessed = r.get("url", []) or []       # previously normal/accessed
        cites = r.get("cited_url", []) or []    # previously given/cited

        total_accessed += int(r.get("n_accessed", 0) or 0)
        total_cites += int(r.get("n_given", 0) or 0)

        per_har.append({
            "harname": r.get("harname"),
            "search_strings": sstrings,
            "accessed": accessed,
            "cites": cites,
            "n_accessed": r.get("n_accessed"),
            "n_cites": r.get("n_given"),
        })

        for q in sstrings:
            if q not in all_search_strings:
                all_search_strings.append(q)
        for u in accessed:
            if u not in all_accessed:
                all_accessed.append(u)
        for u in cites:
            if u not in all_cites:
                all_cites.append(u)

    return {
        "version": version,
        "total_hars": len(har_paths),
        "totals": {
            "unique_search_strings": len(all_search_strings),
            "unique_accessed_urls": len(all_accessed),
            "unique_cites": len(all_cites),
            "accessed_count": total_accessed,
            "cites_count": total_cites,
        },
        "search_strings": all_search_strings,
        "accessed": all_accessed,
        "cites": all_cites,
        "hars": per_har,
    }


def detect_version_for_category(category: str) -> str:
    """By default, use GPT-5 if the category name contains 'gpt-5'."""
    return "gpt5" if ("gpt-5" in category.lower() or "gpt5" in category.lower()) else "gpt-o4"


def main():
    ap = argparse.ArgumentParser(description="Create query_meta.json per results folder (GPT-o4 & GPT-5 aware).")
    ap.add_argument("--results-root", required=True, help="Root of results tree (e.g., ./results)")
    ap.add_argument("--datasets-root", required=True, help="Datasets root (e.g., ./datasets)")
    ap.add_argument("--version", choices=["gpt-o4", "gpt5", "auto"], default="auto",
                    help="Parsing mode; 'auto' selects from category name")
    ap.add_argument("--write-filename", default="query_meta.json",
                    help="Output JSON filename placed in each results folder")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would be written without creating files")
    args = ap.parse_args()

    results_root = Path(args.results_root).resolve()
    datasets_root = Path(args.datasets_root).resolve()

    if not results_root.exists():
        raise SystemExit(f"results_root does not exist: {results_root}")
    if not datasets_root.exists():
        raise SystemExit(f"datasets_root does not exist: {datasets_root}")

    # Find all network-logs-prompt-* folders (any timestamp variant)
    result_dirs: List[Path] = []
    for p in results_root.rglob("*"):
        if p.is_dir() and RESULTS_DIR_PATTERN.match(p.name):
            result_dirs.append(p)

    if not result_dirs:
        print(f"No results folders matching 'network-logs-prompt-*' under {results_root}")
        return

    print(f"Found {len(result_dirs)} results folders.")

    for rdir in sorted(result_dirs):
        m = RESULTS_DIR_PATTERN.match(rdir.name)
        assert m
        prompt_id = m.group("prompt_id")

        # Derive category for version and datasets path
        try:
            rel = rdir.relative_to(results_root)
            category = rel.parts[0] if rel.parts else rdir.parent.name
        except Exception:
            category = rdir.parent.name

        # Version selection
        version = detect_version_for_category(category) if args.version == "auto" else args.version

        # Determine the single HAR for this results dir
        har_paths = find_har_for_results_dir(rdir, results_root, datasets_root, prompt_id)

        summary = aggregate_results(har_paths, version)
        out_path = rdir / args.write_filename
        payload = {
            "prompt_id": int(prompt_id),
            "category": category,
            "results_folder": str(rdir),
            **summary
        }

        if args.dry_run:
            print(f"\n[{rdir}] Would write {args.write_filename} with {len(summary.get('hars', []))} HAR(s).")
            print(json.dumps(payload, indent=2)[:1000] + ("...\n" if len(json.dumps(payload)) > 1000 else "\n"))
        else:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            print(f"Wrote {out_path}  (HARs: {len(summary.get('hars', []))}, ver: {version})")


if __name__ == "__main__":
    main()
