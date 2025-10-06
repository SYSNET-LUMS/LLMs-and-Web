#!/usr/bin/env python3
"""
measure_accessed_urls.py

Scans a root directory for `query_meta.json` files, extracts `accessed` URLs,
measures bytes downloaded for each URL, and writes results to CSV and a log file.

New features:
 - CSV includes `is_cited` column (compares normalized URLs ignoring tracking params).
 - `--chunking` / `-c` mode: for each top-level directory under the root, split its
   immediate subdirectories into 10 random chunks (10% each). The script processes
   chunk 0 for all top-level dirs, waits for those tasks to finish, then chunk 1,
   etc. This reduces peak load on any single top-level folder while keeping the
   run moving.
 - Resume support for chunking: if an existing CSV log or log file is present the
   script reads them and treats already-finalized (meta_path, url) pairs as done
   and will skip them when submitting new work.

Terminal UI:
 - Shows exactly N slot lines (Thread 1..N) and a Total Progress line.
 - When a slot is working it shows a truncated URL: "Thread 3: working on https://..."
 - Nothing else is printed to the terminal (detailed info is written to the log file).

Logging:
 - Everything detailed goes to a log file under `.logs/run_of_{root_dir_name}_{timestamp}.log`
   unless overridden with --log-file.

Retries:
 - 429: respectful backoff (honors Retry-After if present), configurable max retries.
 - 403: exponential backoff, configurable max retries.
 - Retries are scheduled asynchronously so they do not hold worker slots.

Usage: same as before. Add `--chunking` or `-c` to enable chunked processing.
"""

import os
import json
import argparse
import csv
import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import Future
from pathlib import Path
from typing import List, Tuple, Optional, Dict
import sys
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import random
import math

# Try optional imports
try:
    import pycurl
    HAVE_PYCURL = True
except Exception:
    pycurl = None
    HAVE_PYCURL = False

try:
    import requests
    HAVE_REQUESTS = True
except Exception:
    requests = None
    HAVE_REQUESTS = False

# Shared state for progress/UI
slot_status: Dict[str, str] = {}
slot_lock = threading.Lock()
available_slots: List[str] = []
slot_semaphore = None  # initialized after parsing concurrency

completed_count = 0
completed_lock = threading.Lock()
stop_monitor = threading.Event()

# pending tasks counter (tracks initial + retry submissions)
pending_tasks = 0
pending_lock = threading.Lock()

logger = logging.getLogger(__name__)

# ---------- helpers for file discovery and meta parsing ----------

TRACKING_QS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'gclid', 'fbclid', 'utm', 'utm_name', 'utm_id'
}


def normalize_url_for_compare(u: str) -> str:
    """Normalize URL for comparison: remove tracking query params, lowercase scheme+netloc,
    strip fragment, and sort remaining query params. Returns canonical string."""
    try:
        p = urlparse(u)
    except Exception:
        return u.strip()
    scheme = (p.scheme or 'http').lower()
    netloc = (p.netloc or '').lower()
    path = p.path or ''
    qs = parse_qsl(p.query, keep_blank_values=True)
    filtered = [(k, v) for (k, v) in qs if k.lower() not in TRACKING_QS]
    filtered.sort()
    new_q = urlencode(filtered, doseq=True)
    normalized = urlunparse((scheme, netloc, path, '', new_q, ''))
    return normalized


def find_query_meta_files(root: str) -> List[str]:
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        if 'query_meta.json' in filenames:
            found.append(os.path.join(dirpath, 'query_meta.json'))
    return found


def extract_urls_from_meta(meta_path: str) -> Tuple[List[str], dict]:
    """Return (accessed_urls, meta) where meta includes prompt_id, category and cited_set
    (normalized) for quick lookups."""
    with open(meta_path, 'r', encoding='utf-8') as f:
        j = json.load(f)
    prompt_id = j.get('prompt_id')
    category = j.get('category')
    accessed = j.get('accessed', []) or []
    cites = j.get('cites', []) or []

    urls = []
    for item in accessed:
        if isinstance(item, str):
            urls.append(item)
        elif isinstance(item, dict):
            for k in ('url', 'uri', 'href', 'link'):
                if k in item and isinstance(item[k], str):
                    urls.append(item[k])
                    break
    # build normalized cited set
    cited_set = set()
    for c in cites:
        if isinstance(c, str):
            cited_set.add(normalize_url_for_compare(c))
        elif isinstance(c, dict):
            for k in ('url', 'uri', 'href', 'link'):
                if k in c and isinstance(c[k], str):
                    cited_set.add(normalize_url_for_compare(c[k]))
                    break
    meta = {'prompt_id': prompt_id, 'category': category, 'cited_set': cited_set}
    return urls, meta

# ---------- load previous progress ----------

def load_previous_progress(csv_path: str, log_path: Optional[str]) -> set:
    """Return a set of (meta_path, url) tuples which were already completed in a previous run.

    This checks both the CSV output (preferred) and the log file for "Job result:" entries.
    """
    done = set()

    # First read CSV if present
    try:
        if csv_path and os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if headers:
                    # find indices
                    try:
                        meta_idx = headers.index('meta_path')
                    except ValueError:
                        meta_idx = 0
                    try:
                        url_idx = headers.index('url')
                    except ValueError:
                        url_idx = 3
                    try:
                        final_idx = headers.index('final')
                    except ValueError:
                        final_idx = len(headers) - 1
                else:
                    meta_idx, url_idx, final_idx = 0, 3, -1

                for row in reader:
                    if len(row) <= max(meta_idx, url_idx, final_idx):
                        continue
                    final = row[final_idx].strip().lower()
                    if final in ('true', '1', '1.0', 'yes', 'y'):
                        done.add((row[meta_idx], row[url_idx]))
    except Exception:
        logger.exception('Failed to read existing CSV for resume; continuing without CSV info')

    # Next, parse log file for Job result lines (adds to the set)
    try:
        if log_path and os.path.exists(log_path):
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if 'Job result:' in line:
                        try:
                            after = line.split('Job result:', 1)[1]
                            parts = after.split('::')
                            if len(parts) >= 2:
                                mf = parts[0].strip()
                                url = parts[1].strip()
                                done.add((mf, url))
                        except Exception:
                            continue
    except Exception:
        logger.exception('Failed to read existing log for resume; continuing without log info')

    return done

# ---------- HTTP measurement helpers ----------

def parse_headers_raw(raw_lines: List[bytes]) -> Dict[str, str]:
    headers = {}
    for line in raw_lines:
        try:
            s = line.decode('iso-8859-1').strip()
        except Exception:
            continue
        if not s or ':' not in s:
            continue
        k, v = s.split(':', 1)
        headers[k.strip().lower()] = v.strip()
    return headers


def measure_url_pycurl(url: str, timeout: int = 30, user_agent: Optional[str] = None, decompress: bool = False) -> Tuple[Optional[int], Optional[int], Dict[str, str], Optional[str]]:
    if not HAVE_PYCURL:
        return None, None, {}, 'pycurl not installed'

    c = pycurl.Curl()
    c.setopt(pycurl.WRITEFUNCTION, lambda data: None)
    header_lines = []
    c.setopt(pycurl.HEADERFUNCTION, lambda h: header_lines.append(h))
    c.setopt(pycurl.URL, url)
    c.setopt(pycurl.FOLLOWLOCATION, True)
    c.setopt(pycurl.MAXREDIRS, 10)
    c.setopt(pycurl.CONNECTTIMEOUT, 10)
    c.setopt(pycurl.TIMEOUT, timeout)
    try:
        c.setopt(pycurl.NOSIGNAL, 1)
    except Exception:
        pass

    ua = user_agent or 'measure-bot/1.0'
    c.setopt(pycurl.USERAGENT, ua)

    if decompress:
        c.setopt(pycurl.ACCEPT_ENCODING, '')
    else:
        c.setopt(pycurl.HTTPHEADER, ['Accept-Encoding: gzip, deflate'])

    try:
        c.perform()
        size = c.getinfo(pycurl.SIZE_DOWNLOAD)
        code = c.getinfo(pycurl.RESPONSE_CODE)
        hdrs = parse_headers_raw(header_lines)
        c.close()
        return int(size), int(code), hdrs, None
    except Exception as e:
        try:
            c.close()
        except Exception:
            pass
        return None, None, {}, f'pycurl error: {e}'


def measure_url_requests(url: str, timeout: int = 30, user_agent: Optional[str] = None, decompress: bool = False) -> Tuple[Optional[int], Optional[int], Dict[str, str], Optional[str]]:
    if not HAVE_REQUESTS:
        return None, None, {}, 'requests not installed'
    headers = {'Accept-Encoding': 'gzip, deflate', 'User-Agent': user_agent or 'measure-bot/1.0'}
    try:
        with requests.get(url, headers=headers, stream=True, timeout=timeout, allow_redirects=True) as r:
            if not decompress:
                try:
                    raw = r.raw
                    raw.decode_content = False
                    total = 0
                    while True:
                        chunk = raw.read(8192)
                        if not chunk:
                            break
                        total += len(chunk)
                    return int(total), int(r.status_code), {k.lower(): v for k, v in r.headers.items()}, None
                except Exception:
                    total = 0
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            total += len(chunk)
                    return int(total), int(r.status_code), {k.lower(): v for k, v in r.headers.items()}, None
            else:
                total = 0
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        total += len(chunk)
                return int(total), int(r.status_code), {k.lower(): v for k, v in r.headers.items()}, None
    except Exception as e:
        return None, None, {}, f'requests error: {e}'

# ---------- worker wrapper, CSV writing, retry orchestration ----------

def _short_url(u: str, max_len: int = 120) -> str:
    if not u:
        return ''
    if len(u) <= max_len:
        return u
    return u[:max_len-3] + '...'


def worker_wrapper(meta_path: str, url: str, meta: dict, attempt: int, timeout: int, use_requests: bool,
                   max_retries_429: int, max_retries_403: int, user_agent: Optional[str], decompress: bool,
                   writer, write_lock, scheduler):
    """
    Acquire a slot, update slot_status to show the short current URL, perform measurement,
    write CSV and log, schedule retries if needed. Release slot on completion.
    """
    global completed_count, pending_tasks

    slot_id = None
    # Acquire a slot (this will block if all slots are busy)
    slot_semaphore.acquire()
    try:
        with slot_lock:
            slot_id = available_slots.pop(0)
            slot_status[slot_id] = f'working on {_short_url(url)}'

        # Perform actual measurement
        if (not use_requests) and HAVE_PYCURL:
            size, status, headers, note = measure_url_pycurl(url, timeout=timeout, user_agent=user_agent, decompress=decompress)
        else:
            size, status, headers, note = measure_url_requests(url, timeout=timeout, user_agent=user_agent, decompress=decompress)

        res = {
            'meta_path': meta_path,
            'prompt_id': meta.get('prompt_id'),
            'category': meta.get('category'),
            'url': url,
            'http_status': status,
            'bytes': size,
            'note': note,
            'attempt': attempt,
            'final': True,
            'retry': False,
            'retry_delay': None
        }

        # 429 handling
        if status == 429:
            ra = headers.get('retry-after')
            delay = None
            if ra:
                try:
                    delay = int(float(ra))
                except Exception:
                    delay = None
            if delay is None:
                delay = min(300, 30 * attempt)
            if attempt < max_retries_429:
                res['final'] = False
                res['retry'] = True
                res['retry_delay'] = delay
                res['note'] = (note or '') + f' 429 -> retry after {delay}s (attempt {attempt}/{max_retries_429})'
            else:
                res['note'] = (note or '') + f' 429 -> max retries exceeded ({max_retries_429})'

        # 403 handling
        elif status == 403:
            if attempt < max_retries_403:
                delay = 2 * (2 ** (attempt - 1))
                res['final'] = False
                res['retry'] = True
                res['retry_delay'] = delay
                res['note'] = (note or '') + f' 403 -> exponential backoff {delay}s (attempt {attempt}/{max_retries_403})'
            else:
                res['note'] = (note or '') + f' 403 -> max retries exceeded ({max_retries_403})'

        # Determine is_cited: normalize current URL and check meta['cited_set']
        normalized = normalize_url_for_compare(url)
        is_cited = False
        try:
            cited_set = meta.get('cited_set', set())
            if normalized in cited_set:
                is_cited = True
        except Exception:
            is_cited = False

        # Log detailed job result (file only)
        logger.info(f"Job result: {meta_path} :: {url} :: attempt={attempt} status={status} bytes={size} is_cited={is_cited} note={res['note']}")

        # Write CSV row (note: added is_cited column)
        with write_lock:
            writer.writerow([res['meta_path'], res['prompt_id'], res['category'], res['url'], res['http_status'], res['bytes'], is_cited, res['note'], res['attempt'], res['final']])

        # Update completion and pending counters if final
        if res['final']:
            with completed_lock:
                completed_count += 1
            with pending_lock:
                pending_tasks -= 1

        # If we need to retry, schedule without occupying a slot
        if res.get('retry'):
            next_attempt = res['attempt'] + 1
            delay = res['retry_delay'] or 30
            logger.info(f"Scheduling retry for {url} after {delay}s (attempt {next_attempt})")
            def schedule_cb():
                submit_task(meta_path, url, meta, next_attempt)
            scheduler.submit(lambda: (time.sleep(delay), schedule_cb()))

    except Exception as e:
        logger.exception(f'Unexpected worker error for {url}: {e}')
        with write_lock:
            writer.writerow([meta_path, meta.get('prompt_id'), meta.get('category'), url, None, None, False, f'worker exception: {e}', attempt, True])
        with completed_lock:
            completed_count += 1
        with pending_lock:
            pending_tasks -= 1
    finally:
        # release slot and mark as idle
        with slot_lock:
            if slot_id is not None:
                slot_status[slot_id] = 'idle'
                available_slots.append(slot_id)
        slot_semaphore.release()

# Helper to submit tasks (increments pending_tasks before submission)

def submit_task(meta_path, url, meta, attempt=1):
    global pending_tasks
    logger.debug(f'Queueing: {meta_path} :: {url} (attempt {attempt})')
    with pending_lock:
        pending_tasks += 1
    future = workers.submit(worker_wrapper, meta_path, url, meta, attempt, args.timeout, args.use_requests,
                             args.max_retries_429, args.max_retries_403, args.user_agent, args.decompress,
                             csv_writer, write_lock, scheduler)
    return future

# ---------- UI thread: show one line per slot + progress ----------

def ui_thread_fn(total_urls: int, poll_interval: float = 0.5):
    """
    Redraw the terminal to show exactly one line per worker slot and a Total Progress line.
    Only this function writes to stdout.
    """
    bar_width = 40
    while not stop_monitor.is_set():
        try:
            with slot_lock:
                ordered = sorted(slot_status.items(), key=lambda kv: int(kv[0].split()[1]))
            with completed_lock:
                comp = completed_count
            pct = (comp / total_urls * 100) if total_urls > 0 else 100.0
            filled = int((comp / total_urls) * bar_width) if total_urls > 0 else bar_width
            bar = '[' + ('#' * filled).ljust(bar_width) + ']'

            out_lines = []
            for slot_id, status in ordered:
                short = status
                if len(short) > 120:
                    short = short[:117] + '...'
                out_lines.append(f"{slot_id}: {short}")

            out_lines.append('')
            out_lines.append(f"Total URLs: {comp}/{total_urls} {bar} ({pct:.1f}%)")

            # Clear screen and print (only UI prints to stdout)
            
            sys.stdout.write('\033[2J\033[H')
            sys.stdout.write('\n'.join(out_lines) + '\n')
            sys.stdout.flush()

        except Exception:
            logger.exception('ui thread error')
        time.sleep(poll_interval)

# ---------- logging setup ----------

def setup_file_logging(root_dir: str, log_file_arg: Optional[str] = None) -> str:
    """Configure logging to a file only. Returns the path to the log file."""
    logs_dir = Path('.logs')
    logs_dir.mkdir(exist_ok=True)

    if log_file_arg:
        log_path = Path(log_file_arg)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        root_name = Path(root_dir).name or 'run'
        ts = time.strftime('%Y%m%d_%H%M%S')
        log_path = logs_dir / f'run_of_{root_name}_{ts}.log'

    root_logger = logging.getLogger()
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    fh = logging.FileHandler(str(log_path), encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    root_logger.addHandler(fh)
    root_logger.setLevel(logging.INFO)

    return str(log_path)

# ---------- main flow ----------

def main():
    global workers, scheduler, csv_writer, write_lock, slot_semaphore, available_slots, slot_status, args, pending_tasks, completed_count

    p = argparse.ArgumentParser(description='Measure HTML bytes for URLs listed in query_meta.json files (with polite retries)')
    p.add_argument('root', help='root directory to scan for query_meta.json')
    p.add_argument('--output', '-o', default='measured_sizes.csv', help='CSV output file')
    p.add_argument('--concurrency', '-j', type=int, default=20, help='worker concurrency')
    p.add_argument('--timeout', type=int, default=30, help='per-request timeout seconds')
    p.add_argument('--use-requests', action='store_true', help='force fallback to requests instead of pycurl')
    p.add_argument('--dedupe', action='store_true', help='deduplicate URLs per meta file')
    p.add_argument('--max-retries-429', type=int, default=5, help='max retries for 429 responses')
    p.add_argument('--max-retries-403', type=int, default=3, help='max retries for 403 responses')
    p.add_argument('--decompress', action='store_true', help='enable automatic decompression (match curl --compressed)')
    p.add_argument('--user-agent', type=str, default='measure-bot/1.0', help='User-Agent header to send')
    p.add_argument('--debug-url', type=str, default=None, help='If set, perform a single debug fetch of this URL and exit')
    p.add_argument('--log-file', type=str, default=None, help='file to write logs to (overrides automatic naming)')
    p.add_argument('--chunking', '-c', action='store_true', help='Enable chunked processing across top-level dirs (10%% batches)')
    args = p.parse_args()

    if args.use_requests and not HAVE_REQUESTS:
        print('requests requested but not installed. Install with: pip install requests', file=sys.stderr)
        return
    if (not args.use_requests) and (not HAVE_PYCURL):
        print('pycurl not available; switching to requests fallback. To install pycurl: pip install pycurl', file=sys.stderr)

    # configure file-only logging
    log_path = setup_file_logging(args.root, args.log_file)
    logger.info(f'Logging to {log_path}')

    # debug fetch prints directly to terminal (user explicitly asked)
    if args.debug_url:
        if (not args.use_requests) and HAVE_PYCURL:
            size, status, headers, note = measure_url_pycurl(args.debug_url, timeout=args.timeout, user_agent=args.user_agent, decompress=args.decompress)
        else:
            size, status, headers, note = measure_url_requests(args.debug_url, timeout=args.timeout, user_agent=args.user_agent, decompress=args.decompress)

        print('URL:', args.debug_url)
        print('HTTP status:', status)
        print('Bytes reported:', size)
        print('Note:', note)
        print('Response headers:')
        for k, v in headers.items():
            print(f'  {k}: {v}')
        return

    # Discover all meta files to compute overall total URLs (used by UI)
    all_meta_files = find_query_meta_files(args.root)
    total_urls_all = 0
    meta_file_access_counts = {}
    for mf in all_meta_files:
        try:
            urls, meta = extract_urls_from_meta(mf)
            meta_file_access_counts[mf] = len(urls)
            total_urls_all += len(urls)
        except Exception:
            meta_file_access_counts[mf] = 0

    logger.info(f'Found {len(all_meta_files)} meta files with total {total_urls_all} accessed URLs')

    # configure resume: read existing CSV/log to find completed (meta_path,url) pairs
    completed_set = load_previous_progress(args.output, log_path)
    # initialize completed_count from previous run
    with completed_lock:
        completed_count = len(completed_set)

    # remaining total shown to UI is total_urls_all - already completed
    total_urls = max(0, total_urls_all - len(completed_set))

    # open CSV (note is_cited column) - open in append mode so we don't overwrite previous results
    out_exists = os.path.exists(args.output)
    out_f = open(args.output, 'a', newline='', encoding='utf-8')
    writer = csv.writer(out_f)
    # If file didn't exist, write header
    if not out_exists:
        writer.writerow(['meta_path', 'prompt_id', 'category', 'url', 'http_status', 'bytes', 'is_cited', 'note', 'attempt', 'final'])
        out_f.flush()

    write_lock = threading.Lock()

    workers = ThreadPoolExecutor(max_workers=args.concurrency)
    scheduler = ThreadPoolExecutor(max_workers=1)

    # set up worker slots
    slot_semaphore = threading.Semaphore(args.concurrency)
    with slot_lock:
        available_slots = [f'Thread {i+1}' for i in range(args.concurrency)]
        slot_status = {slot: 'idle' for slot in available_slots}

    # start UI thread (only UI prints to stdout)
    ui_thread = threading.Thread(target=ui_thread_fn, args=(total_urls,), daemon=True)
    ui_thread.start()

    # expose csv_writer globally for worker_wrapper
    csv_writer = writer

    try:
        if not args.chunking:
            # Simple: submit all tasks at once, skipping already-completed
            for mf in all_meta_files:
                try:
                    urls, meta = extract_urls_from_meta(mf)
                except Exception as e:
                    logger.exception(f'Failed to read {mf}: {e}')
                    continue
                if args.dedupe:
                    seen = set(); uniq = []
                    for u in urls:
                        if u not in seen:
                            seen.add(u); uniq.append(u)
                    urls = uniq
                for url in urls:
                    if (mf, url) in completed_set:
                        continue
                    submit_task(mf, url, meta, 1)

            logger.info('Submitted all initial tasks (skipping already completed)')

            # wait until all pending tasks (initial + retries) have completed
            while True:
                with pending_lock:
                    if pending_tasks == 0:
                        break
                time.sleep(0.5)

        else:
            # Chunking mode: for each top-level dir under root, take its immediate subdirs,
            # shuffle them, split into 10 chunks, and process chunk-by-chunk across all top-level dirs.
            root_path = Path(args.root)
            top_level_dirs = [str(p) for p in root_path.iterdir() if p.is_dir()]
            if not top_level_dirs:
                # fallback to treating root as single top-level
                top_level_dirs = [str(root_path)]

            # prepare per-top-level subdir lists
            per_top_subdirs = {}
            for top in top_level_dirs:
                try:
                    subdirs = [str(p) for p in Path(top).iterdir() if p.is_dir()]
                except Exception:
                    subdirs = []
                random.shuffle(subdirs)
                per_top_subdirs[top] = subdirs

            # compute number of chunks as 10; create chunks indices
            NUM_CHUNKS = 10
            # For chunk_idx in 0..NUM_CHUNKS-1, build combined list of meta files from that chunk for each top-level dir
            for chunk_idx in range(NUM_CHUNKS):
                chunk_meta_files = []
                for top, subdirs in per_top_subdirs.items():
                    if not subdirs:
                        # if no subdirs, consider searching top itself on chunk 0
                        if chunk_idx == 0:
                            chunk_dirs = [top]
                        else:
                            chunk_dirs = []
                    else:
                        chunk_size = max(1, math.ceil(len(subdirs) / NUM_CHUNKS))
                        start = chunk_idx * chunk_size
                        chunk_dirs = subdirs[start:start+chunk_size]

                    # for each selected dir, find meta files under it
                    for d in chunk_dirs:
                        try:
                            mfs = find_query_meta_files(d)
                            chunk_meta_files.extend(mfs)
                        except Exception:
                            pass

                # remove duplicates and sort for deterministic order
                chunk_meta_files = sorted(set(chunk_meta_files))

                # Filter out meta files that contain only already-completed URLs
                filtered_chunk_meta_files = []
                for mf in chunk_meta_files:
                    try:
                        urls, meta = extract_urls_from_meta(mf)
                    except Exception:
                        continue
                    # if any url in this meta file remains to be processed, keep it
                    any_remaining = any(((mf, u) not in completed_set) for u in urls)
                    if any_remaining:
                        filtered_chunk_meta_files.append(mf)

                if not filtered_chunk_meta_files:
                    logger.info(f'Chunk {chunk_idx}: no meta files with remaining URLs; skipping')
                    continue

                logger.info(f'Processing chunk {chunk_idx+1}/{NUM_CHUNKS} with {len(filtered_chunk_meta_files)} meta files')

                # submit tasks for this chunk (skipping completed URLs)
                for mf in filtered_chunk_meta_files:
                    try:
                        urls, meta = extract_urls_from_meta(mf)
                    except Exception as e:
                        logger.exception(f'Failed to read {mf}: {e}')
                        continue
                    if args.dedupe:
                        seen = set(); uniq = []
                        for u in urls:
                            if u not in seen:
                                seen.add(u); uniq.append(u)
                        urls = uniq
                    for url in urls:
                        if (mf, url) in completed_set:
                            continue
                        submit_task(mf, url, meta, 1)

                # wait until this chunk's work and its retries complete
                logger.info(f'Waiting for chunk {chunk_idx+1} to finish')
                while True:
                    with pending_lock:
                        if pending_tasks == 0:
                            break
                    time.sleep(0.5)
                logger.info(f'Chunk {chunk_idx+1} completed')

    except KeyboardInterrupt:
        logger.warning('Interrupted by user; shutting down')
    finally:
        workers.shutdown(wait=True)
        scheduler.shutdown(wait=True)
        stop_monitor.set()
        ui_thread.join(timeout=2)
        # ensure CSV file flushed to disk
        try:
            out_f.flush()
            out_f.close()
        except Exception:
            pass
        logger.info('Shutdown complete')

if __name__ == '__main__':
    main()
