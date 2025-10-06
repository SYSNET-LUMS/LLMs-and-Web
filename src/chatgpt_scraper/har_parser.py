# import json
# import re
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple


# def parse_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Extract all timing, size, metadata fields, and raw content text from a single HAR entry.
#     """
#     metrics: Dict[str, Any] = {}

#     # Top-level metadata
#     metrics['priority'] = entry.get('_priority')
#     metrics['resourceType'] = entry.get('_resourceType')
#     metrics['pageref'] = entry.get('pageref')
#     metrics['connection_id'] = entry.get('connection')
#     metrics['server_ip_address'] = entry.get('serverIPAddress')
#     metrics['startedDateTime'] = entry.get('startedDateTime')
#     metrics['time_total_ms'] = entry.get('time')

#     # Request block
#     req = entry.get('request', {})
#     metrics['request_method'] = req.get('method')
#     metrics['request_url'] = req.get('url')
#     metrics['request_httpVersion'] = req.get('httpVersion')
#     metrics['request_headers_count'] = len(req.get('headers', []))
#     metrics['request_query_count'] = len(req.get('queryString', []))
#     metrics['request_cookies_count'] = len(req.get('cookies', []))
#     metrics['request_headers_size'] = req.get('headersSize')
#     metrics['request_body_size'] = req.get('bodySize')

#     # Post-data
#     post = req.get('postData')
#     if post:
#         metrics['postData_mimeType'] = post.get('mimeType')
#         text = post.get('text')
#         if text is not None:
#             metrics['postData_text_length'] = len(text)

#     # Response block
#     res = entry.get('response', {})
#     metrics['response_status'] = res.get('status')
#     metrics['response_httpVersion'] = res.get('httpVersion')
#     metrics['response_headers_count'] = len(res.get('headers', []))
#     metrics['response_cookies_count'] = len(res.get('cookies', []))
#     metrics['response_headers_size'] = res.get('headersSize')
#     metrics['response_body_size'] = res.get('bodySize')

#     # Content sub-block
#     content = res.get('content', {})
#     metrics['content_size'] = content.get('size')
#     metrics['content_mimeType'] = content.get('mimeType')
#     metrics['content_text'] = content.get('text')
#     metrics['transfer_size'] = res.get('_transferSize')

#     # Cache info
#     cache = entry.get('cache', {})
#     metrics['cache_beforeRequest'] = cache.get('beforeRequest')
#     metrics['cache_afterRequest'] = cache.get('afterRequest')

#     # Detailed timings
#     timings = entry.get('timings', {})
#     for phase, t in timings.items():
#         metrics[f'time_{phase}_ms'] = t

#     return metrics


# def parse_sse_stream(content_text: str) -> List[Dict[str, Any]]:
#     """
#     Parse a Server-Sent Events (SSE) stream into a list of events.
#     """
#     entries: List[Dict[str, Any]] = []
#     last_event_type: Optional[str] = None

#     for chunk in content_text.strip().split("\n\n"):
#         lines = chunk.splitlines()
#         event_type = None
#         data_parts: List[str] = []

#         for line in lines:
#             if line.startswith("event:"):
#                 event_type = line.split("event:", 1)[1].strip()
#             elif line.startswith("data:"):
#                 data_parts.append(line.split("data:", 1)[1].strip())

#         if event_type is not None:
#             last_event_type = event_type
#         event_type = event_type or last_event_type

#         data_str = "".join(data_parts)
#         try:
#             payload: Any = json.loads(data_str)
#         except json.JSONDecodeError:
#             payload = data_str

#         entries.append({"eventType": event_type, "payload": payload})

#     return entries

# def extract_search_terms(entry: Dict[str, Any], parsed_events: List[Dict[str, Any]], version: str) -> List[str]:
#     """
#     For gpt-o4: extract from SSE deltas and metadata as before.
#     For gpt-5: the initial user prompt is in the request postData.
#     """
#     if version == 'gpt5':
#         search_call_re = re.compile(r'search\(\s*["\'](.*?)["\']\s*\)', re.DOTALL)
#         queries: List[str] = []

#         for ev in parsed_events:
#             if ev.get("eventType") != "delta":
#                 continue
#             d = ev["payload"]
#             if not isinstance(d, dict):
#                 continue

#             # look for a batch-patch that appends to /message/metadata
#             if d.get("o") == "patch" and isinstance(d.get("v"), list):
#                 for op in d["v"]:
#                     if op.get("p") == "/message/metadata" and op.get("o") == "append":
#                         meta = op.get("v")
#                         if isinstance(meta, dict):
#                             sqs = meta.get("search_queries")
#                             if isinstance(sqs, list):
#                                 for sq in sqs:
#                                     q = sq.get("q")
#                                     if isinstance(q, str):
#                                         queries.append(q)
            

#             # 2) From search("...") in message.content.text
#             #    (message can be at d["message"] OR d["v"]["message"])
#             cand_msgs = []
#             if isinstance(d.get("message"), dict):
#                 cand_msgs.append(d["message"])
#             if isinstance(d.get("v"), dict) and isinstance(d["v"].get("message"), dict):
#                 cand_msgs.append(d["v"]["message"])

#             for msg in cand_msgs:
#                 content = msg.get("content")
#                 if isinstance(content, dict):
#                     text = content.get("text")
#                     if isinstance(text, str):
#                         for m in search_call_re.findall(text):
#                             if isinstance(m, str) and m:
#                                 queries.append(m)

#         # de-duplicate while preserving order
#         seen = set()
#         deduped = []
#         for q in queries:
#             if q not in seen:
#                 seen.add(q)
#                 deduped.append(q)

#         return queries
#     else:
#         # original extract_search_queries logic
#         return extract_search_queries(parsed_events)


# def extract_search_queries(parsed_events: List[Dict[str, Any]]) -> List[str]:
#     """Extract 'search_queries' values from SSE deltas."""
#     queries: List[str] = []
#     for ev in parsed_events:
#         if ev.get("eventType") != "delta":
#             continue
#         d = ev["payload"]
#         if not isinstance(d, dict) or d.get("o") != "patch" or not isinstance(d.get("v"), list):
#             continue
#         for op in d["v"]:
#             if op.get("p") == "/message/metadata" and op.get("o") == "append":
#                 meta = op.get("v")
#                 if isinstance(meta, dict):
#                     for sq in meta.get("search_queries", []):
#                         q = sq.get("q")
#                         if isinstance(q, str):
#                             queries.append(q)
#     return queries

# def count_urls(parsed_events: List[Dict[str, Any]]) -> None:
#     """
#     Given a list of SSE events as returned by parse_sse_stream(),
#     prints how many URLs GPT accessed (during the search phase)
#     vs. how many it actually returned in its final response.

#     We start collecting “accessed” URLs once we see the assistant
#     append "Searching" to its thoughts, and we stop (and begin collecting
#     “given” URLs) once we hit the finished_successfully separator.
#     """
#     accessed: List[str] = []
#     given:    List[str] = []
#     seen_search_marker = False
#     after_sep = False
#     after_sep_counter = 0

#     for ev in parsed_events:
#         if ev.get("eventType") != "delta":
#             continue

#         d = ev["payload"]
#         if not isinstance(d, dict):
#             continue

#         # # 1) detect search kickoff
#         # if d.get("p") == "/message/content/thoughts/0/summary" and d.get("v") == "Searching":
#         #     seen_search_marker = True
#         #     continue

#         # 2) detect separator (end of search phase)
#         if (d.get("p") == "/message/status"
#             and d.get("o") == "replace"
#             and d.get("v") == "finished_successfully"):
#             after_sep_counter += 1
#             if after_sep_counter == 1:
#                 after_sep = True
#             continue

#         # 3a) pre-separator: collect any search_result_group URLs
#         if not after_sep:
#             # Case A: delta contains a list of search_result_group objects
#             if isinstance(d.get("v"), list):
#                 for item in d["v"]:
#                     if isinstance(item, dict) and item.get("type") == "search_result_group":
#                         for entry in item.get("entries", []):
#                             url = entry.get("url")
#                             if url:
#                                 accessed.append(url)

#             # Case B: delta is metadata/search_result_groups/.../entries
#             #    where d["v"] is a list of plain search_result dicts
#             if isinstance(d.get("p"), str) and "/search_result_groups" in d["p"] and d["p"].endswith("/entries"):
#                 for entry in d["v"]:
#                     if isinstance(entry, dict):
#                         url = entry.get("url")
#                         if url:
#                             accessed.append(url)

#         # # 3b) post-separator: collect only url_moderation URLs
#         # if after_sep and d.get("type") == "url_moderation":
#         #     um = d.get("url_moderation_result", {})
#         #     url = um.get("full_url")
#         #     if url:
#         #         given.append(url)

#         # 3b) post-separator: collect only url_moderation URLs
#         if after_sep and d.get("type") == "url_moderation":
#             um = d.get("url_moderation_result", {})
#             url = um.get("full_url")
#             if url:
#                 given.append(url)



#     # print counts
#     # print(f"GPT accessed URLs ({len(accessed)}):")
#     # for u in accessed:
#     #     print("  ", u)

#     # print(f"\nURLs in assistant response ({len(given)}):")
#     # for u in given:
#     #     print("  ", u)
    
#     return accessed, given

# def extract_urls(parsed_events: List[Dict[str, Any]], version: str) -> Tuple[List[str], List[str], List[str], List[str]]:
#     if version == 'gpt5':
#         # gpt-5 count_urls returns accessed, given
#         accessed, given = count_urls(parsed_events)
#         # dedupe & classify utm
#         normal, cited = [], []
#         for u in accessed + given:
#             if 'utm_source=chatgpt.com' in u:
#                 if u not in cited:
#                     cited.append(u)
#             elif u not in normal:
#                 normal.append(u)
#         return accessed, given, normal, cited
#     else:
#         # original extract_urls logic
#         return extract_urls_original(parsed_events)
    
# def extract_urls_original(parsed_events: List[Dict[str, Any]]) -> Tuple[List[str], List[str], List[str], List[str]]:
#     """
#     Walk SSE events, split into:
#       - accessed URLs (pre-response)
#       - given URLs    (post-response moderation)
#     Then classify all_urls into:
#       - normal_urls  (no utm)
#       - cited_urls   (contain utm_source=chatgpt.com)
#     Returns (accessed, given, normal_urls, cited_urls)
#     """
#     accessed: List[str] = []
#     given: List[str] = []
#     seen_sep = False
#     sep_count = 0

#     for ev in parsed_events:
#         if ev.get("eventType") != "delta":
#             continue
#         d = ev["payload"]
#         if not isinstance(d, dict):
#             continue
#         # detect separator (second finished_successfully)
#         if d.get("p") == "/message/status" and d.get("o") == "replace" and d.get("v") == "finished_successfully":
#             sep_count += 1
#             if sep_count == 2:
#                 seen_sep = True
#             continue
#         if not seen_sep:
#             # search_result_group entries embedded
#             if isinstance(d.get("v"), list):
#                 for item in d["v"]:
#                     if isinstance(item, dict) and item.get("type") == "search_result_group":
#                         for ent in item.get("entries", []):
#                             url = ent.get("url")
#                             if url:
#                                 accessed.append(url)
#             # explicit entries path
#             if isinstance(d.get("p"), str) and "/search_result_groups" in d.get("p") and d.get("p").endswith("/entries"):
#                 for ent in d["v"]:
#                     url = ent.get("url")
#                     if url:
#                         accessed.append(url)
#         else:
#             # after second separator: URL moderation
#             if d.get("type") == "url_moderation":
#                 um = d.get("url_moderation_result", {}) or {}
#                 url = um.get("full_url")
#                 if url:
#                     given.append(url)

#     # dedupe preserving order
#     all_urls = accessed + given
#     normal_urls: List[str] = []
#     cited_urls: List[str] = []
#     for u in all_urls:
#         if u in normal_urls or u in cited_urls:
#             continue
#         if 'utm_source=chatgpt.com' in u:
#             cited_urls.append(u)
#         else:
#             normal_urls.append(u)

#     return accessed, given, normal_urls, cited_urls


# def process_har_files(har_list: List[str], target_url: str, version: str = 'gpt-o4') -> List[Dict[str, Any]]:
#     results: List[Dict[str, Any]] = []
#     for har_path in har_list:
#         try:
#             with open(har_path, 'r', encoding='utf-8') as f:
#                 har = json.load(f)
#             entries = har.get('entries') or har.get('log', {}).get('entries', [])
#             matched = next((e for e in entries if e.get('request', {}).get('url') == target_url), None)
#             if not matched:
#                 raise ValueError(f"No entry with URL '{target_url}' in {har_path}")

#             metrics = parse_entry(matched)
#             events = parse_sse_stream(metrics.get('content_text', ''))

#             # version-aware extraction
#             search_terms = extract_search_terms(matched, events, version)
#             accessed, given, normal_urls, cited_urls = extract_urls(events, version)

#             results.append({
#                 'harname': har_path,
#                 'search_strings': search_terms,
#                 'url': normal_urls,
#                 'cited_url': cited_urls,
#                 'metrics': metrics,
#                 'n_accessed': len(accessed),
#                 'n_given': len(given),
#             })
#         except Exception as e:
#             results.append({'harname': har_path, 'error': str(e)})
#     return results


# def har_parser(har_list: List[str], version: str = 'gpt-o4') -> List[Dict[str, Any]]:
#     """
#     Entry point: processes HAR files for the chatgpt conversation endpoint
#     and prints totals of search strings and URLs.
#     """
#     target = "https://chatgpt.com/backend-api/f/conversation"
#     results = process_har_files(har_list, target, version)

#     # Summarize totals
#     total_searches = sum(len(r.get('search_strings', [])) for r in results if not r.get('error'))
#     total_urls = sum(len(r.get('url', [])) for r in results if not r.get('error'))
#     for r in results:
#         print(r.get('url'))
#         print(r.get('search_strings'))
#     print(f"Total search strings across all files: {total_searches}")
#     print(f"Total URLs across all files: {total_urls}")

#     return results

import json
import re
import html
from typing import Any, Dict, List, Iterable, Tuple

def parse_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all timing, size, metadata fields, and raw content text from a single HAR entry.
    """
    metrics: Dict[str, Any] = {}

    # Top-level metadata
    metrics['priority']           = entry.get('_priority')
    metrics['resourceType']       = entry.get('_resourceType')
    metrics['pageref']            = entry.get('pageref')
    metrics['connection_id']      = entry.get('connection')
    metrics['server_ip_address']  = entry.get('serverIPAddress')
    metrics['startedDateTime']    = entry.get('startedDateTime')
    metrics['time_total_ms']      = entry.get('time')

    # — Request block —
    req = entry.get('request', {})
    metrics['request_method']         = req.get('method')
    metrics['request_url']            = req.get('url')
    metrics['request_httpVersion']    = req.get('httpVersion')
    metrics['request_headers_count']  = len(req.get('headers', []))
    metrics['request_query_count']    = len(req.get('queryString', []))
    metrics['request_cookies_count']  = len(req.get('cookies', []))
    metrics['request_headers_size']   = req.get('headersSize')
    metrics['request_body_size']      = req.get('bodySize')

    # Post-data
    post = req.get('postData')
    if post:
        metrics['postData_mimeType']     = post.get('mimeType')
        text = post.get('text')
        if text is not None:
            metrics['postData_text_length'] = len(text)

    # — Response block —
    res = entry.get('response', {})
    metrics['response_status']         = res.get('status')
    metrics['response_httpVersion']    = res.get('httpVersion')
    metrics['response_headers_count']  = len(res.get('headers', []))
    metrics['response_cookies_count']  = len(res.get('cookies', []))
    metrics['response_headers_size']   = res.get('headersSize')
    metrics['response_body_size']      = res.get('bodySize')

    # Content sub-block
    content = res.get('content', {})
    metrics['content_size']            = content.get('size')
    metrics['content_mimeType']        = content.get('mimeType')
    metrics['content_text']            = content.get('text')  # raw response text
    # (_transferSize is the true on-the-wire bytes including headers)
    metrics['transfer_size']           = res.get('_transferSize')

    # — Cache info —
    cache = entry.get('cache', {})
    metrics['cache_beforeRequest']     = cache.get('beforeRequest')
    metrics['cache_afterRequest']      = cache.get('afterRequest')

    # — Detailed timings —
    timings = entry.get('timings', {})
    for phase, t in timings.items():
        metrics[f'time_{phase}_ms'] = t

    return metrics

def parse_sse_stream(content_text: str) -> List[Dict[str, Any]]:
    """
    Parse a Server-Sent Events (SSE) stream into a list of events.
    Each event is a dict with:
      - 'eventType': the SSE event type (e.g., 'delta', 'delta_encoding')
      - 'payload': the JSON-decoded data or raw string if not valid JSON
    """
    entries: List[Dict[str, Any]] = []
    last_event_type: str = None

    # Split on double newlines to separate SSE blocks
    for chunk in content_text.strip().split("\n\n"):
        lines = chunk.splitlines()
        event_type = None
        data_parts: List[str] = []

        for line in lines:
            if line.startswith("event:"):
                event_type = line[len("event:"):].strip()
            elif line.startswith("data:"):
                # collect the data payload lines
                data_parts.append(line[len("data:"):].strip())

        # If no event: line, reuse the last seen event type
        if event_type is not None:
            last_event_type = event_type
        event_type = event_type or last_event_type

        # Combine all data parts into one payload string
        data_str = "".join(data_parts)

        # Try to JSON-decode; fallback to raw string on failure
        try:
            payload: Any = json.loads(data_str)
        except json.JSONDecodeError:
            payload = data_str

        entries.append({
            "eventType": event_type,
            "payload": payload
        })

    return entries

def _iter_q_values(obj: Any) -> Iterable[str]:
    """Recursively yield all string values where the key == 'q'."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "q" and isinstance(v, str):
                yield html.unescape(v.strip())
            yield from _iter_q_values(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_q_values(item)

def _iter_strings(obj: Any) -> Iterable[str]:
    """Recursively yield all strings found anywhere."""
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _iter_strings(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_strings(item)
    elif isinstance(obj, str):
        yield obj

def extract_search_queries(parsed_events: List[Dict[str, Any]]) -> List[str]:
    """
    Collect all string values under key 'q' anywhere in the payloads.
    If none are found in a payload, fall back to extracting search("...") calls
    from text strings inside that payload.
    De-duplicates while preserving order.
    """
    queries: List[str] = []
    search_call_re = re.compile(r'search\(\s*["\'](.*?)["\']\s*\)', re.DOTALL)

    for ev in parsed_events:
        if ev.get("eventType") != "delta":
            continue
        payload = ev.get("payload")
        if not isinstance(payload, dict):
            continue

        found_qs = [q for q in _iter_q_values(payload) if q]

        if found_qs:
            queries.extend(found_qs)
        else:
            # Only if no q’s were found
            for s in _iter_strings(payload):
                for m in search_call_re.findall(s):
                    if m:
                        queries.append(html.unescape(m.strip()))

    # Deduplicate while preserving order
    seen = set()
    deduped: List[str] = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            deduped.append(q)

    return deduped

def extract_urls(parsed_events: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    """
    Given a list of SSE events as returned by parse_sse_stream(),
    returns two lists:
      - accessed URLs (during search phase)
      - given URLs (after the final finished_successfully separator)

    We start collecting accessed URLs as soon as we see search_result_groups,
    and we only start collecting given URLs *after the last* finished_successfully.
    """
    accessed: List[str] = []
    given: List[str] = []

    # --- pass 1: locate last separator index ---
    last_sep_idx = -1
    for i, ev in enumerate(parsed_events):
        if ev.get("eventType") != "delta":
            continue
        d = ev.get("payload")
        if isinstance(d, dict) and d.get("p") == "/message/status" \
           and d.get("o") == "replace" and d.get("v") == "finished_successfully":
            last_sep_idx = i

    # --- pass 2: collect URLs ---
    after_sep = False
    for i, ev in enumerate(parsed_events):
        if ev.get("eventType") != "delta":
            continue
        d = ev.get("payload")
        if not isinstance(d, dict):
            continue

        # flip into "after" mode only once at the last separator
        if d.get("p") == "/message/status" and d.get("o") == "replace" and d.get("v") == "finished_successfully":
            if i == last_sep_idx:
                after_sep = True
            continue

        if not after_sep:
            # Case A: list of search_result_group objects
            if isinstance(d.get("v"), list):
                for item in d["v"]:
                    if isinstance(item, dict) and item.get("type") == "search_result_group":
                        for entry in item.get("entries", []):
                            url = entry.get("url")
                            if url:
                                accessed.append(url)
            # Case B: explicit /search_result_groups/.../entries path
            if isinstance(d.get("p"), str) and "/search_result_groups" in d["p"] and d["p"].endswith("/entries"):
                for entry in d.get("v", []):
                    if isinstance(entry, dict):
                        url = entry.get("url")
                        if url:
                            accessed.append(url)

        else:  # after_sep
            if d.get("type") == "url_moderation":
                um = d.get("url_moderation_result", {})
                url = um.get("full_url")
                if url:
                    given.append(url)
        
        # dedupe preserving order
        all_urls = accessed + given
        normal_urls: List[str] = []
        cited_urls: List[str] = []
        for u in all_urls:
            if u in normal_urls or u in cited_urls:
                continue
            if 'utm_source=chatgpt.com' in u:
                cited_urls.append(u)
            else:
                normal_urls.append(u)

    return accessed, given, normal_urls, cited_urls

def process_har_files(har_list: List[str], target_url: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for har_path in har_list:
        try:
            with open(har_path, 'r', encoding='utf-8') as f:
                har = json.load(f)
            entries = har.get('entries') or har.get('log', {}).get('entries', [])
            matched = next((e for e in entries if e.get('request', {}).get('url') == target_url), None)
            if not matched:
                raise ValueError(f"No entry with URL '{target_url}' in {har_path}")

            # extract full metrics + SSE content
            metrics = parse_entry(matched)
            events = parse_sse_stream(metrics.get('content_text', ''))

            # search queries
            queries = extract_search_queries(events)
            # urls & counts
            accessed, given, normal_urls, cited_urls = extract_urls(events)

            results.append({
                'harname': har_path,
                'search_strings': queries,
                'url': normal_urls,
                'cited_url': cited_urls,
                'metrics': metrics,
                'n_accessed': len(accessed),
                'n_given': len(given),
            })
        except Exception as e:
            results.append({'harname': har_path, 'error': str(e)})
    return results

# =========================
# Claude Parser Functions
# =========================

from urllib.parse import urlparse
import json
import re
from typing import Any, Dict, List, Tuple
from pathlib import Path

CLAUDE_COMPLETION_RE = re.compile(
    r"^/api/organizations/[^/]+/chat_conversations/[^/]+/completion(?:$|\?)"
)

def find_and_parse_claude_completion(har_path: str) -> Dict[str, Any]:
    """
    Load a HAR file and find the first Claude completion entry
    (org ID + chat ID don't need to be known).
    Returns metrics dict including 'entry_index' and 'matched_url'.
    Raises ValueError if not found.
    """
    with open(har_path, 'r', encoding='utf-8') as f:
        har = json.load(f)

    entries = har.get('entries') or har.get('log', {}).get('entries', [])
    for idx, entry in enumerate(entries):
        req = entry.get('request', {})
        url = req.get('url') or ""
        try:
            p = urlparse(url)
        except Exception:
            continue
        if p.netloc == "claude.ai" and CLAUDE_COMPLETION_RE.match(p.path):
            metrics = parse_entry(entry)
            metrics['entry_index'] = idx
            metrics['matched_url'] = url
            return metrics

    raise ValueError("No Claude completion entry found in HAR.")


def extract_claude_queries(parsed_events: List[Dict[str, Any]]) -> List[str]:
    """
    Extract web search queries from Claude SSE events.
    Works whether the partial JSON decodes to a dict {"query": "..."}
    or a list of dicts [{"query": "..."}].
    """
    queries: List[str] = []
    buffer: List[str] = []
    in_search_block = False

    for ev in parsed_events:
        payload = ev.get("payload")
        if not isinstance(payload, dict):
            continue

        t = payload.get("type")

        if t == "content_block_start" and payload.get("content_block", {}).get("name") == "web_search":
            in_search_block = True
            buffer = []

        elif in_search_block and t == "content_block_delta":
            delta = payload.get("delta", {})
            if delta.get("type") == "input_json_delta":
                part = delta.get("partial_json")
                if isinstance(part, str):
                    buffer.append(part)

        elif in_search_block and t == "content_block_stop":
            joined = "".join(buffer).strip()
            if joined:
                try:
                    obj = json.loads(joined)
                    if isinstance(obj, dict):
                        q = obj.get("query")
                        if q:
                            queries.append(q.strip())
                    elif isinstance(obj, list):
                        for item in obj:
                            if isinstance(item, dict) and "query" in item:
                                q = item["query"]
                                if q:
                                    queries.append(q.strip())
                except json.JSONDecodeError:
                    pass
            in_search_block = False
            buffer = []

    # Deduplicate while preserving order
    seen = set()
    deduped: List[str] = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            deduped.append(q)

    return deduped


def count_urls_claude(parsed_events: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    """
    From Claude SSE events:
      - Accessed URLs: inside web_search tool_result blocks
      - Cited  URLs:   inside answer text blocks via citation_start_delta (and any prefilled citations)

    Returns (accessed_urls, cited_urls), both deduped with order preserved.
    """
    accessed: List[str] = []
    cited: List[str] = []

    # ---- helpers ----
    def dedupe_keep_order(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in items:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def extract_urls_from_partial_json(chunks: List[str]) -> List[str]:
        """Join partial_json fragments and pull any 'url' fields from list/dict payloads."""
        if not chunks:
            return []
        joined = "".join(chunks).strip()
        urls: List[str] = []
        try:
            obj = json.loads(joined)
            if isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict) and "url" in item and item["url"]:
                        urls.append(item["url"])
            elif isinstance(obj, dict):
                if "url" in obj and obj["url"]:
                    urls.append(obj["url"])
        except json.JSONDecodeError:
            pass
        return urls

    # ---- state for accessed (tool_result) blocks ----
    in_result_block = False
    result_buffer: List[str] = []

    # ---- state for answer (text) blocks ----
    in_text_block = False

    for ev in parsed_events:
        payload = ev.get("payload")
        if not isinstance(payload, dict):
            continue

        t = payload.get("type")

        # ====== START blocks ======
        if t == "content_block_start":
            cb = payload.get("content_block", {}) or {}
            cb_name = cb.get("name")
            cb_type = cb.get("type")

            # Accessed: web_search tool_result block
            if cb_name == "web_search" and cb_type == "tool_result":
                in_result_block = True
                result_buffer = []

            # Cited: text answer block; also consider any prefilled citations array
            if cb_type == "text":
                in_text_block = True
                # If Claude pre-populated 'citations' in the start block
                pre = cb.get("citations")
                if isinstance(pre, list):
                    for c in pre:
                        if isinstance(c, dict):
                            u = c.get("url")
                            if u:
                                cited.append(u)

        # ====== DELTA blocks ======
        elif t == "content_block_delta":
            delta = payload.get("delta", {}) or {}
            d_type = delta.get("type")

            # Accessed: accumulate JSON pieces
            if in_result_block and d_type == "input_json_delta":
                part = delta.get("partial_json")
                if isinstance(part, str):
                    result_buffer.append(part)

            # Cited: streaming citations during answer
            if in_text_block and d_type == "citation_start_delta":
                citation = delta.get("citation", {}) or {}
                url = citation.get("url")
                if url:
                    cited.append(url)
                # Some payloads also duplicate sources; include their URLs too
                sources = citation.get("sources")
                if isinstance(sources, list):
                    for s in sources:
                        if isinstance(s, dict):
                            su = s.get("url")
                            if su:
                                cited.append(su)

        # ====== STOP blocks ======
        elif t == "content_block_stop":
            # finalize accessed urls block
            if in_result_block:
                accessed.extend(extract_urls_from_partial_json(result_buffer))
                in_result_block = False
                result_buffer = []

            # finalize text block
            if in_text_block:
                in_text_block = False

        # (message_* events are ignored for URL extraction)

    # Deduplicate with order preserved
    # accessed = dedupe_keep_order(accessed)
    cited = dedupe_keep_order(cited)

    return accessed, cited


MARKER = "[no answer captured]"

def reconstruct_answer_from_sse(parsed_events):
    """Concatenate all text_delta chunks from text content blocks, in order."""
    buf = []
    in_text_block = False
    for ev in parsed_events:
        payload = ev.get("payload")
        if not isinstance(payload, dict):
            continue
        t = payload.get("type")

        if t == "content_block_start":
            cb = (payload.get("content_block") or {})
            if cb.get("type") == "text":
                in_text_block = True

        elif t == "content_block_delta" and in_text_block:
            delta = payload.get("delta") or {}
            if delta.get("type") == "text_delta":
                text = delta.get("text", "")
                if text:
                    buf.append(text)

        elif t == "content_block_stop" and in_text_block:
            in_text_block = False

    return "".join(buf).strip()

PROMPT_ID_RE = re.compile(r"network-logs-prompt-(\d+)\.har$", re.IGNORECASE)

def response_txt_path_for_har(har_dir: Path, category: str, model: str, dataset_run: str, har_filename: str) -> Path:
    """
    Map:
      ./<category>_{model}_{dataset_run}_1/<category>_hars_{model}_{dataset_run}_1/network-logs-prompt-<id>.har
    →  ./<category>_{model}_{dataset_run}_1/<category>_responses_{model}_{dataset_run}_1/response-prompt-<id>.txt
    """
    m = PROMPT_ID_RE.search(har_filename)
    if not m:
        return None
    prompt_id = m.group(1)
    base = har_dir.parent  # ./<category>_{model}_{dataset_run}_1
    resp_dir = base / f"{category}_responses_{model}_{dataset_run}_1"
    return resp_dir / f"response-prompt-{prompt_id}.txt"

def replace_marker_in_response_file(resp_path: Path, answer_text: str) -> bool:
    """
    Replace the single line '[no answer captured]' with answer_text.
    Returns True if replaced, False otherwise.
    """
    if not resp_path or not resp_path.exists():
        return False
    try:
        txt = resp_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        txt = resp_path.read_text(encoding="utf-8", errors="replace")

    # Replace exactly one occurrence of the marker line
    # Support optional surrounding whitespace/newlines around marker line.
    # We’ll do a conservative replace: the literal marker once.
    if MARKER in txt:
        new_txt = txt.replace(MARKER, answer_text, 1)
        resp_path.write_text(new_txt, encoding="utf-8")
        return True
    return False


# ======================================
# Main har_parser starting function
# ======================================

def har_parser(har_list: List[str]) -> List[Dict[str, Any]]:
    """
    Entry point: processes HAR files for ChatGPT or Claude.
    Returns a list of result dicts. Each dict contains extracted queries, URLs, and metrics.
    """
    results: List[Dict[str, Any]] = []
    for har_path in har_list:
        try:
            # First, try as ChatGPT HAR
            try:
                chatgpt_target = "https://chatgpt.com/backend-api/f/conversation"
                res = process_har_files([har_path], chatgpt_target)
                # process_har_files returns a list, so unwrap
                r = res[0]
                if not r.get("error"):
                    r["source"] = "chatgpt"
                    results.append(r)
                    continue  # done with this HAR
            except Exception:
                pass

            # If not ChatGPT, try Claude
            metrics = find_and_parse_claude_completion(har_path)
            events = parse_sse_stream(metrics.get("content_text", "") or "")
            queries = extract_claude_queries(events)
            accessed, cited = count_urls_claude(events)
            answer = reconstruct_answer_from_sse(events)

            results.append({
                "harname": har_path,
                "source": "claude",
                "search_strings": queries,
                "url": accessed,
                "cited_url": cited,
                "answer": answer,
                "metrics": metrics,
                "n_accessed": len(accessed),
                "n_cited": len(cited),
            })

        except Exception as e:
            results.append({"harname": har_path, "error": str(e)})

    # Summarize totals
    total_searches = sum(len(r.get("search_strings", [])) for r in results if not r.get("error"))
    total_urls = sum(len(r.get("url", [])) for r in results if not r.get("error"))
    for r in results:
        print(r.get('url'))
        print(r.get('search_strings'))
    print(f"Total search strings across all files: {total_searches}")
    print(f"Total URLs across all files: {total_urls}")

    return results