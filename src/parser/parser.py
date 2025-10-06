import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def parse_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all timing, size, metadata fields, and raw content text from a single HAR entry.
    """
    metrics: Dict[str, Any] = {}

    # Top-level metadata
    metrics['priority'] = entry.get('_priority')
    metrics['resourceType'] = entry.get('_resourceType')
    metrics['pageref'] = entry.get('pageref')
    metrics['connection_id'] = entry.get('connection')
    metrics['server_ip_address'] = entry.get('serverIPAddress')
    metrics['startedDateTime'] = entry.get('startedDateTime')
    metrics['time_total_ms'] = entry.get('time')

    # Request block
    req = entry.get('request', {})
    metrics['request_method'] = req.get('method')
    metrics['request_url'] = req.get('url')
    metrics['request_httpVersion'] = req.get('httpVersion')
    metrics['request_headers_count'] = len(req.get('headers', []))
    metrics['request_query_count'] = len(req.get('queryString', []))
    metrics['request_cookies_count'] = len(req.get('cookies', []))
    metrics['request_headers_size'] = req.get('headersSize')
    metrics['request_body_size'] = req.get('bodySize')

    # Post-data
    post = req.get('postData')
    if post:
        metrics['postData_mimeType'] = post.get('mimeType')
        text = post.get('text')
        if text is not None:
            metrics['postData_text_length'] = len(text)

    # Response block
    res = entry.get('response', {})
    metrics['response_status'] = res.get('status')
    metrics['response_httpVersion'] = res.get('httpVersion')
    metrics['response_headers_count'] = len(res.get('headers', []))
    metrics['response_cookies_count'] = len(res.get('cookies', []))
    metrics['response_headers_size'] = res.get('headersSize')
    metrics['response_body_size'] = res.get('bodySize')

    # Content sub-block
    content = res.get('content', {})
    metrics['content_size'] = content.get('size')
    metrics['content_mimeType'] = content.get('mimeType')
    metrics['content_text'] = content.get('text')
    metrics['transfer_size'] = res.get('_transferSize')

    # Cache info
    cache = entry.get('cache', {})
    metrics['cache_beforeRequest'] = cache.get('beforeRequest')
    metrics['cache_afterRequest'] = cache.get('afterRequest')

    # Detailed timings
    timings = entry.get('timings', {})
    for phase, t in timings.items():
        metrics[f'time_{phase}_ms'] = t

    return metrics


def parse_sse_stream(content_text: str) -> List[Dict[str, Any]]:
    """
    Parse a Server-Sent Events (SSE) stream into a list of events.
    Each event is a dict with:
      - 'eventType'
      - 'payload'
    """
    entries: List[Dict[str, Any]] = []
    last_event_type: Optional[str] = None

    for chunk in content_text.strip().split("\n\n"):
        lines = chunk.splitlines()
        event_type = None
        data_parts: List[str] = []

        for line in lines:
            if line.startswith("event:"):
                event_type = line.split("event:", 1)[1].strip()
            elif line.startswith("data:"):
                data_parts.append(line.split("data:", 1)[1].strip())

        if event_type is not None:
            last_event_type = event_type
        event_type = event_type or last_event_type

        data_str = "".join(data_parts)
        try:
            payload: Any = json.loads(data_str)
        except json.JSONDecodeError:
            payload = data_str

        entries.append({"eventType": event_type, "payload": payload})

    return entries


def extract_search_queries(parsed_events: List[Dict[str, Any]]) -> List[str]:
    """Extract 'search_queries' values from SSE deltas."""
    queries: List[str] = []
    for ev in parsed_events:
        if ev.get("eventType") != "delta":
            continue
        d = ev["payload"]
        if not isinstance(d, dict):
            continue
        if d.get("o") == "patch" and isinstance(d.get("v"), list):
            for op in d["v"]:
                if op.get("p") == "/message/metadata" and op.get("o") == "append":
                    meta = op.get("v")
                    if isinstance(meta, dict):
                        sqs = meta.get("search_queries")
                        if isinstance(sqs, list):
                            for sq in sqs:
                                q = sq.get("q")
                                if isinstance(q, str):
                                    queries.append(q)
    return queries


def count_urls(parsed_events: List[Dict[str, Any]]) -> (List[str], List[str]):
    """
    Count accessed vs given URLs in SSE events.
    Returns two lists: accessed, given
    """
    accessed: List[str] = []
    given: List[str] = []
    seen_sep = False
    sep_count = 0

    for ev in parsed_events:
        if ev.get("eventType") != "delta":
            continue
        d = ev["payload"]
        if not isinstance(d, dict):
            continue
        if d.get("p") == "/message/status" and d.get("o") == "replace" and d.get("v") == "finished_successfully":
            sep_count += 1
            if sep_count == 2:
                seen_sep = True
            continue
        if not seen_sep:
            if isinstance(d.get("v"), list):
                for item in d["v"]:
                    if isinstance(item, dict) and item.get("type") == "search_result_group":
                        for entry in item.get("entries", []):
                            url = entry.get("url")
                            if url:
                                accessed.append(url)
            if isinstance(d.get("p"), str) and "/search_result_groups" in d.get("p") and d.get("p").endswith("/entries"):
                for entry in d["v"]:
                    url = entry.get("url")
                    if url:
                        accessed.append(url)
        else:
            if d.get("type") == "url_moderation":
                um = d.get("url_moderation_result", {})
                url = um.get("full_url")
                if url:
                    given.append(url)

    return accessed, given


def process_har_files(har_list: List[str], target_url: str) -> List[Dict[str, Any]]:
    """
    Parse multiple HAR files and extract metrics for entries matching target_url.
    """
    results: List[Dict[str, Any]] = []
    for har_path in har_list:
        try:
            with open(har_path, 'r', encoding='utf-8') as f:
                har = json.load(f)
            entries = har.get('entries') or har.get('log', {}).get('entries', [])
            matched = next((e for e in entries if e.get('request', {}).get('url') == target_url), None)
            if not matched:
                raise ValueError(f"No entry with URL '{target_url}' in {har_path}")

            m = parse_entry(matched)
            parsed = parse_sse_stream(m.get('content_text', ''))
            queries = extract_search_queries(parsed)
            accessed, given = count_urls(parsed)

            results.append({
                'har_file': Path(har_path).name,
                'metrics': m,
                'queries': queries,
                'n_accessed': len(accessed),
                'n_given': len(given),
            })
        except Exception as e:
            results.append({'har_file': Path(har_path).name, 'error': str(e)})
    return results


def har_parser(har_list: List[str]) -> List[Dict[str, Any]]:
    """
    Entry point: processes HAR files for the chatgpt conversation endpoint.
    """
    target = "https://chatgpt.com/backend-api/f/conversation"
    return process_har_files(har_list, target)