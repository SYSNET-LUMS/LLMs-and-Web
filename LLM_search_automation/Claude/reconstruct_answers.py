import re
from pathlib import Path

MARKER = "[no answer captured]"

model = "opus-4.1"
dataset_run = "1"

har_dirs = {
    "navigational": Path(f"./navigational_{model}_{dataset_run}/navigational_hars_{model}_{dataset_run}"),
    "abstain": Path(f"./abstain_{model}_{dataset_run}/abstain_hars_{model}_{dataset_run}"),
    "factual": Path(f"./factual_{model}_{dataset_run}/factual_hars_{model}_{dataset_run}"),
    "instrumental": Path(f"./instrumental_{model}_{dataset_run}/instrumental_hars_{model}_{dataset_run}"),
    "transactional": Path(f"./transactional_{model}_{dataset_run}/transactional_hars_{model}_{dataset_run}"),
}

import json
from typing import Any, Dict, Optional, Any
import re


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
        metrics[f'time_{phase}_ms']     = t

    return metrics

from urllib.parse import urlparse

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

import json
from typing import Any, List, Dict

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
      ./<category>_{model}_{dataset_run}/<category>_hars_{model}_{dataset_run}/network-logs-prompt-<id>.har
    →  ./<category>_{model}_{dataset_run}/<category>_responses_{model}_{dataset_run}/response-prompt-<id>.txt
    """
    m = PROMPT_ID_RE.search(har_filename)
    if not m:
        return None
    prompt_id = m.group(1)
    base = har_dir.parent  # ./<category>_{model}_{dataset_run}
    resp_dir = base / f"{category}_responses_{model}_{dataset_run}"
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

pattern = "network-logs-prompt-*"

for category, har_dir in har_dirs.items():
    for har_path in sorted(har_dir.glob(pattern)):
            har_filename = har_path.name
            # 1) parse the main entry
            m = find_and_parse_claude_completion(str(har_path))

            # 2) parse SSE and extract queries + URL counts
            content_text = m.get("content_text")
            if not content_text:
                print(f"[NO SSE DATA] category={category}, file={har_filename}")
                continue

            parsed_events = parse_sse_stream(content_text)

            # 3) reconstruct answer text and patch response file
            answer_text = reconstruct_answer_from_sse(parsed_events)
            resp_path = response_txt_path_for_har(har_dir, category, model, dataset_run, har_filename)
            if answer_text and resp_path:
                replaced = replace_marker_in_response_file(resp_path, answer_text)
                if not replaced:
                    print(f"[MARKER NOT FOUND] category={category}, file={har_filename}, resp_file={resp_path}")
            else:
                print(f"[NO ANSWER OR RESP PATH] category={category}, file={har_filename}, resp_file={resp_path}")