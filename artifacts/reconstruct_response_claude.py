import json

def reconstruct_claude_response(har_file_path):
    try:
        with open(har_file_path, 'r', encoding='utf-8') as f:
            har_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {har_file_path}")
        return
    except json.JSONDecodeError:
        print("Error: Invalid JSON file.")
        return

    print(f"Scanning {har_file_path} for Claude streams...")

    # 1. Find the streaming request
    # Claude responses usually come from endpoints ending in /completion 
    # or have the mimeType 'text/event-stream'.
    stream_entry = None
    
    for entry in har_data['log']['entries']:
        response = entry.get('response', {})
        content = response.get('content', {})
        mime_type = content.get('mimeType', '')
        text = content.get('text', '')
        url = entry.get('request', {}).get('url', '')

        # Filter for likely candidates
        if 'text/event-stream' in mime_type and 'claude.ai' in url:
            # We want the one with actual data
            if len(text) > 100: 
                stream_entry = entry
                break
    
    if not stream_entry:
        print("Could not find a 'text/event-stream' response in the HAR file.")
        print("Tip: Ensure the HAR file records the actual generation of the message.")
        return

    print(f"Found stream in request to: {stream_entry['request']['url']}")

    # 2. Parse the Server-Sent Events (SSE)
    response_text = stream_entry['response']['content']['text']
    lines = response_text.split('\n')

    full_response = []
    
    for line in lines:
        line = line.strip()
        
        # Claude streams usually look like:
        # event: content_block_delta
        # data: {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}
        
        if line.startswith('data: '):
            data_str = line[6:] # Remove 'data: '
            
            try:
                data_json = json.loads(data_str)
            except json.JSONDecodeError:
                continue
            
            if not isinstance(data_json, dict):
                continue

            # --- Extract Text Logic ---
            
            # Format 1: Modern "Messages" API (content_block_delta)
            if 'delta' in data_json and 'text' in data_json['delta']:
                full_response.append(data_json['delta']['text'])
            
            # Format 2: Legacy "Text Completion" API (completion)
            elif 'completion' in data_json:
                full_response.append(data_json['completion'])

    # 3. Print Output
    print("\n" + "="*20 + " RECONSTRUCTED CLAUDE RESPONSE " + "="*20 + "\n")
    print("".join(full_response).strip())

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_har_file>")
        sys.exit(1)

    har_file = sys.argv[1]
    reconstruct_claude_response(har_file)
