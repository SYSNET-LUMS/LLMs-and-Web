import json
import re

def reconstruct_chatgpt_response(har_file_path):
    try:
        with open(har_file_path, 'r', encoding='utf-8') as f:
            har_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {har_file_path}")
        return
    except json.JSONDecodeError:
        print("Error: Invalid JSON file.")
        return

    # 1. Find the conversation POST request
    conversation_entry = None
    for entry in har_data['log']['entries']:
        response = entry.get('response', {})
        content = response.get('content', {})
        mime_type = content.get('mimeType', '')
        text = content.get('text', '')

        # We look for 'text/event-stream' which contains the chat generation
        if 'text/event-stream' in mime_type and text:
            conversation_entry = entry
            break
    
    if not conversation_entry:
        print("Could not find a conversation stream in the HAR file.")
        return

    print("Found conversation stream. parsing...")

    # 2. Parse the stream
    response_text = conversation_entry['response']['content']['text']
    lines = response_text.split('\n')

    final_response = []
    thoughts = []
    
    # State to track where the stream is currently writing
    state = {"current_path": None}

    def process_update(data):
        """Recursively process updates to handle both simple appends and batch patches."""
        # 1. Update Path if present
        if 'p' in data:
            state["current_path"] = data['p']

        # 2. Extract Value
        val = data.get('v')

        # 3. Handle Batch Updates (List of updates)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    process_update(item)
            return

        # 4. Handle Text Append (String value)
        if isinstance(val, str) and state["current_path"]:
            # Main Response Text
            if state["current_path"] == '/message/content/parts/0':
                final_response.append(val)
            
            # Thinking / Reasoning Process (supports multiple thought blocks)
            # Paths look like: /message/content/thoughts/0/content
            elif '/message/content/thoughts' in state["current_path"] and state["current_path"].endswith("/content"):
                thoughts.append(val)

    for line in lines:
        line = line.strip()
        if line.startswith('data: '):
            data_str = line[6:]
            if data_str == '[DONE]':
                break
            
            try:
                data_json = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            # Skip lines that aren't dictionaries (e.g., data: "v1")
            if not isinstance(data_json, dict):
                continue

            process_update(data_json)

    # 3. Print Results
    if thoughts:
        print("\n" + "="*20 + " REASONING / THOUGHTS " + "="*20 + "\n")
        print("".join(thoughts).strip())
    
    print("\n" + "="*20 + " FINAL RESPONSE " + "="*20 + "\n")
    print("".join(final_response).strip())

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python script.py <path_to_har_file>")
        sys.exit(1)

    har_file = sys.argv[1]
    reconstruct_chatgpt_response(har_file)


