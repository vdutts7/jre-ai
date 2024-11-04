import json
import os
import math

def split_json_file(input_file, chunk_size_mb=100):
    # Create output directory
    output_dir = os.path.join('output', 'chunks')
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the original JSON file
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Get playlist items and transcripts
    playlist_items = data['playlistItems']
    transcripts = data['transcripts']
    
    # Calculate number of items per chunk
    total_items = len(playlist_items)
    chunk_count = math.ceil(os.path.getsize(input_file) / (chunk_size_mb * 1024 * 1024))
    items_per_chunk = math.ceil(total_items / chunk_count)
    
    # Split into chunks
    for i in range(0, total_items, items_per_chunk):
        chunk_items = playlist_items[i:i + items_per_chunk]
        
        # Create chunk data with same structure
        chunk_data = {
            'playlistId': data['playlistId'],
            'playlistItems': chunk_items,
            'transcripts': {
                video['contentDetails']['videoId']: transcripts[video['contentDetails']['videoId']]
                for video in chunk_items
                if video['contentDetails']['videoId'] in transcripts
            }
        }
        
        # Write chunk to file in the new directory
        filename = f"chunk_{i//items_per_chunk}.json"
        output_file = os.path.join(output_dir, filename)
        with open(output_file, 'w') as f:
            json.dump(chunk_data, f, indent=2)
        print(f"Created chunk: {output_file}")


# Usage
split_json_file('out/PLk1Sqn_f33KuWf3tW9BBe_4TP7x8l0m3T.json', chunk_size_mb=100)