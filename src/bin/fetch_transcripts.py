from youtube_transcript_api import YouTubeTranscriptApi
import yt_dlp
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve environment variables
PLAYLIST_ID = os.getenv('YOUTUBE_PLAYLIST_ID')
OUTPUT_DIR = 'out'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'{PLAYLIST_ID}.json')

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_playlist_videos(playlist_id):
    # Configure yt-dlp
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'force_generic_extractor': False
    }
    
    videos = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            # Extract playlist information
            playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
            playlist_info = ydl.extract_info(playlist_url, download=False)
            
            # Process each video in the playlist
            for entry in playlist_info['entries']:
                if entry:
                    videos.append({
                        'contentDetails': {'videoId': entry.get('id')},
                        'snippet': {'title': entry.get('title', '(Title unavailable)')}
                    })
                    print(f"Found video: {entry.get('title', '(Title unavailable)')} ({entry.get('id')})")
        
        except Exception as e:
            print(f"Error fetching playlist: {e}")
    
    return videos

def fetch_transcript(video_id):
    try:
        # Fetch the transcript
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        return transcript
    except Exception as e:
        print(f"Could not retrieve transcript for video {video_id}: {e}")
        return None

def main():
    videos = fetch_playlist_videos(PLAYLIST_ID)
    transcripts_map = {}

    # Process each video in the playlist
    for video in videos:
        video_id = video['contentDetails']['videoId']
        title = video['snippet']['title']
        print(f"Fetching transcript for video: {video_id} - {title}")
        
        transcript = fetch_transcript(video_id)
        
        # Add transcript to the map if available, otherwise store an empty list
        transcripts_map[video_id] = {
            'videoId': video_id,
            'transcript': transcript if transcript else []
        }

    # Prepare the playlist details in the same format as the original JS script
    playlist_details_with_transcripts = {
        'playlistId': PLAYLIST_ID,
        'playlistItems': videos,
        'transcripts': transcripts_map
    }

    # Save the results to a JSON file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(playlist_details_with_transcripts, f, ensure_ascii=False, indent=2)
    print(f"Transcripts saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
