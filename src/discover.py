"""
YouTube Video Discovery Module

Discovers YouTube videos based on keywords defined in alliances.json.
Uses yt-dlp for reliable YouTube search and video discovery.
"""

import json
import os
import datetime
import yt_dlp


CONFIG_PATH = os.path.join("config", "alliances.json")
OUTPUT_PATH = os.path.join("data", "todo_urls.json")


def load_keywords():
    """Load keywords from alliances.json and return flattened list with alliance context."""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Return list of tuples: (alliance_name, query)
    queries = []
    for alliance, query_list in data['keywords'].items():
        for query in query_list:
            queries.append((alliance, query))
    return queries


def search_youtube_videos(query, max_results=3):
    """
    Search YouTube for videos using yt-dlp.
    Returns list of video dictionaries with id, title, channel, url.
    """
    videos = []
    
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'default_search': 'ytsearch',
            'max_downloads': max_results,
        }
        
        search_query = f"ytsearch{max_results}:{query}"
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Extract info without downloading
            info = ydl.extract_info(search_query, download=False)
            
            if 'entries' in info:
                for entry in info['entries']:
                    if entry:
                        videos.append({
                            'id': entry.get('id', ''),
                            'title': entry.get('title', 'Unknown'),
                            'channel': entry.get('channel', 'Unknown'),
                            'url': entry.get('url', f"https://www.youtube.com/watch?v={entry.get('id', '')}")
                        })
        
    except Exception as e:
        print(f"   -> Error searching for '{query}': {str(e)[:100]}")
    
    return videos


def discover_videos():
    """Main discovery function that searches for videos and saves to todo list."""
    queries = load_keywords()
    video_list = []
    seen_ids = set()
    
    print(f"--- Starting Discovery for {datetime.date.today()} ---")
    print(f"Total queries to process: {len(queries)}\n")
    
    for alliance, query in queries:
        print(f"Searching [{alliance}]: {query}...")
        
        videos = search_youtube_videos(query, max_results=3)
        
        for video in videos:
            vid_id = video['id']
            
            # Deduplication
            if vid_id not in seen_ids:
                print(f"  ✓ Found: {video['title'][:60]}...")
                video_list.append({
                    "id": vid_id,
                    "url": video['url'],
                    "title": video['title'],
                    "channel": video['channel'],
                    "alliance": alliance,  # Track which alliance this belongs to
                    "search_query": query,
                    "status": "pending"
                })
                seen_ids.add(vid_id)
            else:
                print(f"  ⊗ Duplicate: {video['title'][:60]}...")
    
    # Save to todo file
    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(video_list, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"Discovery Complete!")
    print(f"Total unique videos found: {len(video_list)}")
    print(f"Saved to: {OUTPUT_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    discover_videos()
