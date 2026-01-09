"""
YouTube Video Discovery Engine.

Discovers YouTube videos based on keywords defined in config/alliances.json.
Uses yt-dlp to find recent videos per political alliance category.
Returns video list programmatically for use by scraper.py.
"""

import json
import os
import datetime
import yt_dlp
from typing import List, Dict


CONFIG_PATH = os.path.join("config", "alliances.json")


def load_keywords():
    """
    Load keywords from alliances.json configuration file.
    
    Returns:
        List of tuples: (alliance_name, query_string)
    """
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    queries = []
    for alliance, query_list in data.items():
        for query in query_list:
            queries.append((alliance, query))
    
    return queries


def search_youtube_videos(query: str, max_results: int = 5) -> List[Dict]:
    """
    Search YouTube for videos matching the query using yt-dlp.
    
    Args:
        query: Search query string
        max_results: Maximum number of videos to return (default: 5)
    
    Returns:
        List of video dictionaries with id, title, channel, url
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
        print(f"Error searching for '{query}': {str(e)[:100]}")
    
    return videos


def discover_videos() -> List[Dict]:
    """
    Main discovery function that searches for videos based on alliance keywords.
    
    Searches YouTube for 3-5 recent videos per keyword category,
    deduplicates by video ID, and returns the video list.
    
    Returns:
        List of video dictionaries with keys: id, url, title, channel, 
        alliance, search_query, status
    """
    queries = load_keywords()
    video_list = []
    seen_ids = set()
    
    print(f"Starting Discovery for {datetime.date.today()}")
    print(f"Total queries to process: {len(queries)}\n")
    
    for alliance, query in queries:
        print(f"Searching [{alliance}]: {query}...")
        
        try:
            videos = search_youtube_videos(query, max_results=5)
            
            for video in videos:
                vid_id = video['id']
                
                if vid_id and vid_id not in seen_ids:
                    print(f"  Found: {video['title'][:60]}...")
                    video_list.append({
                        "id": vid_id,
                        "url": video['url'],
                        "title": video['title'],
                        "channel": video['channel'],
                        "alliance": alliance,
                        "search_query": query,
                        "status": "pending"
                    })
                    seen_ids.add(vid_id)
                elif vid_id in seen_ids:
                    print(f"  Duplicate: {video['title'][:60]}...")
        
        except Exception as e:
            print(f"  Error processing query '{query}': {str(e)[:100]}")
            continue
    
    print(f"\nDiscovery Complete!")
    print(f"Total unique videos found: {len(video_list)}")
    
    return video_list


if __name__ == "__main__":
    # When run standalone, can optionally save for debugging
    videos = discover_videos()
    print(f"\nDiscovered {len(videos)} videos")
