"""
YouTube Comment Scraper Module

Scrapes comments from YouTube videos discovered by discover.py.
Uses yt-dlp for reliable comment extraction.
"""

import json
import os
import time
import yt_dlp


TODO_PATH = os.path.join("data", "todo_urls.json")
DATA_LAKE_PATH = os.path.join("data", "raw_comments.json")


def extract_comments_from_video(video_url, max_comments=50):
    """
    Extract comments from a YouTube video using yt-dlp.
    Returns list of comment dictionaries.
    """
    comments = []
    
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'skip_download': True,
            'writesubtitles': False,
            'writeautomaticsub': False,
            'getcomments': True,
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            
            # Extract comments from info dict
            comment_data = info.get('comments', [])
            
            for comment in comment_data[:max_comments]:
                comments.append({
                    'text': comment.get('text', ''),
                    'author': comment.get('author', 'Unknown'),
                    'likes': comment.get('like_count', 0),
                    'timestamp': comment.get('timestamp', ''),
                    'time_text': comment.get('time_text', '')
                })
        
    except Exception as e:
        print(f"      Error: {str(e)[:150]}")
    
    return comments


def scrape_comments():
    """Main scraping function that processes all videos from todo list."""
    if not os.path.exists(TODO_PATH):
        print(f"❌ No '{TODO_PATH}' found. Run discover.py first.")
        return
    
    with open(TODO_PATH, 'r', encoding='utf-8') as f:
        video_list = json.load(f)
    
    if not video_list:
        print("❌ No videos found in todo list.")
        return
    
    all_comments = []
    
    print(f"{'='*60}")
    print(f"--- Starting Comment Scraper ---")
    print(f"Total videos to process: {len(video_list)}")
    print(f"{'='*60}\n")
    
    for i, video in enumerate(video_list, 1):
        video_url = video.get('url', '')
        video_id = video.get('id', '')
        video_title = video.get('title', 'Unknown')
        
        if not video_url:
            print(f"[{i}/{len(video_list)}] ⚠ Skipping: No URL found")
            continue
        
        print(f"[{i}/{len(video_list)}] Processing: {video_title[:50]}...")
        print(f"   Video ID: {video_id}")
        
        try:
            comments = extract_comments_from_video(video_url, max_comments=50)
            
            for comment in comments:
                record = {
                    "video_id": video_id,
                    "video_title": video_title,
                    "video_url": video_url,
                    "alliance": video.get('alliance', 'Unknown'),
                    "search_query": video.get('search_query', ''),
                    "comment_text": comment.get('text', ''),
                    "author": comment.get('author', 'Unknown'),
                    "likes": comment.get('likes', 0),
                    "timestamp": comment.get('timestamp', ''),
                    "time_text": comment.get('time_text', '')
                }
                all_comments.append(record)
            
            print(f"   ✓ Extracted {len(comments)} comments")
            
        except Exception as e:
            print(f"   ✗ Error: {str(e)[:100]}")
        
        # Rate limiting - be respectful to YouTube servers
        if i < len(video_list):
            time.sleep(2)
    
    # Save to data lake
    os.makedirs("data", exist_ok=True)
    with open(DATA_LAKE_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_comments, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"Scraping Complete!")
    print(f"Total comments collected: {len(all_comments)}")
    print(f"Saved to: {DATA_LAKE_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    scrape_comments()
