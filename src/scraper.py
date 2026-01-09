"""
YouTube Comment Scraper - Producer Component.

This module implements the Producer pattern in the decoupled ETL architecture.
It scrapes comments using yt-dlp and immediately pushes the data to Supabase
via the DataSystem class.

The Producer does NOT wait for processing - it simply:
1. Scrapes comments from YouTube
2. Uploads raw JSON to Supabase Storage (Data Lake)
3. Creates a PENDING job in job_queue table
4. Moves to the next video

The Consumer (processor.py) will independently poll the queue and process jobs.
"""

import json
import os
import time
from typing import List, Dict, Optional
from datetime import datetime
import yt_dlp

from infra.data_manager import DataSystem
from discover import discover_videos


MAX_COMMENTS_PER_VIDEO = 50


def scrape_comments_from_video(video_url: str, max_comments: int = 50) -> List[Dict]:
    """
    Extract comments from a YouTube video using yt-dlp.
    
    Args:
        video_url: YouTube video URL
        max_comments: Maximum number of comments to extract
    
    Returns:
        List of comment dictionaries
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
        print(f"Error extracting comments: {str(e)[:150]}")
    
    return comments


def scrape_comments(video_list: Optional[List[Dict]] = None):
    """
    Main scraping function - Producer component.
    
    Scrapes comments for each video and immediately pushes data to Supabase
    via DataSystem. Does not wait for downstream processing.
    
    Args:
        video_list: Optional list of video dictionaries. If None, will call
                   discover_videos() to get videos programmatically.
    """
    # Get video list - either from parameter or discover programmatically
    if video_list is None:
        print("No video list provided, running discovery...")
        video_list = discover_videos()
    
    if not video_list:
        print("No videos found to process.")
        return
    
    # Initialize DataSystem for Supabase operations
    try:
        data_system = DataSystem(bucket_name='raw_data')
    except RuntimeError as e:
        print(f"Error initializing DataSystem: {e}")
        print("Cannot proceed without Supabase connection.")
        return
    
    print("=" * 60)
    print("Starting Comment Scraper (Producer)")
    print(f"Total videos to process: {len(video_list)}")
    print("=" * 60)
    print()
    
    for i, video in enumerate(video_list, 1):
        video_url = video.get('url', '')
        video_id = video.get('id', '')
        video_title = video.get('title', 'Unknown')
        
        if not video_url:
            print(f"[{i}/{len(video_list)}] Skipping: No URL found")
            continue
        
        print(f"[{i}/{len(video_list)}] Processing: {video_title[:50]}...")
        print(f"  Video ID: {video_id}")
        
        try:
            # Scrape comments
            comments = scrape_comments_from_video(video_url, MAX_COMMENTS_PER_VIDEO)
            
            if not comments:
                print(f"  No comments extracted")
                continue
            
            # Structure the data
            structured_data = {
                "video_id": video_id,
                "video_title": video_title,
                "video_url": video_url,
                "alliance": video.get('alliance', 'Unknown'),
                "search_query": video.get('search_query', ''),
                "channel": video.get('channel', 'Unknown'),
                "scraped_at": datetime.now().isoformat(),
                "comments": comments
            }
            
            # Prepare metadata for job queue
            video_metadata = {
                "video_id": video_id,
                "video_title": video_title,
                "video_url": video_url,
                "alliance": video.get('alliance', 'Unknown'),
                "search_query": video.get('search_query', ''),
                "channel": video.get('channel', 'Unknown'),
                "comment_count": len(comments)
            }
            
            # Save to Supabase via DataSystem (Producer pattern)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"comments/{video_id}_{timestamp}.json"
            
            job_id = data_system.save_raw_json(
                data=structured_data,
                filename=filename,
                video_metadata=video_metadata
            )
            
            if job_id:
                print(f"  Extracted {len(comments)} comments")
                print(f"  Job {job_id} created in queue")
            else:
                print(f"  Extracted {len(comments)} comments (failed to save)")
        
        except Exception as e:
            print(f"  Error processing video: {str(e)[:100]}")
            continue
        
        # Rate limiting
        if i < len(video_list):
            time.sleep(2)
    
    print()
    print("=" * 60)
    print("Scraping Complete!")
    print("=" * 60)


if __name__ == "__main__":
    # When run standalone, automatically discovers videos
    scrape_comments()
