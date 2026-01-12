"""
Tamil News Portal Scraper - High Confidence Location Data Source.

This module scrapes district-specific news from Daily Thanthi to provide
100% accurate location sentiment data. Unlike YouTube scraping where location
is inferred, news portals have explicit district information in URLs.

The scraper:
1. Fetches headlines from district-specific news pages
2. Maps headlines to the 'comments' field for existing NLP pipeline compatibility
3. Includes location_override field for guaranteed location accuracy
4. Uploads data to Supabase Storage via DataSystem
"""

import json
import os
import time
from typing import List, Dict, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup

from infra.data_manager import DataSystem


# Daily Thanthi district URLs configuration
# All 38 districts of Tamil Nadu
DISTRICT_URLS = {
    # Northern Districts
    "Chennai": "https://www.dailythanthi.com/Districts/Chennai",
    "Chengalpattu": "https://www.dailythanthi.com/Districts/Chengalpattu",
    "Kancheepuram": "https://www.dailythanthi.com/Districts/Kancheepuram",
    "Thiruvallur": "https://www.dailythanthi.com/Districts/Thiruvallur",
    "Vellore": "https://www.dailythanthi.com/Districts/Vellore",
    "Ranipet": "https://www.dailythanthi.com/Districts/Ranipet",
    "Tirupathur": "https://www.dailythanthi.com/Districts/Tirupattur",
    "Tiruvannamalai": "https://www.dailythanthi.com/Districts/Tiruvannamalai",
    "Villupuram": "https://www.dailythanthi.com/Districts/Villupuram",
    "Kallakurichi": "https://www.dailythanthi.com/Districts/Kallakurichi",
    
    # Western Districts
    "Salem": "https://www.dailythanthi.com/Districts/Salem",
    "Namakkal": "https://www.dailythanthi.com/Districts/Namakkal",
    "Erode": "https://www.dailythanthi.com/Districts/Erode",
    "Tiruppur": "https://www.dailythanthi.com/Districts/Tirupur",
    "Coimbatore": "https://www.dailythanthi.com/Districts/Coimbatore",
    "Nilgiris": "https://www.dailythanthi.com/Districts/Nilgiris",
    "Dharmapuri": "https://www.dailythanthi.com/Districts/Dharmapuri",
    "Krishnagiri": "https://www.dailythanthi.com/Districts/Krishnagiri",
    
    # Central Districts
    "Tiruchirappalli": "https://www.dailythanthi.com/Districts/Trichy",
    "Perambalur": "https://www.dailythanthi.com/Districts/Perambalur",
    "Ariyalur": "https://www.dailythanthi.com/Districts/Ariyalur",
    "Cuddalore": "https://www.dailythanthi.com/Districts/Cuddalore",
    "Nagapattinam": "https://www.dailythanthi.com/Districts/Nagapattinam",
    "Mayiladuthurai": "https://www.dailythanthi.com/Districts/Mayiladuthurai",
    "Tiruvarur": "https://www.dailythanthi.com/Districts/Tiruvarur",
    "Thanjavur": "https://www.dailythanthi.com/Districts/Thanjavur",
    "Pudukkottai": "https://www.dailythanthi.com/Districts/Pudukkottai",
    "Karur": "https://www.dailythanthi.com/Districts/Karur",
    
    # Southern Districts
    "Madurai": "https://www.dailythanthi.com/Districts/Madurai",
    "Theni": "https://www.dailythanthi.com/Districts/Theni",
    "Dindigul": "https://www.dailythanthi.com/Districts/Dindigul",
    "Sivaganga": "https://www.dailythanthi.com/Districts/Sivaganga",
    "Ramanathapuram": "https://www.dailythanthi.com/Districts/Ramanathapuram",
    "Virudhunagar": "https://www.dailythanthi.com/Districts/Virudhunagar",
    "Thoothukudi": "https://www.dailythanthi.com/Districts/Thoothukudi",
    "Tirunelveli": "https://www.dailythanthi.com/Districts/Tirunelveli",
    "Tenkasi": "https://www.dailythanthi.com/Districts/Tenkasi",
    "Kanyakumari": "https://www.dailythanthi.com/Districts/Kanyakumari",
}

# User-Agent header to avoid blocking
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Maximum headlines to scrape per district
MAX_HEADLINES_PER_DISTRICT = int(os.getenv('MAX_HEADLINES_PER_DISTRICT', '20'))


def fetch_page(url: str, timeout: int = 10) -> Optional[str]:
    """
    Fetch HTML content from a URL.
    
    Args:
        url: URL to fetch
        timeout: Request timeout in seconds
    
    Returns:
        HTML content as string, or None if error
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'  # Ensure proper encoding for Tamil text
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching {url}: {str(e)[:100]}")
        return None


def extract_headlines(html: str, district: str) -> List[str]:
    """
    Extract headlines from Daily Thanthi HTML page.
    
    This function tries multiple CSS selectors to find headlines, as website
    structure may vary. Common selectors for news sites:
    - h2, h3 tags (common headline tags)
    - div.ListingNews_content (specific to Daily Thanthi structure)
    - article tags
    - div.news-item or similar
    
    Args:
        html: HTML content of the page
        district: District name (for logging)
    
    Returns:
        List of headline text strings
    """
    headlines = []
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Strategy 1: Try specific Daily Thanthi selectors
        # Common patterns for news listing pages
        selectors = [
            'div.ListingNews_content h3 a',  # Headlines in listing div
            'div.ListingNews_content h2 a',
            'div.news-list h3 a',
            'article h3 a',
            'div.news-item h3 a',
            'h3 a',  # Fallback: any h3 with link
            'h2 a',  # Fallback: any h2 with link
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                print(f"  Found {len(elements)} headlines using selector: {selector}")
                for elem in elements[:MAX_HEADLINES_PER_DISTRICT]:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 10:  # Filter out very short text
                        headlines.append(text)
                break  # Use first selector that finds elements
        
        # Strategy 2: If no headlines found, try extracting from common news structures
        if not headlines:
            # Look for any divs with class containing "news", "article", "headline"
            news_divs = soup.find_all(['div', 'article'], class_=lambda x: x and any(
                keyword in x.lower() for keyword in ['news', 'article', 'headline', 'story']
            ))
            
            for div in news_divs[:MAX_HEADLINES_PER_DISTRICT]:
                # Try to find headline text within the div
                headline_elem = div.find(['h2', 'h3', 'h4', 'a'])
                if headline_elem:
                    text = headline_elem.get_text(strip=True)
                    if text and len(text) > 10:
                        headlines.append(text)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_headlines = []
        for headline in headlines:
            headline_lower = headline.lower()
            if headline_lower not in seen:
                seen.add(headline_lower)
                unique_headlines.append(headline)
        
        return unique_headlines[:MAX_HEADLINES_PER_DISTRICT]
    
    except Exception as e:
        print(f"  Error parsing HTML for {district}: {str(e)[:100]}")
        return []


def scrape_district_news(district: str, url: str) -> Optional[Dict]:
    """
    Scrape news headlines for a specific district.
    
    Args:
        district: District name
        url: URL of the district news page
    
    Returns:
        Dictionary with scraped data, or None if error
    """
    print(f"Scraping {district}...")
    print(f"  URL: {url}")
    
    # Fetch page
    html = fetch_page(url)
    if not html:
        print(f"  Failed to fetch page")
        return None
    
    # Extract headlines
    headlines = extract_headlines(html, district)
    
    if not headlines:
        print(f"  No headlines found")
        return None
    
    print(f"  Extracted {len(headlines)} headlines")
    
    # Structure data payload with Weighted Hybrid model
    # News headlines are authoritative_content (weight 3.0) vs user_comments (weight 1.0)
    structured_data = {
        "meta": {
            "source": "DailyThanthi",
            "type": "news_headline",
            "url": url,
            "district": district,
            "scraped_at": datetime.now().isoformat(),
            "headline_count": len(headlines)
        },
        "location_override": district,  # 100% confidence location from URL (ground truth)
        "authoritative_content": headlines,  # High weight (3.0) - authoritative signal
        "user_comments": []  # Empty for news sources
    }
    
    return structured_data


def scrape_news_portals(district_urls: Optional[Dict[str, str]] = None):
    """
    Main scraping function for news portals.
    
    Scrapes headlines from each district page and uploads to Supabase
    via DataSystem. Follows the same Producer pattern as scraper.py.
    
    Args:
        district_urls: Optional dictionary mapping district names to URLs.
                      If None, uses default DISTRICT_URLS.
    """
    if district_urls is None:
        district_urls = DISTRICT_URLS
    
    if not district_urls:
        print("No district URLs provided")
        return
    
    # Initialize DataSystem for Supabase operations
    try:
        data_system = DataSystem(bucket_name='raw_data')
    except RuntimeError as e:
        print(f"Error initializing DataSystem: {e}")
        print("Cannot proceed without Supabase connection.")
        return
    
    print("=" * 60)
    print("Starting News Portal Scraper (High Confidence Location)")
    print(f"Total districts to process: {len(district_urls)}")
    print("=" * 60)
    print()
    
    for i, (district, url) in enumerate(district_urls.items(), 1):
        print(f"[{i}/{len(district_urls)}] Processing: {district}")
        
        try:
            # Scrape district news
            data = scrape_district_news(district, url)
            
            if not data:
                print(f"  Skipping {district} (no data extracted)")
                continue
            
            # Prepare metadata for job queue
            news_metadata = {
                "source": "DailyThanthi",
                "type": "news_headline",
                "district": district,
                "url": url,
                "headline_count": len(data.get('authoritative_content', [])),
                "location_override": district
            }
            
            # Save to Supabase via DataSystem (Producer pattern)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"news/{district.lower()}_{timestamp}.json"
            
            job_id = data_system.save_raw_json(
                data=data,
                filename=filename,
                video_metadata=news_metadata
            )
            
            if job_id:
                print(f"  Uploaded {len(data.get('authoritative_content', []))} headlines")
                print(f"  Job {job_id} created in queue")
            else:
                print(f"  Failed to upload data")
        
        except Exception as e:
            print(f"  Error processing {district}: {str(e)[:100]}")
            continue
        
        # Rate limiting between districts
        if i < len(district_urls):
            time.sleep(2)
    
    print()
    print("=" * 60)
    print("News Scraping Complete!")
    print("=" * 60)


if __name__ == "__main__":
    # When run standalone, scrapes all configured districts
    scrape_news_portals()

