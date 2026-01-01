#!/usr/bin/env python3
"""
YouTube Video Data Fetcher
Fetches video metadata (title, description, thumbnail, publish date) from YouTube Data API v3
"""

import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Configuration
DEFAULT_CSV = 'recipes.csv'
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
YOUTUBE_API_ENDPOINT = 'https://www.googleapis.com/youtube/v3/videos'


class YouTubeAPIError(Exception):
    """Custom exception for YouTube API errors"""
    pass


class YouTubeFetcher:
    """Fetches video metadata from YouTube Data API v3"""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("YouTube API key not found. Please set YOUTUBE_API_KEY in .env file")
        self.api_key = api_key
        self.session = requests.Session()

    def extract_video_id(self, url: str) -> Optional[str]:
        """
        Extract video ID from YouTube URL.

        Supports formats:
        - https://www.youtube.com/watch?v=VIDEO_ID
        - https://youtu.be/VIDEO_ID
        - https://www.youtube.com/embed/VIDEO_ID
        """
        if not url:
            return None

        # Parse URL
        parsed = urlparse(url)

        # Standard watch URL
        if parsed.hostname in ['www.youtube.com', 'youtube.com']:
            if parsed.path == '/watch':
                query = parse_qs(parsed.query)
                return query.get('v', [None])[0]
            # Embed URL
            elif parsed.path.startswith('/embed/'):
                return parsed.path.split('/')[2]

        # Short URL
        elif parsed.hostname == 'youtu.be':
            return parsed.path.lstrip('/')

        return None

    def fetch_video_data(self, video_id: str) -> Optional[Dict]:
        """
        Fetch video metadata from YouTube API.

        Returns dict with:
        - title: Video title
        - description: Video description
        - thumbnail_url: High-quality thumbnail URL
        - published_at: ISO 8601 publish date
        """
        try:
            params = {
                'part': 'snippet',
                'id': video_id,
                'key': self.api_key
            }

            response = self.session.get(YOUTUBE_API_ENDPOINT, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Check for API errors
            if 'error' in data:
                error = data['error']
                raise YouTubeAPIError(f"API Error: {error.get('message', 'Unknown error')}")

            # Check if video exists
            if not data.get('items'):
                logging.warning(f"Video not found: {video_id}")
                return None

            snippet = data['items'][0]['snippet']

            # Get highest quality thumbnail
            thumbnails = snippet['thumbnails']
            thumbnail_url = (
                thumbnails.get('maxres', {}).get('url') or
                thumbnails.get('high', {}).get('url') or
                thumbnails.get('medium', {}).get('url') or
                thumbnails.get('default', {}).get('url')
            )

            return {
                'title': snippet['title'],
                'description': snippet['description'],
                'thumbnail_url': thumbnail_url,
                'published_at': snippet['publishedAt']
            }

        except requests.RequestException as e:
            logging.error(f"Network error fetching video {video_id}: {e}")
            return None
        except YouTubeAPIError as e:
            logging.error(f"YouTube API error: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching video {video_id}: {e}")
            return None


class CSVHandler:
    """Handles CSV reading/writing with UTF-8 BOM"""

    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)

    def backup(self) -> str:
        """Create backup of CSV file"""
        backup_path = self.csv_path.with_suffix('.csv.backup3')
        shutil.copy(str(self.csv_path), str(backup_path))
        return str(backup_path)

    def read_rows(self) -> List[Dict]:
        """Read CSV rows as dictionaries"""
        rows = []
        with open(self.csv_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                rows.append(row)
        return rows

    def write_rows(self, rows: List[Dict]):
        """Write rows back to CSV with UTF-8 BOM"""
        if not rows:
            return

        # Write to temp file first
        temp_path = self.csv_path.with_suffix('.tmp')

        fieldnames = list(rows[0].keys())
        with open(temp_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(rows)

        # Atomic rename
        shutil.move(str(temp_path), str(self.csv_path))

    def verify_integrity(self, original_count: int):
        """Verify row count matches"""
        rows = self.read_rows()
        if len(rows) != original_count:
            raise ValueError(f"Row count mismatch! Original: {original_count}, Current: {len(rows)}")


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('youtube_fetch.log')
        ]
    )


def format_date(iso_date: str) -> str:
    """Convert ISO 8601 date to DD.MM.YYYY format"""
    try:
        dt = datetime.fromisoformat(iso_date.replace('Z', '+00:00'))
        return dt.strftime('%d.%m.%Y')
    except:
        return iso_date


def main():
    parser = argparse.ArgumentParser(description='YouTube Video Data Fetcher')
    parser.add_argument('--csv', default=DEFAULT_CSV, help='Path to CSV file')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without updating')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    parser.add_argument('--no-backup', action='store_true', help='Skip CSV backup')

    args = parser.parse_args()

    # Setup
    setup_logging(args.verbose)

    print("YouTube Video Data Fetcher")
    print("=" * 30)
    print(f"CSV: {args.csv}")
    print()

    # Check API key
    if not YOUTUBE_API_KEY:
        print("ERROR: YOUTUBE_API_KEY not found in .env file")
        print("\nPlease create a .env file with:")
        print("YOUTUBE_API_KEY=your_api_key_here")
        sys.exit(1)

    # Initialize
    csv_handler = CSVHandler(args.csv)
    youtube = YouTubeFetcher(YOUTUBE_API_KEY)

    # Read CSV
    try:
        rows = csv_handler.read_rows()
        original_count = len(rows)
        print(f"Loaded {original_count} rows from CSV")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    # Find YouTube rows
    youtube_rows = []
    for i, row in enumerate(rows, 1):
        platform = row.get('Platform', '').strip().lower()
        link = row.get('Link', '').strip()

        if platform == 'youtube' or 'youtube.com' in link or 'youtu.be' in link:
            youtube_rows.append((i, row))

    print(f"Found {len(youtube_rows)} YouTube videos")
    print()

    if not youtube_rows:
        print("No YouTube videos found in CSV")
        return

    # Backup CSV
    if not args.no_backup and not args.dry_run:
        backup_path = csv_handler.backup()
        print(f"Creating backup: {backup_path} ✓")
        print()

    # Dry run preview
    if args.dry_run:
        print("[DRY RUN] No files will be modified\n")
        for i, row in youtube_rows[:5]:
            link = row.get('Link', '')
            video_id = youtube.extract_video_id(link)
            print(f"  {i}. {row.get('Başlık', 'Unknown')}")
            print(f"     Link: {link}")
            print(f"     Video ID: {video_id}")
            print()
        if len(youtube_rows) > 5:
            print(f"  ... and {len(youtube_rows) - 5} more")
        print("\nNo changes made (dry run).")
        return

    # Process YouTube videos
    stats = {'total': 0, 'updated': 0, 'skipped': 0, 'failed': 0}
    failed_videos = []

    print("Processing YouTube videos...\n")
    for i, row in tqdm(youtube_rows, desc="Fetching"):
        stats['total'] += 1

        link = row.get('Link', '').strip()

        # Extract video ID
        video_id = youtube.extract_video_id(link)
        if not video_id:
            logging.error(f"Row {i}: Invalid YouTube URL: {link}")
            stats['failed'] += 1
            failed_videos.append((link, "Invalid URL"))
            continue

        # Check if already has data
        current_description = row.get('Açıklama', '').strip()
        current_title = row.get('Başlık', '').strip()
        has_image = bool(row.get('Görsel URL', '').strip())
        has_date = bool(row.get('Tarih', '').strip())

        # Check if description is meaningful (not just the title)
        has_real_description = current_description and current_description != current_title

        if has_real_description and has_image and has_date:
            logging.debug(f"Row {i}: Already has data, skipping")
            stats['skipped'] += 1
            continue

        # Fetch video data
        video_data = youtube.fetch_video_data(video_id)

        if not video_data:
            stats['failed'] += 1
            failed_videos.append((link, "Failed to fetch data"))
            continue

        # Update row
        if not has_real_description:
            row['Açıklama'] = video_data['description']
        if not has_image:
            row['Görsel URL'] = video_data['thumbnail_url']
        if not has_date:
            row['Tarih'] = format_date(video_data['published_at'])

        logging.info(f"Row {i}: Updated '{video_data['title']}'")
        stats['updated'] += 1

    # Write updated CSV
    if stats['updated'] > 0:
        print("\nUpdating CSV...")
        try:
            csv_handler.write_rows(rows)
            csv_handler.verify_integrity(original_count)
            print("CSV updated successfully! ✓")
        except Exception as e:
            print(f"Error writing CSV: {e}")
            sys.exit(1)

    # Summary
    print("\n" + "=" * 30)
    print("Summary:")
    print(f"  Total: {stats['total']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Failed: {stats['failed']}")

    # Failed videos
    if failed_videos:
        print(f"\nFailed videos saved to: failed_youtube_urls.txt")
        with open('failed_youtube_urls.txt', 'w') as f:
            for url, reason in failed_videos:
                f.write(f"{url}\t{reason}\n")


if __name__ == '__main__':
    main()
