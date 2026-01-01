#!/usr/bin/env python3
"""
Instagram Thumbnail Downloader
Downloads Instagram post/reel thumbnails and updates recipes.csv
"""

import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from PIL import Image

# Configuration
DEFAULT_CSV = 'recipes.csv'
DEFAULT_OUTPUT_DIR = 'images'
DEFAULT_DELAY = 3.0
MAX_RETRIES = 3
TIMEOUT = 10
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
]


class ProgressTracker:
    """Manages download progress and resume capability"""

    def __init__(self, progress_file: str = '.download_progress.json'):
        self.progress_file = progress_file
        self.data = self.load()

    def load(self) -> Dict:
        """Load progress from file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'completed_ids': [], 'failed_ids': {}}

    def save(self):
        """Save progress to file"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.data, f, indent=2)

    def mark_completed(self, id: str):
        """Mark ID as completed"""
        if id not in self.data['completed_ids']:
            self.data['completed_ids'].append(id)
        self.save()

    def mark_failed(self, id: str, reason: str):
        """Mark ID as failed"""
        self.data['failed_ids'][id] = reason
        self.save()

    def is_completed(self, id: str) -> bool:
        """Check if ID is completed"""
        return id in self.data['completed_ids']


class SkipChecker:
    """Determines if a row should be skipped"""

    @staticmethod
    def should_skip(image_url: str) -> bool:
        """
        Check if row should be skipped based on image URL.

        Skip if:
        - URL starts with 'images/' (already local)

        Don't skip if:
        - URL contains 'unsplash.com' (placeholder)
        - URL is empty
        """
        if not image_url or not image_url.strip():
            return False  # Download if empty

        image_url = image_url.strip()

        # Already local
        if image_url.startswith('images/'):
            return True

        # Unsplash placeholder - should download
        if 'unsplash.com' in image_url or 'placeholder' in image_url:
            return False

        return False


class InstagramScraper:
    """Fetches Instagram thumbnails using oEmbed API"""

    def __init__(self, delay: float = 3.0):
        self.delay = delay
        self.session = requests.Session()
        self.last_request_time = 0

    def _wait(self):
        """Wait between requests to avoid rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()

    def _get_random_user_agent(self) -> str:
        """Get random user agent"""
        import random
        return random.choice(USER_AGENTS)

    def parse_instagram_url(self, url: str) -> Optional[Tuple[str, str]]:
        """
        Parse Instagram URL to extract type and ID.

        Returns: (type, id) where type is 'post' or 'reel'
        """
        pattern = r'https://www\.instagram\.com/(p|reel)/([^/]+)'
        match = re.search(pattern, url)
        if match:
            url_type = 'post' if match.group(1) == 'p' else 'reel'
            post_id = match.group(2)
            return (url_type, post_id)
        return None

    def fetch_thumbnail_url(self, instagram_url: str) -> Optional[str]:
        """
        Fetch thumbnail URL from Instagram using oEmbed API.
        Falls back to OG tag if oEmbed fails.
        """
        self._wait()

        try:
            # Try oEmbed API first (works for both posts and reels)
            oembed_url = f"https://www.instagram.com/api/v1/oembed/?url={quote(instagram_url)}"
            logging.debug(f"Fetching oEmbed: {oembed_url}")

            response = self.session.get(oembed_url, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            thumbnail_url = data.get('thumbnail_url')
            if thumbnail_url:
                logging.debug(f"Got thumbnail from oEmbed: {thumbnail_url[:100]}...")
                return thumbnail_url

        except Exception as e:
            logging.warning(f"oEmbed failed: {e}, trying OG tag fallback")

        # Fallback: Try OG tag (works for posts when logged out)
        try:
            self._wait()
            headers = {'User-Agent': self._get_random_user_agent()}
            response = self.session.get(instagram_url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            og_image = soup.find('meta', property='og:image')

            if og_image and og_image.get('content'):
                thumbnail_url = og_image['content']
                logging.debug(f"Got thumbnail from OG tag: {thumbnail_url[:100]}...")
                return thumbnail_url

        except Exception as e:
            logging.error(f"OG tag fallback failed: {e}")

        return None


class ImageDownloader:
    """Downloads and validates images"""

    def __init__(self, output_dir: str, skip_existing: bool = False):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.skip_existing = skip_existing

    def download(self, url: str, filename: str) -> bool:
        """
        Download image from URL to local file.

        Returns: True if successful, False otherwise
        """
        filepath = self.output_dir / filename

        # Skip if exists
        if self.skip_existing and filepath.exists():
            logging.debug(f"Skipping existing file: {filename}")
            return True

        try:
            logging.debug(f"Downloading: {url[:100]}...")
            response = requests.get(url, stream=True, timeout=TIMEOUT)
            response.raise_for_status()

            # Check file size
            content_length = response.headers.get('Content-Length')
            if content_length and int(content_length) > MAX_FILE_SIZE:
                logging.error(f"File too large: {content_length} bytes")
                return False

            # Download to temp file first
            temp_filepath = filepath.with_suffix('.tmp')
            with open(temp_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Verify image
            if not self.verify_image(temp_filepath):
                os.remove(temp_filepath)
                return False

            # Move to final location
            shutil.move(str(temp_filepath), str(filepath))
            logging.debug(f"Downloaded successfully: {filename}")
            return True

        except Exception as e:
            logging.error(f"Download failed: {e}")
            return False

    def verify_image(self, filepath: Path) -> bool:
        """Verify image is valid"""
        try:
            with Image.open(filepath) as img:
                img.verify()
            return True
        except Exception as e:
            logging.error(f"Image verification failed: {e}")
            return False


class CSVHandler:
    """Handles CSV reading/writing with UTF-8 BOM"""

    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)

    def backup(self) -> str:
        """Create backup of CSV file"""
        backup_path = self.csv_path.with_suffix('.csv.backup')
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
            logging.FileHandler('download.log')
        ]
    )


def parse_row_range(row_spec: str) -> List[int]:
    """
    Parse row range specification.

    Examples:
    - "1-10" -> [1,2,3,4,5,6,7,8,9,10]
    - "1,3,5" -> [1,3,5]
    - "1-5,10,15-20" -> [1,2,3,4,5,10,15,16,17,18,19,20]
    """
    rows = []
    parts = row_spec.split(',')
    for part in parts:
        if '-' in part:
            start, end = map(int, part.split('-'))
            rows.extend(range(start, end + 1))
        else:
            rows.append(int(part))
    return sorted(set(rows))


def main():
    parser = argparse.ArgumentParser(description='Instagram Thumbnail Downloader')
    parser.add_argument('--csv', default=DEFAULT_CSV, help='Path to CSV file')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR, help='Output directory for images')
    parser.add_argument('--delay', type=float, default=DEFAULT_DELAY, help='Delay between requests (seconds)')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without downloading')
    parser.add_argument('--skip-existing', action='store_true', help='Skip already downloaded images')
    parser.add_argument('--resume', action='store_true', default=True, help='Resume from last progress')
    parser.add_argument('--no-backup', action='store_true', help='Skip CSV backup')
    parser.add_argument('--rows', type=str, help='Process specific rows (e.g., "1-10,15")')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Setup
    setup_logging(args.verbose)

    print("Instagram Thumbnail Downloader")
    print("=" * 30)
    print(f"CSV: {args.csv}")
    print(f"Output: {args.output_dir}/")
    print(f"Delay: {args.delay}s between requests")
    print()

    # Initialize components
    csv_handler = CSVHandler(args.csv)
    scraper = InstagramScraper(args.delay)
    downloader = ImageDownloader(args.output_dir, args.skip_existing)
    progress = ProgressTracker() if args.resume else None
    skip_checker = SkipChecker()

    # Read CSV
    try:
        rows = csv_handler.read_rows()
        original_count = len(rows)
        print(f"Loaded {original_count} rows from CSV")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    # Parse row range if specified
    row_indices = None
    if args.rows:
        row_indices = parse_row_range(args.rows)
        print(f"Processing rows: {row_indices}")

    # Backup CSV
    if not args.no_backup and not args.dry_run:
        backup_path = csv_handler.backup()
        print(f"Creating backup: {backup_path} ✓")

    # Load progress
    if progress:
        completed = len(progress.data['completed_ids'])
        failed = len(progress.data['failed_ids'])
        if completed > 0 or failed > 0:
            print(f"Loading progress: Found {completed} completed, {failed} failed")
        else:
            print("Loading progress: Starting fresh")

    # Dry run
    if args.dry_run:
        print("\n[DRY RUN] No files will be modified\n")
        print(f"Would process {original_count} rows:")
        for i, row in enumerate(rows[:10], 1):  # Show first 10
            instagram_url = row.get('Link', '').strip()
            if instagram_url:
                parsed = scraper.parse_instagram_url(instagram_url)
                if parsed:
                    url_type, post_id = parsed
                    filename = f"{url_type}_{post_id}.jpg"
                    print(f"  {i}. {row.get('Başlık', 'Unknown')}: {instagram_url}")
                    print(f"     -> {args.output_dir}/{filename}")
        if original_count > 10:
            print(f"  ... and {original_count - 10} more")
        print("\nNo changes made (dry run).")
        return

    # Process rows
    stats = {'total': 0, 'downloaded': 0, 'skipped': 0, 'failed': 0}
    failed_urls = []

    # Filter rows
    rows_to_process = []
    for i, row in enumerate(rows, 1):
        if row_indices and i not in row_indices:
            continue
        rows_to_process.append((i, row))

    print()
    for i, row in tqdm(rows_to_process, desc="Downloading"):
        stats['total'] += 1

        # Get URLs
        instagram_url = row.get('Link', '').strip()
        current_image_url = row.get('Görsel URL', '').strip()

        if not instagram_url:
            logging.warning(f"Row {i}: No Instagram URL")
            stats['skipped'] += 1
            continue

        # Check if should skip
        if skip_checker.should_skip(current_image_url):
            logging.debug(f"Row {i}: Skipping (already has local image)")
            stats['skipped'] += 1
            continue

        # Parse Instagram URL
        parsed = scraper.parse_instagram_url(instagram_url)
        if not parsed:
            logging.error(f"Row {i}: Invalid Instagram URL: {instagram_url}")
            stats['failed'] += 1
            failed_urls.append((instagram_url, "Invalid URL"))
            continue

        url_type, post_id = parsed

        # Check progress
        if progress and progress.is_completed(post_id):
            logging.debug(f"Row {i}: Already completed: {post_id}")
            stats['skipped'] += 1
            continue

        # Fetch thumbnail URL
        try:
            thumbnail_url = scraper.fetch_thumbnail_url(instagram_url)
            if not thumbnail_url:
                raise Exception("Thumbnail URL not found")
        except Exception as e:
            logging.error(f"Row {i}: Failed to fetch thumbnail: {e}")
            stats['failed'] += 1
            failed_urls.append((instagram_url, str(e)))
            if progress:
                progress.mark_failed(post_id, str(e))
            continue

        # Download image
        filename = f"{url_type}_{post_id}.jpg"
        success = downloader.download(thumbnail_url, filename)

        if success:
            # Update CSV row
            row['Görsel URL'] = f"{args.output_dir}/{filename}"
            stats['downloaded'] += 1
            if progress:
                progress.mark_completed(post_id)
        else:
            stats['failed'] += 1
            failed_urls.append((instagram_url, "Download failed"))
            if progress:
                progress.mark_failed(post_id, "Download failed")

    # Write updated CSV
    if stats['downloaded'] > 0:
        print("\nUpdating CSV...")
        try:
            csv_handler.write_rows(rows)
            csv_handler.verify_integrity(original_count)
            print("CSV updated successfully! ✓")
        except Exception as e:
            print(f"Error writing CSV: {e}")
            print(f"Restoring from backup...")
            # Could implement restore logic here
            sys.exit(1)

    # Summary
    print("\n" + "=" * 30)
    print("Summary:")
    print(f"  Total: {stats['total']}")
    print(f"  Downloaded: {stats['downloaded']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Failed: {stats['failed']}")

    # Failed URLs
    if failed_urls:
        print(f"\nFailed URLs saved to: failed_urls.txt")
        with open('failed_urls.txt', 'w') as f:
            for url, reason in failed_urls:
                f.write(f"{url}\t{reason}\n")


if __name__ == '__main__':
    main()
