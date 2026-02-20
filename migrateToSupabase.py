#!/usr/bin/env python3
"""
CSV → Supabase Migration Script
Reads recipes.csv and uploads to Supabase via REST API
"""

import csv
import json
import urllib.request
import urllib.error

SUPABASE_URL = "https://dobykurhhrfcflselhwe.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRvYnlrdXJoaHJmY2Zsc2VsaHdlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE1NzcxMTIsImV4cCI6MjA4NzE1MzExMn0.LkQDF9O_WzRzcNSpS2NO0YSjTeen_DIuc5WsSyPP3CQ"

HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

def parse_date(date_str):
    """DD.MM.YYYY → YYYY-MM-DD"""
    if not date_str or '.' not in date_str:
        return None
    parts = date_str.strip().split('.')
    if len(parts) == 3:
        try:
            return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        except Exception:
            return None
    return None

def insert_batch(batch):
    """Insert a batch of recipes via Supabase REST API"""
    url = f"{SUPABASE_URL}/rest/v1/recipes"
    data = json.dumps(batch, ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=HEADERS, method='POST')
    try:
        with urllib.request.urlopen(req) as resp:
            return True, resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        return False, body

def migrate():
    recipes = []
    skipped = 0

    with open('recipes.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            title = (row.get('Başlık') or row.get('Baslik') or '').strip()
            platform = (row.get('Platform') or 'Instagram').strip()
            image_url = (row.get('Görsel URL') or row.get('Gorsel URL') or '').strip() or None
            link_url = (row.get('Link') or '').strip() or None
            description = (row.get('Açıklama') or row.get('Aciklama') or '').strip() or None
            hashtags = (row.get('Hashtag') or '').strip() or None
            published_date = parse_date(row.get('Tarih') or '')

            # Skip rows with no meaningful data
            if not title and not description:
                skipped += 1
                continue

            recipes.append({
                'title': title or None,
                'platform': platform,
                'image_url': image_url,
                'link_url': link_url,
                'description': description,
                'published_date': published_date,
                'hashtags': hashtags,
            })

    print(f"Parsed {len(recipes)} recipes ({skipped} skipped)")

    BATCH_SIZE = 500
    total_inserted = 0

    for i in range(0, len(recipes), BATCH_SIZE):
        batch = recipes[i:i + BATCH_SIZE]
        success, result = insert_batch(batch)
        if success:
            total_inserted += len(batch)
            print(f"  ✓ Batch {i+1}–{i+len(batch)} inserted ({total_inserted} total)")
        else:
            print(f"  ✗ Batch {i+1}–{i+len(batch)} FAILED: {result[:200]}")
            break

    print(f"\nDone! {total_inserted} recipes migrated to Supabase.")

if __name__ == '__main__':
    migrate()
