#!/usr/bin/env python3
"""
CSV Başlık ve Hashtag Düzenleyici
- Başlıkları açıklamadan otomatik oluşturur
- Hashtag'leri ayrı sütuna çıkarır
"""

import csv
import re
import shutil
from pathlib import Path


def extract_hashtags(text):
    """Metinden hashtag'leri çıkar"""
    if not text:
        return []

    # #kelime formatındaki hashtag'leri bul
    hashtags = re.findall(r'#\w+', text)
    return hashtags


def generate_title_from_description(description):
    """Açıklamadan otomatik başlık oluştur"""
    if not description:
        return "İsimsiz Tarif"

    # Açıklamayı temizle
    text = description.strip()

    # Hashtag'leri ve emoji'leri kaldır
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r'[^\w\s\.,!?çÇğĞıİöÖşŞüÜ-]', '', text)

    # İlk satırı al
    first_line = text.split('\n')[0].strip()

    # Çok uzunsa ilk cümleyi al
    if len(first_line) > 50:
        # İlk cümleyi bul (. ! ? ile biten)
        sentences = re.split(r'[.!?]+', first_line)
        if sentences:
            first_line = sentences[0].strip()

    # Hala çok uzunsa ilk 50 karakteri al
    if len(first_line) > 50:
        first_line = first_line[:47] + "..."

    # Boşsa fallback
    if not first_line:
        # Focaccia, Tiramisu gibi kelimeler var mı?
        words = text.split()
        for word in words:
            if len(word) > 3 and word[0].isupper():
                return word
        return "İsimsiz Tarif"

    return first_line


def main():
    csv_path = Path('recipes.csv')

    # Backup oluştur
    backup_path = csv_path.with_suffix('.csv.backup2')
    shutil.copy(str(csv_path), str(backup_path))
    print(f"✓ Backup oluşturuldu: {backup_path}")

    # CSV'yi oku
    rows = []
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')
        fieldnames = reader.fieldnames

        for row in reader:
            rows.append(row)

    print(f"✓ {len(rows)} satır okundu")

    # Yeni sütun ekle: Hashtag
    new_fieldnames = list(fieldnames)
    if 'Hashtag' not in new_fieldnames:
        # Tarih'ten sonra ekle
        if 'Tarih' in new_fieldnames:
            tarih_index = new_fieldnames.index('Tarih')
            new_fieldnames.insert(tarih_index + 1, 'Hashtag')
        else:
            new_fieldnames.append('Hashtag')

    # Her satırı işle
    updated_count = 0
    for i, row in enumerate(rows, 1):
        açıklama = row.get('Açıklama', '')
        mevcut_başlık = row.get('Başlık', '')

        # 1. Hashtag'leri çıkar
        hashtags = extract_hashtags(açıklama)
        row['Hashtag'] = ' '.join(hashtags) if hashtags else ''

        # 2. Başlığı güncelle (sadece "Tarif1", "Tarif2" gibi generic isimler için)
        if mevcut_başlık.startswith('Tarif') and mevcut_başlık[5:].isdigit():
            yeni_başlık = generate_title_from_description(açıklama)
            row['Başlık'] = yeni_başlık
            print(f"  {i}. '{mevcut_başlık}' → '{yeni_başlık}' | Hashtag: {row['Hashtag'][:50]}...")
            updated_count += 1
        else:
            # Başlık zaten güzel, sadece hashtag ekle
            if hashtags:
                print(f"  {i}. '{mevcut_başlık}' (değişmedi) | Hashtag: {row['Hashtag'][:50]}...")

    # CSV'yi yaz
    temp_path = csv_path.with_suffix('.tmp')
    with open(temp_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)

    # Atomik rename
    shutil.move(str(temp_path), str(csv_path))

    print(f"\n✓ CSV güncellendi!")
    print(f"  - {updated_count} başlık güncellendi")
    print(f"  - Hashtag sütunu eklendi")
    print(f"  - Toplam satır: {len(rows)}")


if __name__ == '__main__':
    main()
