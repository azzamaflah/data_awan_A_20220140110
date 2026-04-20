from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# KONFIGURASI MONGODB ATLAS
# ─────────────────────────────────────────────────────────────────────────────

MONGO_URI       = "mongodb+srv://dbUser:123password321@cluster0.y5qtfve.mongodb.net/?retryWrites=true&w=majority"
DB_NAME         = "cnbc_crawler"
COLLECTION_NAME = "articles"

try:
    client     = MongoClient(MONGO_URI)
    db         = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    client.server_info()
    print("✅ Koneksi ke MongoDB Atlas Berhasil!")
    print(f"   DB         : {DB_NAME}")
    print(f"   Collection : {COLLECTION_NAME}")
except Exception as e:
    print(f"❌ Gagal konek ke MongoDB Atlas: {e}")
    exit()

# ─────────────────────────────────────────────────────────────────────────────
# KONFIGURASI CRAWLER
# ─────────────────────────────────────────────────────────────────────────────

# Batas maksimal artikel yang disimpan
MAX_ARTICLES = 10

SOURCE_PAGES = [
    
    "https://www.cnbcindonesia.com/tag/energi-terbarukan",
    "https://www.cnbcindonesia.com/tag/lingkungan",
    "https://www.cnbcindonesia.com/tag/sustainability",
    "https://www.cnbcindonesia.com/tag/esg",
    "https://www.cnbcindonesia.com/tag/perubahan-iklim",
    "https://www.cnbcindonesia.com/tag/kendaraan-listrik",
    "https://www.cnbcindonesia.com/tag/green",
    "https://www.cnbcindonesia.com/tag/net-zero",
    "https://www.cnbcindonesia.com/tag/emisi-karbon",
    "https://www.cnbcindonesia.com/tag/panel-surya",
  
    "https://www.cnbcindonesia.com/search?q=energi+terbarukan",
    "https://www.cnbcindonesia.com/search?q=sustainability",
    "https://www.cnbcindonesia.com/search?q=lingkungan+hidup",
    "https://www.cnbcindonesia.com/search?q=perubahan+iklim",
    "https://www.cnbcindonesia.com/search?q=emisi+karbon",
    "https://www.cnbcindonesia.com/search?q=kendaraan+listrik",
    "https://www.cnbcindonesia.com/search?q=esg",
    "https://www.cnbcindonesia.com/search?q=net+zero",
    "https://www.cnbcindonesia.com/search?q=polusi+udara",
    "https://www.cnbcindonesia.com/search?q=panel+surya",
]

# ─────────────────────────────────────────────────────────────────────────────
# SETUP SELENIUM DRIVER
# ─────────────────────────────────────────────────────────────────────────────

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=chrome_options)
    # Sembunyikan tanda webdriver agar tidak dideteksi
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Kumpulkan semua URL artikel dari halaman listing
# ─────────────────────────────────────────────────────────────────────────────

def kumpulkan_url_artikel(driver, source_url: str) -> list:
    """
    Buka halaman indeks/listing CNBC Indonesia,
    lalu kumpulkan semua URL artikel yang ada.
    """
    print(f"\n🌐 Membuka sumber: {source_url}")
    try:
        driver.get(source_url)
        time.sleep(5)
    except Exception as e:
        print(f"   ⚠️  Gagal buka halaman sumber: {e}")
        return []

    soup     = BeautifulSoup(driver.page_source, 'lxml')
    url_list = []

    # Coba berbagai selector untuk menangkap link artikel
    # CNBC Indonesia memiliki beberapa template halaman listing
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        # URL artikel CNBC Indo selalu mengandung /read/ atau pola angka-judul
        if (
            href.startswith("https://www.cnbcindonesia.com")
            and "/read/" in href
            and href not in url_list
        ):
            url_list.append(href.split("?")[0])  # Buang query string

    # Fallback: cari lewat tag <article>
    if not url_list:
        for artikel in soup.find_all('article'):
            link_tag = artikel.find('a', href=True)
            if link_tag:
                href = link_tag['href']
                if href.startswith("http") and href not in url_list:
                    url_list.append(href.split("?")[0])

    print(f"   🔍 Ditemukan {len(url_list)} URL artikel di halaman ini.")
    return url_list


# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Ekstrak detail satu artikel
# ─────────────────────────────────────────────────────────────────────────────

def ekstrak_detail_artikel(driver, url: str) -> dict | None:
    """
    Kunjungi URL artikel, ekstrak semua field yang dibutuhkan,
    dan return sebagai dict. Return None jika gagal / tidak relevan.
    """
    print(f"\n📄 Memproses: {url}")
    try:
        driver.get(url)
        time.sleep(random.uniform(2, 4))
    except Exception as e:
        print(f"   ⚠️  Gagal buka URL: {e}")
        return None

    detail_soup = BeautifulSoup(driver.page_source, 'lxml')

    # ── 1. JUDUL ─────────────────────────────────────────────────────────────
    judul_meta = detail_soup.find('meta', property='og:title')
    judul      = judul_meta['content'].strip() if judul_meta else ""

    if not judul:
        h1_tag = detail_soup.find('h1')
        judul  = h1_tag.get_text(strip=True) if h1_tag else "N/A"

    # ── 2. TANGGAL PUBLISH ────────────────────────────────────────────────────
    # Prioritas: meta dtk:publishdate → article:published_time → sekarang
    tanggal = None

    tanggal_tag = detail_soup.find('meta', attrs={'name': 'dtk:publishdate'})
    if tanggal_tag and tanggal_tag.get('content'):
        tanggal = tanggal_tag['content'].strip()

    if not tanggal:
        og_date = detail_soup.find('meta', property='article:published_time')
        if og_date and og_date.get('content'):
            tanggal = og_date['content'].strip()

    if not tanggal:
        # Cari elemen waktu di HTML
        time_tag = detail_soup.find('time')
        if time_tag:
            tanggal = time_tag.get('datetime') or time_tag.get_text(strip=True)

    if not tanggal:
        tanggal = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── 3. AUTHOR ─────────────────────────────────────────────────────────────
    author = "Redaksi CNBC Indonesia"

    author_tag = detail_soup.find('meta', attrs={'name': 'dtk:author'})
    if author_tag and author_tag.get('content'):
        author = author_tag['content'].strip()

    if author == "Redaksi CNBC Indonesia":
        # Fallback selector HTML
        for sel in ['div.author a', 'span.author', 'div.byline a',
                    'a.author-name', 'span.reporter-name', 'div.penulis']:
            el = detail_soup.select_one(sel)
            if el:
                author = el.get_text(strip=True)
                break

    # ── 4. TAG / KATEGORI ─────────────────────────────────────────────────────
    tags = "N/A"

    tags_tag = detail_soup.find('meta', attrs={'name': 'keywords'})
    if tags_tag and tags_tag.get('content'):
        tags = tags_tag['content'].strip()

    if tags == "N/A":
        # Coba ambil dari elemen tag di halaman
        tag_elements = detail_soup.select('div.tags-artikel a, div.tag-list a, a[rel="tag"]')
        if tag_elements:
            tags = ", ".join(t.get_text(strip=True) for t in tag_elements)

    # ── 5. THUMBNAIL ──────────────────────────────────────────────────────────
    thumbnail = "N/A"

    thumb_meta = detail_soup.find('meta', property='og:image')
    if thumb_meta and thumb_meta.get('content'):
        thumbnail = thumb_meta['content'].strip()

    if thumbnail == "N/A":
        tw_img = detail_soup.find('meta', attrs={'name': 'twitter:image'})
        if tw_img and tw_img.get('content'):
            thumbnail = tw_img['content'].strip()

    # ── 6. ISI BERITA ─────────────────────────────────────────────────────────
    isi_berita = "Isi berita tidak ditemukan"

    # Selector konten CNBC Indonesia (beberapa template)
    konten_selectors = [
        ('div', {'class': 'detail_text'}),
        ('div', {'class': 'detail-text'}),
        ('div', {'id': 'articleBody'}),
        ('div', {'class': 'article-content'}),
        ('div', {'class': 'content-detail'}),
    ]

    for tag_name, attrs in konten_selectors:
        body_div = detail_soup.find(tag_name, attrs)
        if body_div:
            paragraphs = [
                p.get_text(strip=True)
                for p in body_div.find_all('p')
                if p.get_text(strip=True)
            ]
            if paragraphs:
                isi_berita = " ".join(paragraphs)
                break

    return {
        'url'            : url,
        'judul'          : judul,
        'tanggal_publish': tanggal,
        'author'         : author,
        'tag_kategori'   : tags,
        'isi_berita'     : isi_berita,
        'thumbnail'      : thumbnail,
        'tema'           : 'Environmental Sustainability',
        'scraped_at'     : datetime.now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# FUNGSI UTAMA CRAWLER
# ─────────────────────────────────────────────────────────────────────────────

def crawl_cnbc_hybrid():
    driver = get_driver()
    print("\n" + "=" * 65)
    print("  CNBC Indonesia Crawler — Environmental Sustainability")
    print("=" * 65)

    # ── Fase 1: Kumpulkan semua URL dari semua halaman sumber ─────────────
    print("\n[FASE 1] Mengumpulkan URL artikel dari semua sumber...")
    semua_url = []
    url_sudah_dikunjungi = set()

    for source_url in SOURCE_PAGES:
        ditemukan = kumpulkan_url_artikel(driver, source_url)
        for u in ditemukan:
            if u not in url_sudah_dikunjungi:
                semua_url.append(u)
                url_sudah_dikunjungi.add(u)
        time.sleep(random.uniform(1.5, 3.0))

    print(f"\n📦 Total URL unik yang akan diproses: {len(semua_url)}")

    # ── Fase 2: Crawl & simpan setiap artikel ────────────────────────────
    print("\n[FASE 2] Crawl dan parse detail setiap artikel...\n")
    print(f"🎯 Target: {MAX_ARTICLES} artikel relevan\n")
    count_berhasil = 0
    count_skip     = 0
    count_error    = 0

    for idx, url in enumerate(semua_url, start=1):
        # ── Stop jika sudah mencapai batas ───────────────────────────────
        if count_berhasil >= MAX_ARTICLES:
            print(f"\n🏁 Target {MAX_ARTICLES} artikel tercapai. Crawling dihentikan.")
            break

        print(f"[{idx}/{len(semua_url)}]", end=" ")
        try:
            data = ekstrak_detail_artikel(driver, url)

            if data is None:
                count_skip += 1
                continue

            # ── Simpan ke MongoDB Atlas ───────────────────────────────────
            print(f"   💾 Mencoba simpan: {data['judul'][:50]}...")
            result = collection.update_one(
                {'url': url},
                {'$set': data},
                upsert=True
            )

            if result.acknowledged:
                label = "INSERTED" if result.upserted_id else "UPDATED"
                print(f"   ✅ {label}: {data['judul'][:55]}...")
                count_berhasil += 1
            else:
                print(f"   ❌ GAGAL SIMPAN: Atlas tidak merespon.")
                count_error += 1

        except Exception as e:
            print(f"   ⚠️  Error pada artikel ini: {e}")
            count_error += 1
            continue

        # Jeda manusiawi antar artikel
        time.sleep(random.uniform(2, 4))

    # ── Ringkasan Akhir ───────────────────────────────────────────────────
    driver.quit()
    print("\n" + "=" * 65)
    print("  ✨ CRAWLING SELESAI!")
    print(f"  📊 Total URL ditemukan   : {len(semua_url)}")
    print(f"  ✅ Berhasil disimpan     : {count_berhasil}")
    print(f"  ⏩ Di-skip (gagal parse) : {count_skip}")
    print(f"  ❌ Error                 : {count_error}")
    print("=" * 65 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    crawl_cnbc_hybrid()