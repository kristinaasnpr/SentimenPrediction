import datetime
import feedparser
import requests
from bs4 import BeautifulSoup
import os
import re
import logging
import pandas as pd
import yfinance as yf
from dateutil import parser
from datetime import timedelta
import firebase_admin
from firebase_admin import credentials, firestore

# Set up logging
logging.basicConfig(filename='scraper_debug.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Firestore
cred = credentials.Certificate('serviceAccountKey.json')
firebase_admin.initialize_app(cred)
db = firestore.client()
collection_name = 'users'  # Ganti dengan nama koleksi yang Anda inginkan

# Define symbol and keywords mapping (as a dictionary)
symbol_keywords = {
    "ACES.JK": ["ACE Hardware Indonesia", "ACES", "ACES.JK"],
    "ADRO.JK": ["Adaro Energy Indonesia", "ADRO", "ADRO.JK"],
    "AKRA.JK": ["AKR Corporindo", "AKRA", "AKRA.JK"],
    "AMMN.JK": ["Amman Mineral International", "AMMN", "AMMN.JK"],
    "AMRT.JK": ["Sumber Alfaria Trijaya", "AMRT", "AMRT.JK"],
    "ANTM.JK": ["Aneka Tambang", "ANTM", "ANTM.JK"],
    "ARTO.JK": ["Bank Jago", "ARTO", "ARTO.JK"],
    "ASII.JK": ["Astra International", "ASII", "ASII.JK"],
    "BBCA.JK": ["BCA", "Bank Central Asia", "BBCA", "BBCA.JK"],
    "BBNI.JK": ["BNI", "Bank Negara Indonesia", "BBNI", "BBNI.JK"],
    "BBRI.JK": ["BRI", "Bank Rakyat Indonesia", "BBRI", "BBRI.JK"],
    "BBTN.JK": ["BTN", "Bank Tabungan Negara", "BBTN.JK"],
    "BMRI.JK": ["Bank Mandiri", "BMRI", "BMRI.JK"],
    "BRIS.JK": ["Bank Syariah Indonesia", "BRIS", "BRIS.JK"],
    "BRPT.JK": ["Barito Pacific", "BRPT", "BRPT.JK"],
    "BUKA.JK": ["Bukalapak", "BUKA.JK"],
    "CPIN.JK": ["Charoen Pokphand Indonesia", "CPIN", "CPIN.JK"],
    "ESSA.JK": ["ESSA Industries Indonesia", "ESSA"],
    "EXCL.JK": ["XL Axiata", "EXCL", "EXCL.JK"],
    "GGRM.JK": ["Gudang Garam", "GGRM", "GGRM.JK"],
    "GOTO.JK": ["Gojek Tokopedia", "GOTO", "GOTO.JK"],
    "HRUM.JK": ["Harum Energy", "HRUM", "HRUM.JK"],
    "ICBP.JK": ["Indofood CBP Sukses Makmur", "ICBP", "ICBP.JK"],
    "INCO.JK": ["Vale Indonesia", "INCO", "INCO.JK"],
    "INDF.JK": ["Indofood Sukses Makmur", "INDF", "INDF.JK"],
    "INKP.JK": ["Indah Kiat Pulp & Paper", "INKP", "INKP.JK"],
    "INTP.JK": ["Indocement Tunggal Prakarsa", "INTP", "INTP.JK"],
    "ISAT.JK": ["Indosat", "ISAT", "ISAT.JK"],
    "ITMG.JK": ["Indo Tambangraya Megah", "ITMG", "ITMG.JK"],
    "KLBF.JK": ["Kalbe Farma", "KLBF", "KLBF.JK"],
    "MAPI.JK": ["Mitra Adiperkasa", "MAPI", "MAPI.JK"],
    "MBMA.JK": ["Merdeka Battery Materials", "MBMA", "MBMA.JK"],
    "MDKA.JK": ["Merdeka Copper Gold", "MDKA", "MDKA.JK"],
    "MEDC.JK": ["Medco Energi Internasional", "MEDC", "MEDC.JK"],
    "MTEL.JK": ["Dayamitra Telekomunikasi", "MTEL", "MTEL.JK"],
    "PGAS.JK": ["Perusahaan Gas Negara", "PGAS", "PGAS.JK"],
    "PGEO.JK": ["Pertamina Geothermal Energy", "PGEO", "PGEO.JK"],
    "PTBA.JK": ["Bukit Asam", "PTBA", "PTBA.JK"],
    "SIDO.JK": ["Sido Muncul", "SIDO", "SIDO.JK"],
    "SMGR.JK": ["Semen Indonesia", "SMGR", "SMGR.JK"],
    "SRTG.JK": ["Saratoga Investama Sedaya", "SRTG", "SRTG.JK"],
    "TLKM.JK": ["Telkom Indonesia", "TLKM", "TLKM.JK"],
    "TOWR.JK": ["Sarana Menara Nusantara", "TOWR", "TOWR.JK"],
    "UNTR.JK": ["United Tractors", "UNTR", "UNTR.JK"],
    "UNVR.JK": ["Unilever Indonesia", "UNVR", "UNVR.JK"]
}

# Define RSS feeds
rss_feeds = [
    "https://www.cnbcindonesia.com/market/rss",
    "https://www.kompas.com/tag/rss-feed",
    "https://lapi.kumparan.com/v2.0/rss/",
    "https://finance.detik.com/rss",
    "https://id.investing.com/rss/news.rss",
    "https://www.cnnindonesia.com/ekonomi/rss",
    "https://www.republika.co.id/rss/ekonomi",
    "https://www.beritasatu.com/rss/pasar-modal",
    "https://www.stockwatch.id/rss"
]

# Fungsi untuk mendapatkan hari kerja terakhir sebelum atau sama dengan tanggal yang diberikan
def get_last_weekday(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    while date.weekday() >= 5:
        date -= datetime.timedelta(days=1)
    return date.strftime("%Y-%m-%d")

# Fungsi untuk mendapatkan hari kerja berikutnya setelah atau sama dengan tanggal yang diberikan
def get_next_weekday(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    while date.weekday() >= 5:
        date += datetime.timedelta(days=1)
    return date.strftime("%Y-%m-%d")

# Fungsi mengambil data saham hanya untuk hari Senin - Jumat
def get_stock_data(symbols, start_date, end_date):
    logging.info(f"Fetching stock data for symbols: {symbols} from {start_date} to {end_date}")
    stock_data = {}
    for symbol in symbols:
        try:
            data = yf.download(symbol, start=start_date, end=end_date)
            if not data.empty:
                data = data[data.index.weekday < 5]  # Hanya ambil hari kerja (Senin-Jumat)
                data.reset_index(inplace=True)
                data['Date'] = data['Date'].astype(str)
                stock_data[symbol] = data[['Date', 'Close']]
        except Exception as e:
            logging.error(f"Failed to fetch data for {symbol}: {e}")
    return stock_data

def find_nearest_price(date, stock_df):
    """Mencari harga saham terdekat dari tanggal yang diberikan."""
    available_dates = stock_df['Date'].values
    if available_dates.size == 0:
        return None

    date_str = date.strftime('%Y-%m-%d')

    # Jika harga ada pada tanggal yang diminta
    if date_str in available_dates:
        return stock_df[stock_df['Date'] == date_str]['Close'].values[0]

    # Jika harga tidak ada, coba cari harga setelahnya
    next_date = date + datetime.timedelta(days=1)
    while next_date.strftime('%Y-%m-%d') not in available_dates:
        next_date += datetime.timedelta(days=1)
        if next_date.weekday() >= 5:  # Sabtu atau Minggu, lewati
            continue
        if next_date > datetime.datetime.today():  # Jika sudah melewati hari ini, return None
            return None

    return stock_df[next_date.strftime('%Y-%m-%d')]['Close'].values[0]

def determine_label(row, stock_data):
    symbol = row[COL_SYMBOL]
    date = row[COL_TANGGAL]

    if not symbol or symbol not in stock_data:
        return None

    stock_df = stock_data[symbol]
    date_obj = pd.to_datetime(date)

    # Untuk Sabtu, Minggu, gunakan harga Jumat dan Senin
    if date_obj.weekday() == 5:  # Sabtu
        prev_close = find_nearest_price(date_obj - datetime.timedelta(days=1), stock_df)  # Jumat
        next_close = find_nearest_price(date_obj + datetime.timedelta(days=2), stock_df)  # Senin
    elif date_obj.weekday() == 6:  # Minggu
        prev_close = find_nearest_price(date_obj - datetime.timedelta(days=2), stock_df)  # Jumat
        next_close = find_nearest_price(date_obj + datetime.timedelta(days=1), stock_df)  # Senin
    elif date_obj.weekday() == 0:  # Senin
        prev_close = find_nearest_price(date_obj - datetime.timedelta(days=3), stock_df)  # Jumat
        next_close = find_nearest_price(date_obj, stock_df)  # Senin
    else:  # Hari Selasa sampai Jumat
        prev_close = find_nearest_price(date_obj - datetime.timedelta(days=1), stock_df)  # Hari sebelumnya
        next_close = find_nearest_price(date_obj, stock_df)  # Hari itu juga

    # Jika harga setelahnya belum tersedia, return None
    if next_close is None:
        return None

    # Beri label berdasarkan perbandingan harga
    if prev_close is not None and next_close is not None:
        if next_close > prev_close:
            return 'Naik'
        elif next_close < prev_close:
            return 'Turun'
        else:
            return 'Netral'  # Jika harga Jumat & Senin sama persis

    return None

def read_existing_data():
    """Read existing article titles from Firestore."""
    existing_titles = set()
    try:
        users_ref = db.collection(collection_name)
        docs = users_ref.stream()
        for doc in docs:
            existing_titles.add(doc.to_dict()[COL_JUDUL])  # Ganti dengan nama kolom yang sesuai
    except Exception as e:
        logging.error(f"Failed to read existing data from Firestore: {e}")
    return existing_titles

def save_to_firestore(article):
    """Simpan artikel ke Firestore."""
    try:
        db.collection(collection_name).add(article)
        logging.info(f"Article saved to Firestore: {article[COL_JUDUL]}")
    except Exception as e:
        logging.error(f"Failed to save article to Firestore: {e}")

def get_full_article(url):
    """Fetch the full article from the given URL."""
    logging.info(f"Attempting to fetch article from: {url}")
    try:
        headers = {
            'User -Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        content = ""
        selectors = [
            'div.detail_text',
            'div.detail-artikelBaru',
            'div[data-sticky-container]',
            'article.detail',
            'div.article-content',
            'div.article-body'
        ]
        for selector in selectors:
            article_body = soup.select_one(selector)
            if article_body:
                paragraphs = article_body.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol'])
                content = ' '.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                if content:
                    logging.info(f"Content extracted using selector: {selector}")
                    break

        if not content:
            paragraphs = soup.find_all(['p', 'h2', 'h3', 'h4', 'ul', 'ol'])
            content = ' '.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
            logging.info("Content extracted from generic paragraph tags.")

        content = remove_img_src(content)  # Remove image tags if any
        return content.strip()  # Return stripped content

    except Exception as e:
        logging.error(f"Failed to fetch article: {e}")
        return None

def scrape_rss_feeds():
    """Scrape articles from RSS feeds and save to Firestore."""
    existing_titles = read_existing_data()

    new_articles = []

    for feed_url in rss_feeds:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            title = entry.title
            link = entry.link
            published = entry.published if 'published' in entry else entry.updated
            formatted_date, formatted_time = parse_date_time(published)
            if not formatted_date or not formatted_time:
                logging.warning(f"Skipping article due to unparseable date: {published}")
                continue

            if title not in existing_titles:
                content = get_full_article(link)
                if content:
                    new_article = {
                        COL_NO: len(new_articles) + 1,
                        COL_JUDUL: title,
                        COL_ISI: content,
                        COL_LINK: link,
                        COL_TANGGAL: formatted_date,
                        COL_SYMBOL: find_symbols(content),
                        COL_LABEL: None  # Label diisi nanti
                    }
                    new_articles.append(new_article)
                    save_to_firestore(new_article)

    if new_articles:
        logging.info(f"Wrote {len(new_articles)} new articles to Firestore.")
    else:
        logging.info("No new articles found.")

# Run the scraper
if __name__ == "__main__":
    scrape_rss_feeds()