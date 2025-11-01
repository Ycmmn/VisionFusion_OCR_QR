# -*- coding: utf-8 -*-
"""
🚀 Excel Web Scraper - Professional Edition
Professional Excel web scraper + Gemini smart analysis + translation
"""
from pathlib import Path
import os, json, re, time, random, threading, socket, shutil
from queue import Queue
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# Gemini SDK Import
# =========================================================
try:
    import google.genai as genai
    from google.genai import types
    print("✅ Gemini SDK loaded successfully")
except Exception as e:
    print(f"❌ Gemini SDK error: {e}")
    import sys
    sys.exit(1)

# =========================================================
#  dynamic paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))

# input: auto-search for Excel file
INPUT_EXCEL_ENV = os.getenv("INPUT_EXCEL")
if INPUT_EXCEL_ENV:
    INPUT_EXCEL = Path(INPUT_EXCEL_ENV)
else:
    search_paths = [SESSION_DIR, SOURCE_FOLDER, RENAMED_DIR, SESSION_DIR / "input"]
    INPUT_EXCEL = None
    for search_path in search_paths:
        if search_path.exists():
            excel_files = list(search_path.glob("*.xlsx"))
            if excel_files:
                for f in excel_files:
                    if not f.name.startswith("output_enriched"):
                        INPUT_EXCEL = f
                        break
                if INPUT_EXCEL:
                    break
    if not INPUT_EXCEL:
        INPUT_EXCEL = SESSION_DIR / "input.xlsx"

OUTPUT_EXCEL = Path(os.getenv(
    "OUTPUT_EXCEL", 
    SESSION_DIR / f"output_enriched_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
))
TEMP_EXCEL = Path(os.getenv("TEMP_EXCEL", SESSION_DIR / "temp_output.xlsx"))
OUTPUT_JSON = Path(os.getenv("OUTPUT_JSON", SESSION_DIR / "scraped_data.json"))


# =========================================================
#  settings
# =========================================================
# api key - only one key
GOOGLE_API_KEY = "AIzaSyB.....ZDouR35hoZNxqsW6pc"

MODEL_NAME = "gemini-2.0-flash-exp"
THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3
MAX_RETRIES_GEMINI = 3
IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']

# Fields to extract
FIELDS = [
    "CompanyNameEN", "CompanyNameFA", "Logo", "Industry", "Certifications",
    "ContactName", "PositionEN", "PositionFA", "Department",
    "Phone1", "Phone2", "Fax", "WhatsApp", "Telegram", "Instagram", "LinkedIn",
    "Website", "Email", "OtherEmails",
    "AddressEN", "AddressFA", "Country", "City",
    "ProductName", "ProductCategory", "ProductDescription", "Applications",
    "Brands", "Description", "History", "Employees", "ClientsPartners", "Markets"
]

# Fields that need translation (EN -> FA)
TRANSLATABLE_FIELDS = [
    ("CompanyNameEN", "CompanyNameFA_translated"),
    ("AddressEN", "AddressFA_translated"),
    ("ProductName", "ProductNameFA"),
    ("ProductCategory", "ProductCategoryFA"),
    ("ProductDescription", "ProductDescriptionFA"),
    ("Applications", "ApplicationsFA"),
    ("Description", "DescriptionFA"),
    ("History", "HistoryFA"),
    ("Employees", "EmployeesFA"),
    ("ClientsPartners", "ClientsPartnersFA"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

lock = threading.Lock()
client = genai.Client(api_key=GOOGLE_API_KEY)

print(f"\n{'='*70}")
print("🚀 Excel Web Scraper - Professional Edition")
print(f"{'='*70}")
print(f"🔑 API Key: {GOOGLE_API_KEY[:20]}...")
print(f"📥 Input: {INPUT_EXCEL}")
print(f"📤 Output: {OUTPUT_EXCEL}")
print(f"{'='*70}\n")


# =========================================================
#  helper functions
# =========================================================
def normalize_url(url):
    """normalize url"""
    if not url or pd.isna(url) or str(url).lower() in ['nan', 'none', '']:
        return None
    url = str(url).strip()
    if url.startswith(('http://', 'https://')):
        return url
    if url.startswith('www.'):
        return f'https://{url}'
    if '.' in url:
        return f'https://{url}'
    return None

def normalize_root(url):
    """extract root domain"""
    u = normalize_url(url)
    if not u:
        return None
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}".lower()

def is_iranian_domain(url):
    """تشخیص دامنه ایرانی"""
    try:
        netloc = urlparse(normalize_root(url)).netloc.lower()
        return any(netloc.endswith(tld) for tld in IRANIAN_TLDS)
    except:
        return False

def domain_exists(url):
    """بررسی وجود دامنه"""
    try:
        host = urlparse(normalize_root(url)).netloc
        socket.gethostbyname(host)
        return True
    except:
        return False

def are_values_same(v1, v2):
    """بررسی یکسان بودن دو مقدار"""
    if not v1 or not v2:
        return False
    return str(v1).strip().lower() == str(v2).strip().lower()