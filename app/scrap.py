from pathlib import Path
import os, re, json, time, random, threading, socket, shutil
from queue import Queue
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# 🔹 Gemini SDK Import (Fixed)
# =========================================================
try:
    import google.genai as genai
    from google.genai import types
    print("✅ Gemini SDK loaded successfully (google-genai).")
except ImportError:
    try:
        import google.genai as genai
        from google.genai import types
        print("⚠️ Using legacy google-generativeai SDK.")
    except Exception as e:
        print("❌ Gemini SDK not installed properly:", e)
        import sys
        sys.exit(1)

# =========================================================
# dynamic session paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))
OUT_JSON = Path(os.getenv("OUT_JSON", SESSION_DIR / "gemini_scrap_output.json"))
QR_RAW_JSON = Path(os.getenv("QR_RAW_JSON", SESSION_DIR / "final_superqr_v6_raw.json"))
QR_CLEAN_JSON = Path(os.getenv("QR_CLEAN_JSON", SESSION_DIR / "final_superqr_v6_clean.json"))
MIX_OCR_QR_JSON = Path(os.getenv("MIX_OCR_QR_JSON", SESSION_DIR / "mix_ocr_qr.json"))
WEB_ANALYSIS_XLSX = Path(os.getenv("WEB_ANALYSIS_XLSX", SESSION_DIR / "web_analysis.xlsx"))



#  Configuration
GOOGLE_API_KEY = "AIzaSyAhuC9Grg_FlxwDwYUW-_CpNaFzjwUg24w"
MODEL_NAME = "gemini-2.5-flash"

THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3  #  increase retry attempts
MAX_RETRIES_GEMINI = 3
CHECK_DOMAIN_EXISTENCE = True

# list of Iranian domains that may have SSL issues
IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']

client = genai.Client(api_key=GOOGLE_API_KEY)
lock = threading.Lock()


# =========================================================
#  dynamic input and output paths
# =========================================================
RAW_INPUT = Path(os.getenv("RAW_INPUT", MIX_OCR_QR_JSON))
CLEAN_URLS = Path(os.getenv("CLEAN_URLS", SESSION_DIR / "urls_clean.json"))
OUTPUT_JSON = Path(os.getenv("OUTPUT_JSON", OUT_JSON))
OUTPUT_EXCEL = Path(os.getenv("OUTPUT_EXCEL", WEB_ANALYSIS_XLSX))
TEMP_EXCEL = Path(os.getenv("TEMP_EXCEL", SESSION_DIR / "web_analysis.tmp.xlsx"))

# ---------------------------------------------
# Fields & Prompts
FIELDS = [
    "CompanyNameEN", "CompanyNameFA", "Logo", "Industry", "Certifications",
    "ContactName", "PositionEN", "PositionFA", "Department",
    "Phone1", "Phone2", "Fax", "WhatsApp", "Telegram", "Instagram", "LinkedIn",
    "Website", "Email", "OtherEmails",
    "AddressEN", "AddressFA", "Country", "City",
    "ProductName", "ProductCategory", "ProductDescription", "Applications",
    "Brands", "Description", "History", "Employees", "ClientsPartners", "Markets"
]

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

PROMPT_EXTRACT = """
You are a bilingual (Persian-English) company information extractor.
Extract the following JSON fields from the provided website text.
Return ONLY strict JSON object. If a field has no value, return null.

Fields:
{fields}

Website text (mixed FA/EN):
---
{text}
---
"""

PROMPT_TRANSLATE_EN2FA = """
Translate the following English fields into formal Persian.
Return ONLY valid JSON with the same keys and Persian values. Do NOT add extra text.

Fields JSON:
{json_chunk}
"""

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


# =============================================================
# Utility Functions
# =============================================================
def normalize_root(url: str) -> str:
    u = url.strip()
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}".lower()

def is_iranian_domain(url: str) -> bool:
    """Check if URL is Iranian domain"""
    try:
        netloc = urlparse(normalize_root(url)).netloc.lower()
        return any(netloc.endswith(tld) for tld in IRANIAN_TLDS)
    except:
        return False

def domain_exists(url: str) -> bool:
    try:
        host = urlparse(normalize_root(url)).netloc
        socket.gethostbyname(host)
        return True
    except Exception as e:
        print(f"❌ Domain check failed for {url}: {e}")
        return False
