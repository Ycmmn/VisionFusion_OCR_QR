# -*- coding: utf-8 -*-
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
# gemini sdk import (fixed)
# =========================================================
try:
    import google.genai as genai
    from google.genai import types
    print("gemini sdk loaded successfully (google-genai).")
except ImportError:
    try:
        import google.genai as genai
        from google.genai import types
        print("using legacy google-generativeai sdk.")
    except Exception as e:
        print("gemini sdk not installed properly:", e)
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

# configuration
GOOGLE_API_KEY = "AIzaSyDMUEVEqDCQpahoyIeXLN0UJ4IKNNPzB70"
MODEL_NAME = "gemini-2.5-flash"

THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3
MAX_RETRIES_GEMINI = 3
CHECK_DOMAIN_EXISTENCE = True

IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']

client = genai.Client(api_key=GOOGLE_API_KEY)
lock = threading.Lock()

# =========================================================
# dynamic input and output paths
# =========================================================
RAW_INPUT = MIX_OCR_QR_JSON
CLEAN_URLS = Path(os.getenv("CLEAN_URLS", SESSION_DIR / "urls_clean.json"))
OUTPUT_JSON = Path(os.getenv("OUTPUT_JSON", OUT_JSON))
OUTPUT_EXCEL = Path(os.getenv("OUTPUT_EXCEL", WEB_ANALYSIS_XLSX))
TEMP_EXCEL = Path(os.getenv("TEMP_EXCEL", SESSION_DIR / "web_analysis.tmp.xlsx"))

# ---------------------------------------------
# fields & prompts
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
    ("Markets", "MarketsFA"),
    ("Brands", "BrandsFA"),
    ("Industry", "IndustryFA"),
    ("Certifications", "CertificationsFA"),
    ("Country", "CountryFA"),
    ("City", "CityFA"),
]

PROMPT_EXTRACT = """
you are a bilingual (persian-english) company information extractor.
extract the following json fields from the provided website text.
return only strict json object. if a field has no value, return null.

fields:
{fields}

website text (mixed fa/en):
---
{text}
---
"""

PROMPT_TRANSLATE_EN2FA = """
translate the following english fields into formal persian.
return only valid json with the same keys and persian values. do not add extra text.

fields json:
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
# utility functions
# =============================================================
def normalize_root(url: str) -> str:
    u = url.strip()
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}".lower()

def is_iranian_domain(url: str) -> bool:
    try:
        netloc = urlparse(normalize_root(url)).netloc.lower()
        return any(netloc.endswith(tld) for tld in IRANIAN_TLDS)
    except:
        return False

# ===================
def normalize_url_for_dedup(url: str) -> str:
    """
    normalize url for duplicate removal
    example: https://www.nivan-sa.com → nivan-sa.com
    """
    if not url or not isinstance(url, str):
        return ""
    
    url = url.strip().lower()
    
    # remove protocol
    url = url.replace('https://', '').replace('http://', '')
    
    # remove www
    url = url.replace('www.', '')
    
    # remove trailing slash
    url = url.rstrip('/')
    
    # remove query and fragment
    url = url.split('?')[0].split('#')[0]
    
    return url
# ================================================


def domain_exists(url: str) -> bool:
    try:
        host = urlparse(normalize_root(url)).netloc
        socket.gethostbyname(host)
        return True
    except Exception as e:
        print(f"domain check failed for {url}: {e}")
        return False

# =============================================================
# extract urls (from ocr + qr + excel) - fixed
# =============================================================
def extract_urls_from_mix(input_path: str, output_path: str):
    print("extracting scrapable urls from mix_ocr_qr.json...")
    try:
        raw = json.loads(Path(input_path).read_text(encoding="utf-8"))
    except Exception as e:
        print(f"error reading input json: {e}")
        return []

    urls = set()
    
    SOCIAL_EXCLUDE = ("instagram.com", "linkedin.com", "twitter.com", "x.com",
                      "facebook.com", "t.me", "wa.me", "youtube.com", 
                      "gmail.com", "yahoo.com", "hotmail.com", "mail.")
    
    FILE_EXCLUDE = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf", ".zip", 
                    ".rar", ".xls", ".xlsx", ".doc", ".docx", ".mp4", ".mp3")
    
    url_pattern = re.compile(r"(https?://[^\s\"'<>]+|www\.[^\s\"'<>]+)", re.I)
    domain_pattern = re.compile(r"^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$", re.I)
    
    stats = {"ocr": 0, "qr": 0, "excel": 0, "direct_urls": 0, "social_excluded": 0, "file_excluded": 0}

    def is_scrapable_url(url_str: str) -> bool:
        if not url_str:
            return False
        
        if any(url_str.lower().endswith(ext) for ext in FILE_EXCLUDE):
            stats["file_excluded"] += 1
            return False
        
        if any(social in url_str.lower() for social in SOCIAL_EXCLUDE):
            stats["social_excluded"] += 1
            return False
        
        return True

    def add_url(url_str: str, source: str):
        if not url_str or not isinstance(url_str, str):
            return
        
        url_str = url_str.strip()
        
        if not url_str:
            return
        
        if not url_str.lower().startswith("http"):
            url_str = "https://" + url_str
        
        if not is_scrapable_url(url_str):
            return
        
        r = normalize_root(url_str)
        if r:
            urls.add(r)
            stats[source] += 1

    def collect(obj, source="ocr"):
        if isinstance(obj, str):
            for m in url_pattern.findall(obj):
                add_url(m, source)
                    
        elif isinstance(obj, list):
            for v in obj:
                if isinstance(v, str):
                    v_stripped = v.strip()
                    if domain_pattern.match(v_stripped):
                        add_url(v_stripped, "direct_urls")
                    else:
                        collect(v, source)
                else:
                    collect(v, source)
                
        elif isinstance(obj, dict):
            if "urls" in obj and obj["urls"]:
                url_list = obj["urls"] if isinstance(obj["urls"], list) else [obj["urls"]]
                for url in url_list:
                    if url:
                        add_url(url, "direct_urls")
            
            for k, v in obj.items():
                current_source = source
                if "qr" in k.lower():
                    current_source = "qr"
                elif "excel" in k.lower():
                    current_source = "excel"
                
                if k == "raw_excel_data":
                    sheets = v.get("sheets", [])
                    for sh in sheets:
                        for row in sh.get("data", []):
                            for val in row.values():
                                collect(val, "excel")
                elif k != "urls":
                    collect(v, current_source)

    collect(raw, source="ocr")
    #
    # ==========  remove duplicate urls ==========
    print(f"\nremoving duplicate urls...")
    unique_urls = {}

    for url in urls:
        normalized = normalize_url_for_dedup(url)
        
        if normalized and normalized not in unique_urls:
            unique_urls[normalized] = url

    print(f"   total urls found: {len(urls)}")
    print(f"   unique urls after deduplication: {len(unique_urls)}")
    print(f"   duplicates removed: {len(urls) - len(unique_urls)}")

    roots = sorted(unique_urls.values())
        
        
    if CHECK_DOMAIN_EXISTENCE:
        print(f"checking domain existence for {len(roots)} urls...")
        valid_roots = []
        for u in roots:
            if domain_exists(u):
                valid_roots.append(u)
            else:
                print(f"  domain not found: {u}")
        roots = valid_roots

    Path(output_path).write_text(
        json.dumps(roots, ensure_ascii=False, indent=2), 
        encoding="utf-8"
    )
    
    print(f"\n{'='*60}")
    print("url extraction summary:")
    print(f"{'='*60}")
    print(f"  direct urls field: {stats['direct_urls']}")
    print(f"  ocr urls extracted: {stats['ocr']}")
    print(f"  qr urls extracted: {stats['qr']}")
    print(f"  excel urls extracted: {stats['excel']}")
    print(f"  social media excluded: {stats['social_excluded']}")
    print(f"  files excluded: {stats['file_excluded']}")
    print(f"  total scrapable urls: {len(roots)}")
    print(f"{'='*60}\n")
    
    return roots

# =============================================================
# web crawling & cleaning (fixed)
# =============================================================
def fetch(url: str) -> tuple[str, str]:
    verify_ssl = not is_iranian_domain(url)
    ssl_status = "ssl on" if verify_ssl else "ssl off (iranian)"
    
    for i in range(MAX_RETRIES_HTTP):
        try:
            print(f"  attempt {i+1}/{MAX_RETRIES_HTTP} [{ssl_status}]: {url}")
            r = requests.get(
                url, 
                headers=HEADERS, 
                timeout=REQUEST_TIMEOUT, 
                verify=verify_ssl,
                allow_redirects=True
            )
            if r.status_code == 200:
                print(f"  success: {url}")
                return (r.text, "")
            else:
                print(f"  status {r.status_code}: {url}")
                if i == MAX_RETRIES_HTTP - 1:
                    return ("", f"HTTP_{r.status_code}")
        except requests.exceptions.SSLError as e:
            if verify_ssl and i == 0:
                print(f"   ssl error, retrying without verification: {url}")
                try:
                    r = requests.get(
                        url, 
                        headers=HEADERS, 
                        timeout=REQUEST_TIMEOUT, 
                        verify=False,
                        allow_redirects=True
                    )
                    if r.status_code == 200:
                        print(f"  success (ssl disabled): {url}")
                        return (r.text, "")
                except:
                    pass
            print(f"  ssl error: {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "SSL_ERROR")
        except requests.exceptions.Timeout:
            print(f"  timeout: {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "TIMEOUT")
        except requests.exceptions.ConnectionError:
            print(f"  connection error: {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "CONNECTION_ERROR")
        except Exception as e:
            print(f"  error: {url} - {str(e)[:100]}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", f"ERROR: {str(e)[:50]}")
        
        time.sleep(2.0 * (i + 1))
    
    return ("", "MAX_RETRIES_EXCEEDED")

def clean_text(html: str) -> str:
    if not html: return ""
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script","style","noscript","iframe","svg"]): t.extract()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()