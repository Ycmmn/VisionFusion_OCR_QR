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

import sys
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# =========================================================
# Gemini SDK Import (Fixed)
# =========================================================
try:
    import google.genai as genai
    from google.genai import types
    print("Gemini SDK loaded successfully (google-genai).")
except ImportError:
    try:
        import google.genai as genai
        from google.genai import types
        print("Using legacy google-generativeai SDK.")
    except Exception as e:
        print("Gemini SDK not installed properly:", e)
        import sys
        sys.exit(1)

# =========================================================
# Dynamic session paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))
OUT_JSON = Path(os.getenv("OUT_JSON", SESSION_DIR / "gemini_scrap_output.json"))
QR_RAW_JSON = Path(os.getenv("QR_RAW_JSON", SESSION_DIR / "final_superqr_v6_raw.json"))
QR_CLEAN_JSON = Path(os.getenv("QR_CLEAN_JSON", SESSION_DIR / "final_superqr_v6_clean.json"))
MIX_OCR_QR_JSON = Path(os.getenv("MIX_OCR_QR_JSON", SESSION_DIR / "mix_ocr_qr.json"))
WEB_ANALYSIS_XLSX = Path(os.getenv("WEB_ANALYSIS_XLSX", SESSION_DIR / "web_analysis.xlsx"))

# Configuration
FALLBACK_CHAIN = [
    {"key": "AI*********************************jpHQ", "model": "gemini-3-flash-preview"},  # A + lite
    {"key": "AIza*****************************nKTag", "model": "gemini-3-flash-preview"},  # B + lite


    {"key": "AI*****************************rzEkw", "model": "gemini-3.1-flash-lite"},       # A + flash
    {"key": "AIz*****************************qf_8xio", "model": "gemini-3.1-flash-lite"},       # B + flash
  
]
_current_slot = 0
_slot_lock = threading.Lock()

# limit the number of concurrent calls to the Gemini API
_GEMINI_CONCURRENCY = 4
_gemini_semaphore = threading.Semaphore(_GEMINI_CONCURRENCY)


def get_current_client():
    entry = FALLBACK_CHAIN[_current_slot]
    return genai.Client(api_key=entry["key"]), entry["model"]




THREAD_COUNT = 5
MAX_DEPTH = 1
MAX_PAGES_PER_SITE = 8
REQUEST_TIMEOUT = (10, 20)
SLEEP_BETWEEN = (0.3, 0.7)
MAX_RETRIES_HTTP = 1
MAX_RETRIES_GEMINI = 2
CHECK_DOMAIN_EXISTENCE = True

IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']

lock = threading.Lock()

# =========================================================
# Dynamic input and output paths
# =========================================================
RAW_INPUT = MIX_OCR_QR_JSON
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
    ("Markets", "MarketsFA"),
    ("Brands", "BrandsFA"),
    ("Industry", "IndustryFA"),
    ("Certifications", "CertificationsFA"),
    ("Country", "CountryFA"),
    ("City", "CityFA"),
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
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
    try:
        netloc = urlparse(normalize_root(url)).netloc.lower()
        return any(netloc.endswith(tld) for tld in IRANIAN_TLDS)
    except:
        return False

# ===================
def normalize_url_for_dedup(url: str) -> str:
    """
    Normalize URL to remove duplicates
    Example: https://www.nivan-sa.com -> nivan-sa.com
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
        print(f"Domain check failed for {url}: {e}")
        return False

# =============================================================
# Extract URLs (from OCR + QR + Excel) - FIXED
# =============================================================
def extract_urls_from_mix(input_path: str, output_path: str):
    print("Extracting scrapable URLs from mix_ocr_qr.json...")
    try:
        raw = json.loads(Path(input_path).read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error reading input JSON: {e}")
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
    # ==========  Remove duplicate URLs ==========
    print(f"\nRemoving duplicate URLs...")
    unique_urls = {}

    for url in urls:
        normalized = normalize_url_for_dedup(url)
        
        if normalized and normalized not in unique_urls:
            unique_urls[normalized] = url

    print(f"   Total URLs found: {len(urls)}")
    print(f"   Unique URLs after deduplication: {len(unique_urls)}")
    print(f"   Duplicates removed: {len(urls) - len(unique_urls)}")

    roots = sorted(unique_urls.values())
        
        
    if CHECK_DOMAIN_EXISTENCE:
        print(f"Checking domain existence for {len(roots)} URLs...")
        valid_roots = []
        for u in roots:
            if domain_exists(u):
                valid_roots.append(u)
            else:
                print(f"  Domain not found: {u}")
        roots = valid_roots

    Path(output_path).write_text(
        json.dumps(roots, ensure_ascii=False, indent=2), 
        encoding="utf-8"
    )
    
    print(f"\n{'='*60}")
    print("URL Extraction Summary:")
    print(f"{'='*60}")
    print(f"  Direct URLs field: {stats['direct_urls']}")
    print(f"  OCR URLs extracted: {stats['ocr']}")
    print(f"  QR URLs extracted: {stats['qr']}")
    print(f"  Excel URLs extracted: {stats['excel']}")
    print(f"  Social media excluded: {stats['social_excluded']}")
    print(f"  Files excluded: {stats['file_excluded']}")
    print(f"  Total scrapable URLs: {len(roots)}")
    print(f"{'='*60}\n")
    
    return roots

# =============================================================
# Web Crawling & Cleaning (FIXED)
# =============================================================

def fetch(url: str) -> tuple[str, str]:
    verify_ssl = False

    # ========== Step 1: without VPN (direct) ==========
    try:
        print(f"  Trying direct (no VPN): {url}")
        session_direct = requests.Session()
        session_direct.trust_env = False
        r = session_direct.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            verify=verify_ssl,
            allow_redirects=True
        )
        if r.status_code == 200:
            print(f"  Success (direct): {url}")
            return (r.text, "")
        else:
            print(f"  Status {r.status_code} (direct): {url}")
    except requests.exceptions.Timeout:
        print(f"  Timeout (direct): {url}")
    except requests.exceptions.ConnectionError:
        print(f"  Connection Error (direct): {url}")
    except Exception as e:
        print(f"  Error (direct): {str(e)[:80]}")

    


    # ========== Step 2.5: Wayback Machine ==========
   
    # ========== Step 3: with VPN ==========
    print(f"  Retrying with VPN: {url}")
    for i in range(MAX_RETRIES_HTTP):
        try:
            print(f"  Attempt {i+1}/{MAX_RETRIES_HTTP} [VPN]: {url}")
            r = requests.get(
                url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
                verify=verify_ssl,
                allow_redirects=True
            )
            if r.status_code == 200:
                print(f"  Success (VPN): {url}")
                return (r.text, "")
            else:
                print(f"  Status {r.status_code} (VPN): {url}")
                if i == MAX_RETRIES_HTTP - 1:
                    return ("", f"HTTP_{r.status_code}")
        except requests.exceptions.Timeout:
            print(f"  Timeout (VPN): {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "TIMEOUT")
        except requests.exceptions.ConnectionError:
            print(f"  Connection Error (VPN): {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "CONNECTION_ERROR")
        except Exception as e:
            print(f"  Error (VPN): {str(e)[:100]}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", f"ERROR: {str(e)[:50]}")

        time.sleep(2.0 * (i + 1))

    return ("", "MAX_RETRIES_EXCEEDED")

def clean_text(html: str) -> str:
    if not html: return ""
    
    soup = BeautifulSoup(html, "html.parser")
    
    # remove extra tags
    for t in soup(["script","style","noscript","iframe","svg"]): 
        t.extract()
    
    # main text
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    
    # if the text is too short, also read from attrs
    if len(text) < 100:
        extra = []
        for tag in soup.find_all(True):
            for attr in ['alt', 'title', 'placeholder', 'content']:
                val = tag.get(attr, '')
                if val and len(val) > 5:
                    extra.append(val)
        text = text + " " + " ".join(extra)
        text = re.sub(r"\s+", " ", text).strip()
    
    return text

def fetch_via_google_search(url: str) -> str:
    """If the site was empty, get information from DuckDuckGo"""
    try:
        domain = urlparse(url).netloc.replace('www.', '')
        
        # method 1: DuckDuckGo
        search_url = f"https://html.duckduckgo.com/html/?q=site:{domain}"
        
        session = requests.Session()
        session.trust_env = False
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "fa-IR,fa;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        
        r = session.get(search_url, headers=headers, timeout=(10, 30), verify=False)
        
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            texts = []
            
            # DuckDuckGo snippets
            for div in soup.find_all('a', class_='result__snippet'):
                t = div.get_text(" ", strip=True)
                if len(t) > 30:
                    texts.append(t)
            
            # if there were no snippets, use the whole text
            if not texts:
                for div in soup.find_all(['div', 'span']):
                    t = div.get_text(" ", strip=True)
                    if len(t) > 50:
                        texts.append(t)
            
            result = " ".join(texts[:20])
            result = re.sub(r"\s+", " ", result).strip()
            
            if len(result) > 100:
                print(f"  Got info from DuckDuckGo: {len(result)} chars")
                return result
        
        # method 2: Wayback Machine
        print(f"  Trying Wayback Machine...")
        wayback_url = f"https://web.archive.org/web/2024/{url}"
        
        r2 = session.get(wayback_url, headers=headers, timeout=(15, 45), verify=False)
        
        if r2.status_code == 200:
            soup2 = BeautifulSoup(r2.text, "html.parser")
            for t in soup2(["script","style","noscript","iframe"]): 
                t.extract()
            text2 = soup2.get_text(" ", strip=True)
            text2 = re.sub(r"\s+", " ", text2).strip()
            
            if len(text2) > 100:
                print(f"  Got info from Wayback Machine: {len(text2)} chars")
                return text2
    
    except Exception as e:
        print(f"  Search failed: {str(e)[:80]}")
    
    return ""

def crawl_site(root: str, max_depth=MAX_DEPTH, max_pages=MAX_PAGES_PER_SITE) -> tuple[str, str]:
    print(f"\nStarting crawl: {root}")
    seen = set()
    q = [(root, 0)]
    texts = []
    errors = []
    
    while q and len(seen) < max_pages:
        url, d = q.pop(0)
        if url in seen or d > max_depth: continue
        seen.add(url)
        
        html, error = fetch(url)
        
        if error:
            errors.append(f"{url}: {error}")
            continue
            
        txt = clean_text(html)
        if txt:
            texts.append(txt[:40000])
            print(f"  Extracted {len(txt)} chars from {url}")
        
        else:
            errors.append(f"{url}: EMPTY_CONTENT")
        
        if html:
            PRIORITY_KEYWORDS = ["about", "contact", "درباره", "تماس", "محصول", "product", "company", "شرکت"]
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                nxt = urljoin(root, a["href"])
                link_text = (a.get_text() + a["href"]).lower()
                is_priority = any(kw in link_text for kw in PRIORITY_KEYWORDS)
                if nxt.startswith(root) and nxt not in seen and len(seen) < max_pages:
                    if is_priority or d == 0:
                        q.append((nxt, d+1))
        
        time.sleep(random.uniform(*SLEEP_BETWEEN))
    
    combined = "\n".join(texts)[:180000]
    
    if not combined:
        error_summary = "; ".join(errors[:3])
        print(f"  No content extracted from {root}")
        return ("", error_summary or "NO_CONTENT")
    
    print(f"  Total extracted: {len(combined)} chars from {len(texts)} pages")
    return (combined, "")

# =============================================================
# Gemini + Translation
# =============================================================


_TRANSIENT_ERROR_MARKERS = [
    "10053", "10054", "10060", "10061",
    "connectionaborted", "connectionreset", "connectionrefused",
    "remotedisconnected", "brokenpipe", "broken pipe",
    "connection error", "connectionerror",
    "eof occurred", "ssl", "handshake",
    "read timed out", "timeout", "timed out",
    "temporarily unavailable", "name resolution",
]


def _is_transient_error(err: str) -> bool:
    e = err.lower()
    return any(marker in e for marker in _TRANSIENT_ERROR_MARKERS)


def _is_quota_error(err: str) -> bool:
    return ("429" in err or "RESOURCE_EXHAUSTED" in err
            or "503" in err or "UNAVAILABLE" in err)


def gemini_json(prompt: str, schema: dict):
    global _current_slot
    schema_obj = types.Schema(type=types.Type.OBJECT, properties=schema, required=[])

    while True:
        with _slot_lock:
            if _current_slot >= len(FALLBACK_CHAIN):
                print("All keys have run out.")
                return {}
            slot_num = _current_slot + 1

        _client, _model = get_current_client()
        switched = False

        for attempt in range(1, MAX_RETRIES_GEMINI + 1):
            try:
                print(f"  [Slot {slot_num}/{len(FALLBACK_CHAIN)}] model={_model} | attempt {attempt}/{MAX_RETRIES_GEMINI}")
                with _gemini_semaphore:
                    resp = _client.models.generate_content(
                        model=_model,
                        contents=[types.Part(text=prompt)],
                        config=types.GenerateContentConfig(
                            temperature=0.1,
                            response_mime_type="application/json",
                            response_schema=schema_obj
                        )
                    )
                print(f"      RAW RESPONSE: {resp.text[:300]!r}")
                return json.loads(resp.text)

            except Exception as e:
                err = str(e)

                if _is_quota_error(err):
                    print(f"  Slot {slot_num} exhausted -> switching key...")
                    with _slot_lock:
                        if _current_slot < len(FALLBACK_CHAIN) - 1:
                            _current_slot += 1
                        else:
                            print("All keys have run out.")
                            return {}
                    time.sleep(2)
                    switched = True
                    break

                if _is_transient_error(err):
                    if attempt < MAX_RETRIES_GEMINI:
                        wait = (2 ** attempt) + random.uniform(0, 1)
                        print(f"  Transient network error (attempt {attempt}/{MAX_RETRIES_GEMINI}): "
                              f"{err[:120]} -> retry in {wait:.1f}s")
                        time.sleep(wait)
                        continue
                    else:
                        print(f"  Transient error persisted after {MAX_RETRIES_GEMINI} attempts: {err[:120]}")
                        return {}

                print(f"  Gemini error: {err[:150]}")
                return {}

        if switched:
            continue

        return {}



def _call_gemini_raw(prompt: str):
    global _current_slot

    while True:
        with _slot_lock:
            if _current_slot >= len(FALLBACK_CHAIN):
                return None
            slot_num = _current_slot + 1

        _client, _model = get_current_client()
        switched = False

        for attempt in range(1, MAX_RETRIES_GEMINI + 1):
            try:
                with _gemini_semaphore:
                    resp = _client.models.generate_content(
                        model=_model,
                        contents=[types.Part(text=prompt)],
                        config=types.GenerateContentConfig(temperature=0.1)
                    )
                return resp.text

            except Exception as e:
                err = str(e)

                if _is_quota_error(err):
                    print(f"  Slot {slot_num} exhausted -> switching key...")
                    with _slot_lock:
                        if _current_slot < len(FALLBACK_CHAIN) - 1:
                            _current_slot += 1
                        else:
                            return None
                    time.sleep(2)
                    switched = True
                    break

                if _is_transient_error(err):
                    if attempt < MAX_RETRIES_GEMINI:
                        wait = (2 ** attempt) + random.uniform(0, 1)
                        print(f"      Transient network error (attempt {attempt}/{MAX_RETRIES_GEMINI}): "
                              f"{err[:120]} -> retry in {wait:.1f}s")
                        time.sleep(wait)
                        continue
                    else:
                        print(f"      Transient error persisted: {err[:120]}")
                        return None

                print(f"      Gemini error: {err[:120]}")
                return None

        if switched:
            continue

        return None




def extract_with_gemini(text: str):
    fields = "\n".join([f"- {f}" for f in FIELDS])
    prompt = PROMPT_EXTRACT.format(fields=fields, text=text)
    schema = {f: types.Schema(type=types.Type.STRING, nullable=True) for f in FIELDS}
    data = gemini_json(prompt, schema)
    return {f: (data.get(f) or "") for f in FIELDS}

def translate_fields(data):
    SUMMARIZE_FIELDS = ["Description", "Applications", "History"]
    
    to_translate = {}
    
    for en, fa_col in TRANSLATABLE_FIELDS:
        if fa_col not in data:
            data[fa_col] = ""
        
        val = data.get(en)
        if val and str(val).strip():
            if en in SUMMARIZE_FIELDS and len(str(val)) > 500:
                print(f"      Summarizing {en} ({len(str(val))} chars)...")
                
                if en == "History":
                    prompt = f"""Summarize this company history in 2-3 concise sentences (max 150 words), then translate to formal Persian.

English text:
{val}

Format:
English Summary: [your summary]
Persian Translation: [ترجمه فارسی]"""
                
                elif en == "Description":
                    prompt = f"""Summarize this company description in 2-3 concise sentences (max 150 words), then translate to formal Persian.

English text:
{val}

Format:
English Summary: [your summary]
Persian Translation: [ترجمه فارسی]"""
                
                elif en == "Applications":
                    prompt = f"""List main applications in bullet points (max 100 words), then translate to formal Persian.

English text:
{val}

Format:
English Summary: [your summary]
Persian Translation: [ترجمه فارسی]"""
                
                

                result = _call_gemini_raw(prompt)

                if result:
                    result = result.strip()
                    if "Persian Translation:" in result:
                        fa_text = result.split("Persian Translation:")[1].strip()
                        data[fa_col] = fa_text
                        print(f"      {en} summarized & translated")
                    else:
                        data[fa_col] = result
                        print(f"      {en} processed")
                    time.sleep(1.5)
                else:
                    print(f"      Failed to summarize {en} (Gemini returned no result)")
                    data[fa_col] = ""


            else:
                to_translate[en] = str(val)
    
    if to_translate:
        prompt = PROMPT_TRANSLATE_EN2FA.format(json_chunk=json.dumps(to_translate, ensure_ascii=False))
        schema = {k: types.Schema(type=types.Type.STRING, nullable=True) for k in to_translate.keys()}
        tr = gemini_json(prompt, schema)
        
        for en, fa_col in TRANSLATABLE_FIELDS:
            if en in tr and tr[en]:
                data[fa_col] = tr[en]
    
    return data

# =============================================================
# Worker & Main (FIXED)
# =============================================================
def worker(q: Queue, results: list):
    while True:
        try:
            root = q.get_nowait()
        except:
            break
        
        try:
            print(f"\n{'='*60}")
            print(f"Processing: {root}")
            print(f"{'='*60}")
            
            text, error = crawl_site(root)
            
            
            if error or not text:
                data = {
                    "url": root, 
                    "error": error or "NO_CONTENT",
                    "status": "FAILED"
                }
                print(f"Failed: {root} - {error or 'NO_CONTENT'}")
            else:
                print(f"Analyzing with Gemini: {root}")
                data = extract_with_gemini(text)

                has_any_value = any(str(v).strip() for v in data.values())

                if not has_any_value:
                    data["url"] = root
                    data["status"] = "FAILED"
                    data["error"] = "GEMINI_EMPTY_RESPONSE"
                    print(f"Failed: {root} - GEMINI_EMPTY_RESPONSE")
                else:
                    data = translate_fields(data)
                    data["url"] = root
                    data["status"] = "SUCCESS"
                    data["error"] = ""
                    print(f"Success: {root}")
                
        except Exception as e:
            data = {
                "url": root, 
                "error": f"EXCEPTION: {str(e)[:100]}",
                "status": "EXCEPTION"
            }
            print(f"Exception for {root}: {str(e)[:100]}")
        
        with lock:
            results.append(data)
            try:
                Path(OUTPUT_JSON).write_text(
                    json.dumps(results, ensure_ascii=False, indent=2), 
                    encoding="utf-8"
                )
            except Exception as e:
                print(f"Failed to save JSON: {e}")
        
        q.task_done()
        time.sleep(random.uniform(*SLEEP_BETWEEN))

def main():
    print("\n" + "="*60)
    print("Starting Web Scraping Process")
    print("="*60 + "\n")
    
    roots = extract_urls_from_mix(RAW_INPUT, CLEAN_URLS)
    if not roots:
        print("No URLs found.")
        return

    results = []
    q = Queue()
    for r in roots: q.put(r)

    threads = []
    for _ in range(min(THREAD_COUNT, len(roots))):
        t = threading.Thread(target=worker, args=(q, results), daemon=True)
        t.start()
        threads.append(t)
    
    for t in threads: t.join()

    print("\n" + "="*60)
    print("Creating Excel Report")
    print("="*60 + "\n")

    df = pd.DataFrame(results)
    
    ordered_cols = ["url", "status", "error"]
    
    for field in FIELDS:
        ordered_cols.append(field)
        for en_field, fa_field in TRANSLATABLE_FIELDS:
            if en_field == field:
                ordered_cols.append(fa_field)
                break
    
    for en_field, fa_field in TRANSLATABLE_FIELDS:
        if en_field not in FIELDS and en_field not in ordered_cols:
            ordered_cols.append(en_field)
            ordered_cols.append(fa_field)
    
    for col in ordered_cols:
        if col not in df.columns:
            df[col] = ""
    
    df = df[ordered_cols]
    
    try:
        tmp = TEMP_EXCEL
        df.to_excel(tmp, index=False)
        shutil.move(tmp, OUTPUT_EXCEL)
        print(f"Excel saved: {OUTPUT_EXCEL}")
    except Exception as e:
        print(f"Failed to save Excel: {e}")
    
    success = len([r for r in results if r.get("status") == "SUCCESS"])
    failed = len([r for r in results if r.get("status") != "SUCCESS"])
    
    print("\n" + "="*60)
    print(f"Success: {success}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()