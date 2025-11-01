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
    """detect Iranian domain"""
    try:
        netloc = urlparse(normalize_root(url)).netloc.lower()
        return any(netloc.endswith(tld) for tld in IRANIAN_TLDS)
    except:
        return False

def domain_exists(url):
    """check domain existence"""
    try:
        host = urlparse(normalize_root(url)).netloc
        socket.gethostbyname(host)
        return True
    except:
        return False

def are_values_same(v1, v2):
    """check if two values are identical"""
    if not v1 or not v2:
        return False
    return str(v1).strip().lower() == str(v2).strip().lower()

# =========================================================
# web scraping with smart ssl
# =========================================================
def fetch(url):
    """fetch page content with smart ssl handling"""
    verify_ssl = not is_iranian_domain(url)
    ssl_status = "🔒 SSL ON" if verify_ssl else "🔓 SSL OFF (Iranian)"
    
    for i in range(MAX_RETRIES_HTTP):
        try:
            print(f"      🔄 Attempt {i+1}/{MAX_RETRIES_HTTP} [{ssl_status}]")
            r = requests.get(
                url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
                verify=verify_ssl,
                allow_redirects=True
            )
            if r.status_code == 200:
                return (r.text, "")
            else:
                if i == MAX_RETRIES_HTTP - 1:
                    return ("", f"HTTP_{r.status_code}")
        except requests.exceptions.SSLError:
            if verify_ssl and i == 0:
                try:
                    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, 
                                   verify=False, allow_redirects=True)
                    if r.status_code == 200:
                        return (r.text, "")
                except:
                    pass
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "SSL_ERROR")
        except requests.exceptions.Timeout:
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "TIMEOUT")
        except requests.exceptions.ConnectionError:
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "CONNECTION_ERROR")
        except Exception as e:
            if i == MAX_RETRIES_HTTP - 1:
                return ("", f"ERROR: {str(e)[:50]}")
        
        time.sleep(2.0 * (i + 1))
    
    return ("", "MAX_RETRIES")

def clean_text(html):
    """clean html and extract text"""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "nav", "footer"]):
        tag.extract()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()

def crawl_site(root):
    """full site crawl"""
    print(f"   🕷️ Crawling: {root}")
    seen = set()
    q = [(root, 0)]
    texts = []
    errors = []
    
    while q and len(seen) < MAX_PAGES_PER_SITE:
        url, depth = q.pop(0)
        if url in seen or depth > MAX_DEPTH:
            continue
        seen.add(url)
        
        html, error = fetch(url)
        
        if error:
            errors.append(f"{url}: {error}")
            continue
        
        txt = clean_text(html)
        if txt:
            texts.append(txt[:40000])
            print(f"      📄 Extracted {len(txt)} chars")
        
        if html and depth < MAX_DEPTH:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                next_url = urljoin(root, a["href"])
                if next_url.startswith(root) and next_url not in seen:
                    q.append((next_url, depth + 1))
        
        time.sleep(random.uniform(*SLEEP_BETWEEN))
    
    combined = "\n".join(texts)[:180000]
    
    if not combined:
        error_summary = "; ".join(errors[:3])
        return ("", error_summary or "NO_CONTENT")
    
    print(f"      ✅ Total: {len(combined)} chars from {len(texts)} pages")
    return (combined, "")

# =========================================================
# Gemini Extraction & Translation
# =========================================================
PROMPT_EXTRACT = """
You are a bilingual (Persian-English) company information extractor.
Extract the following JSON fields from the provided website text.
Return ONLY strict JSON object. If a field has no value, return empty string "".

Fields:
{fields}

Website text (mixed FA/EN):
---
{text}
---
"""

PROMPT_TRANSLATE_EN2FA = """
Translate the following English fields into formal Persian.
Return ONLY valid JSON with the same keys and Persian values.

Fields JSON:
{json_chunk}
"""

def gemini_json(prompt, schema):
    """send request to Gemini with JSON output"""
    schema_obj = types.Schema(type=types.Type.OBJECT, properties=schema, required=[])
    
    for i in range(MAX_RETRIES_GEMINI):
        try:
            resp = client.models.generate_content(
                model=MODEL_NAME,
                contents=[types.Part(text=prompt)],
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json",
                    response_schema=schema_obj
                )
            )
            return json.loads(resp.text)
        except Exception as e:
            print(f"      ⚠️ Gemini error (attempt {i+1}): {str(e)[:100]}")
            if i == MAX_RETRIES_GEMINI - 1:
                return {}
            time.sleep(2 * (i + 1))
    return {}

def extract_with_gemini(text):
    """extract information using Gemini"""
    fields = "\n".join([f"- {f}" for f in FIELDS])
    prompt = PROMPT_EXTRACT.format(fields=fields, text=text[:8000])
    schema = {f: types.Schema(type=types.Type.STRING, nullable=True) for f in FIELDS}
    data = gemini_json(prompt, schema)
    return {f: (data.get(f) or "") for f in FIELDS}

def translate_fields(data):
    """translate English fields to Farsi"""
    to_translate = {en: data.get(en) for en, _ in TRANSLATABLE_FIELDS if data.get(en)}
    
    # add empty FA columns
    for en, fa_col in TRANSLATABLE_FIELDS:
        if fa_col not in data:
            data[fa_col] = ""
    
    if not to_translate:
        return data
    
    prompt = PROMPT_TRANSLATE_EN2FA.format(json_chunk=json.dumps(to_translate, ensure_ascii=False))
    schema = {k: types.Schema(type=types.Type.STRING, nullable=True) for k in to_translate.keys()}
    tr = gemini_json(prompt, schema)
    
    for en, fa_col in TRANSLATABLE_FIELDS:
        if en in tr:
            data[fa_col] = tr[en] or ""
    
    return data

# =========================================================
#  Smart Merge 
# =========================================================
def clean_duplicate_columns(df):
    """remove and merge duplicate columns"""
    print("\n🧹 Cleaning duplicate columns...")
    
    # group columns based on main name
    base_cols = {}
    pattern = re.compile(r'\[\d+\]$')  # pattern [2], [3], ...
    
    for col in df.columns:
        # extract main name
        base = pattern.sub('', str(col))
        if base not in base_cols:
            base_cols[base] = []
        base_cols[base].append(col)
    
    cleaned_df = df.copy()
    
    # for each column group
    for base, cols in base_cols.items():
        if len(cols) <= 1:
            continue
        
        print(f"   🔄 Merging {len(cols)} versions of '{base}'")
        
        # merge all versions
        for idx in df.index:
            values = []
            for col in cols:
                try:
                    val = df.at[idx, col]
                    if val and not pd.isna(val) and str(val).strip() != "":
                        val_str = str(val).strip()
                        if val_str not in values:
                            values.append(val_str)
                except:
                    continue
            
            # merge with separator
            if values:
                if base in ['Phone1', 'Phone2', 'Email', 'OtherEmails', 'WhatsApp', 'Telegram']:
                    merged = ", ".join(values)
                elif base in ['ProductName', 'ProductCategory', 'Brands', 'Applications']:
                    merged = ", ".join(values)
                else:
                    if len(values) == 1:
                        merged = values[0]
                    else:
                        merged = f"{values[0]} | {' | '.join(values[1:])}"
                
                try:
                    cleaned_df.at[idx, base] = merged
                except:
                    pass
        
        # remove duplicate columns
        for col in cols[1:]:
            if col in cleaned_df.columns:
                try:
                    cleaned_df.drop(columns=[col], inplace=True)
                except:
                    pass
    
    print(f"   ✅ Reduced from {len(df.columns)} to {len(cleaned_df.columns)} columns")
    return cleaned_df

def smart_merge(original_df, scraped_data):
    """smart data merging"""
    print("\n🔗 Smart merging data...")
    
    scraped_df = pd.DataFrame(scraped_data)
    
    if scraped_df.empty:
        print("   ⚠️ No scraped data to merge")
        return original_df
    
    result_df = original_df.copy()
    
    for idx, row in result_df.iterrows():
        original_url = normalize_root(row.get('Website') or row.get('url') or row.get('URL'))
        
        if not original_url:
            continue
        
        scraped_row = scraped_df[scraped_df['url'] == original_url]
        
        if scraped_row.empty:
            continue
        
        scraped_row = scraped_row.iloc[0].to_dict()
        
        for col, new_val in scraped_row.items():
            if col in ['url', 'status', 'error']:
                continue
            
            if not new_val or pd.isna(new_val) or str(new_val).strip() == "":
                continue
            
            if col not in result_df.columns:
                result_df[col] = ""
            
            old_val = row.get(col)
            
            if not old_val or pd.isna(old_val) or str(old_val).strip() == "":
                result_df.at[idx, col] = new_val
                print(f"   ✏️ [{idx+1}] {col} = {str(new_val)[:50]}")
            elif not are_values_same(old_val, new_val):
                if col in ['Phone1', 'Phone2', 'Email', 'OtherEmails', 'ProductName', 'Brands']:
                    result_df.at[idx, col] = f"{old_val}, {new_val}"
                else:
                    result_df.at[idx, col] = f"{old_val} | {new_val}"
                print(f"   📝 [{idx+1}] {col} += {str(new_val)[:50]}")
    
    print(f"   ✅ Merged: {len(result_df)} rows × {len(result_df.columns)} columns")
    return result_df


# =========================================================
# Worker Thread
# =========================================================
def worker(q, results):
    while True:
        try:
            item = q.get_nowait()
        except:
            break
        
        idx, url = item
        
        try:
            print(f"\n{'='*60}")
            print(f"[{idx+1}] Processing: {url}")
            print(f"{'='*60}")
            
            text, error = crawl_site(url)
            
            if error or not text:
                data = {
                    "url": url,
                    "error": error or "NO_CONTENT",
                    "status": "FAILED"
                }
                print(f"   ❌ Failed: {error or 'NO_CONTENT'}")
            else:
                print(f"   🧠 Analyzing with Gemini...")
                data = extract_with_gemini(text)
                
                print(f"   🌐 Translating to Persian...")
                data = translate_fields(data)
                
                data["url"] = url
                data["status"] = "SUCCESS"
                data["error"] = ""
                
                print(f"   ✅ Success: {data.get('CompanyNameEN') or data.get('CompanyNameFA', 'Unknown')}")
            
            with lock:
                results.append(data)
                try:
                    Path(OUTPUT_JSON).write_text(
                        json.dumps(results, ensure_ascii=False, indent=2),
                        encoding="utf-8"
                    )
                except:
                    pass
                    
        except Exception as e:
            print(f"   ❌ Exception: {str(e)[:100]}")
            data = {
                "url": url,
                "error": f"EXCEPTION: {str(e)[:100]}",
                "status": "EXCEPTION"
            }
            with lock:
                results.append(data)
        
        q.task_done()
        time.sleep(random.uniform(*SLEEP_BETWEEN))


# =========================================================
#  Main
# =========================================================