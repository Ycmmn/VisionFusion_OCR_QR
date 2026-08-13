# -*- coding: utf-8 -*-
"""
Excel Web Scraper - Professional Edition
Professional web scraping from Excel + smart Gemini analysis + translation
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

import sys
import io
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# =========================================================
# Gemini SDK Import
# =========================================================
try:
    import google.genai as genai
    from google.genai import types
    print("Gemini SDK loaded successfully")
except Exception as e:
    print(f"Gemini SDK error: {e}")
    import sys
    sys.exit(1)

# =========================================================
# Dynamic paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))

# Input: automatic search for the Excel file
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
# Settings
# =========================================================
# API Key - only one key


FALLBACK_CHAIN = [
    {"key": "AIzaSyAL0CcNe2Y_FoezyZwqOSMgAIpB8f0jpHQ", "model": "gemini-3.1-flash-lite"},  # A + lite
    {"key": "AIzaSyCWjNxqXiQge4fR1RADEdmCotR3dpnKTag", "model": "gemini-3.1-flash-lite"},  # B + lite
    {"key": "AIzaSyCnT8k3MzRyyckkn1FKe48517x_f-rzEkw", "model": "gemini-3.1-flash-lite"},  # c + lite

    {"key": "AIzaSyCnT8k3MzRyyckkn1FKe48517x_f-rzEkw", "model": "gemini-3-flash"},       # A + flash
    {"key": "AIzaSyD5Jc5RDfClu_KiAPxZNyJCydQ9qf_8xio", "model": "gemini-3-flash"},       # B + flash
    {"key": "AIzaSyAL0CcNe2Y_FoezyZwqOSMgAIpB8f0jpHQ", "model": "gemini-3-flash"},       # c + flash 
]
_current_slot = 0
_slot_lock = threading.Lock()

THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3
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


print(f"\n{'='*70}")
print("Excel Web Scraper - Professional Edition")
print(f"{'='*70}")
print(f"Fallback chain: {len(FALLBACK_CHAIN)} slots configured")
print(f"Input: {INPUT_EXCEL}")
print(f"Output: {OUTPUT_EXCEL}")
print(f"{'='*70}\n")

# =========================================================
# Helper functions
# =========================================================
def normalize_url(url):
    """Normalize URL"""
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
    """Extract root domain"""
    u = normalize_url(url)
    if not u:
        return None
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}".lower()

def is_iranian_domain(url):
    """Detect Iranian domain"""
    try:
        netloc = urlparse(normalize_root(url)).netloc.lower()
        return any(netloc.endswith(tld) for tld in IRANIAN_TLDS)
    except:
        return False

def domain_exists(url):
    """Check if domain exists"""
    try:
        host = urlparse(normalize_root(url)).netloc
        socket.gethostbyname(host)
        return True
    except:
        return False

def are_values_same(v1, v2):
    """Check whether two values are identical"""
    if not v1 or not v2:
        return False
    return str(v1).strip().lower() == str(v2).strip().lower()

# =========================================================
# Web Scraping with smart SSL handling
# =========================================================
def fetch(url):
    """Fetch page content with smart SSL handling"""
    verify_ssl = not is_iranian_domain(url)
    ssl_status = "SSL ON" if verify_ssl else "SSL OFF (Iranian)"
    
    for i in range(MAX_RETRIES_HTTP):
        try:
            print(f"       Attempt {i+1}/{MAX_RETRIES_HTTP} [{ssl_status}]")
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
    """Clean HTML and extract text"""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "nav", "footer"]):
        tag.extract()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()

def crawl_site(root):
    """Fully crawl the site"""
    print(f"   Crawling: {root}")
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
            print(f"       Extracted {len(txt)} chars")
        
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
    
    print(f"      Total: {len(combined)} chars from {len(texts)} pages")
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
    """Send request to Gemini with JSON output + automatic key/model switching"""
    global _current_slot
    schema_obj = types.Schema(type=types.Type.OBJECT, properties=schema, required=[])

    while True:
        with _slot_lock:
            slot = _current_slot
            if slot >= len(FALLBACK_CHAIN):
                print("       All keys and models have reached their limit.")
                return {}
            entry = FALLBACK_CHAIN[slot]

        try:
            local_client = genai.Client(api_key=entry["key"])
            print(f"       Slot {slot+1}/{len(FALLBACK_CHAIN)} | model={entry['model']} | key=...{entry['key'][-6:]}")
            resp = local_client.models.generate_content(
                model=entry["model"],
                contents=[types.Part(text=prompt)],
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json",
                    response_schema=schema_obj
                )
            )
            return json.loads(resp.text)

        except Exception as e:
            err = str(e)
            if "429" in err or "RESOURCE_EXHAUSTED" in err or "503" in err or "UNAVAILABLE" in err:
                print(f"       Slot {slot+1} exhausted -> switching...")
                with _slot_lock:
                    if _current_slot == slot:  # only one thread should advance
                        _current_slot += 1
                time.sleep(2)
            else:
                print(f"       Gemini error: {err[:100]}")
                return {}



def extract_with_gemini(text):
    """Extract information with Gemini"""
    fields = "\n".join([f"- {f}" for f in FIELDS])
    prompt = PROMPT_EXTRACT.format(fields=fields, text=text[:8000])
    schema = {f: types.Schema(type=types.Type.STRING, nullable=True) for f in FIELDS}
    data = gemini_json(prompt, schema)
    return {f: (data.get(f) or "") for f in FIELDS}

def translate_fields(data):
    """Translate English fields into Persian"""
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
# Smart Merge with cleanup
# =========================================================
def clean_duplicate_columns(df):
    """Remove and merge duplicate columns"""
    print("\nCleaning duplicate columns...")
    
    # group columns by their base name
    base_cols = {}
    pattern = re.compile(r'\[\d+\]$')
    
    for col in df.columns:
        # extract base name
        base = pattern.sub('', str(col))
        if base not in base_cols:
            base_cols[base] = []
        base_cols[base].append(col)
    
    cleaned_df = df.copy()
    
    # for each column group
    for base, cols in base_cols.items():
        if len(cols) <= 1:
            continue
        
        print(f"    Merging {len(cols)} versions of '{base}'")
        
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
    
    print(f"   Reduced from {len(df.columns)} to {len(cleaned_df.columns)} columns")
    return cleaned_df
def smart_merge(original_df, scraped_data):
    """Smart merge of data"""
    print("\nSmart merging data...")
    
    scraped_df = pd.DataFrame(scraped_data)
    
    if scraped_df.empty:
        print("    No scraped data to merge")
        return original_df
    
    result_df = original_df.copy()
    
    # convert all columns to object so they can accept any value type (text/number)
    for col in result_df.columns:
        result_df[col] = result_df[col].astype(object)
    
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
                try:
                    print(f"    [{idx+1}] {col} = {str(new_val)[:50]}")
                except:
                    print(f"    [{idx+1}] {col} = [Updated]")
            elif not are_values_same(old_val, new_val):
                if col in ['Phone1', 'Phone2', 'Email', 'OtherEmails', 'ProductName', 'Brands']:
                    result_df.at[idx, col] = f"{old_val}, {new_val}"
                else:
                    result_df.at[idx, col] = f"{old_val} | {new_val}"
                try:
                    print(f"    [{idx+1}] {col} += {str(new_val)[:50]}")
                except:
                    print(f"    [{idx+1}] {col} += [Added]")
    
    print(f"    Merged: {len(result_df)} rows x {len(result_df.columns)} columns")
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
                print(f"    Failed: {error or 'NO_CONTENT'}")
            else:
                print(f"    Analyzing with Gemini...")
                data = extract_with_gemini(text)
                
                print(f"    Translating to Persian...")
                data = translate_fields(data)
                
                data["url"] = url
                data["status"] = "SUCCESS"
                data["error"] = ""
                
                print(f"    Success: {data.get('CompanyNameEN') or data.get('CompanyNameFA', 'Unknown')}")
            
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
            print(f"    Exception: {str(e)[:100]}")
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
# Main
# =========================================================
def main():
    print("Loading Excel file...")
    if not INPUT_EXCEL.exists():
        print(f"File not found: {INPUT_EXCEL}")
        return
    
    df = pd.read_excel(INPUT_EXCEL)
    print(f"    Loaded {len(df)} rows, {len(df.columns)} columns")
    
    url_col = None
    for col in df.columns:
        col_lower = str(col).strip().lower()
        if 'url' in col_lower or 'website' in col_lower or 'site' in col_lower:
            url_col = col
            break
    
    if not url_col:
        print("No URL column found!")
        return
    
    print(f"    URL column: '{url_col}'")
    
    urls = []
    for idx, row in df.iterrows():
        url = normalize_root(row[url_col])
        if url and domain_exists(url):
            urls.append((idx, url))
    
    print(f"    Found {len(urls)} valid URLs")
    
    if not urls:
        print("No valid URLs to scrape!")
        return
    
    print(f"\nStarting web scraping ({THREAD_COUNT} threads)...")
    
    results = []
    q = Queue()
    for item in urls:
        q.put(item)
    
    threads = []
    for _ in range(min(THREAD_COUNT, len(urls))):
        t = threading.Thread(target=worker, args=(q, results), daemon=True)
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()
    
    final_df = smart_merge(df, results)
    final_df = clean_duplicate_columns(final_df)
    
    print("\nOrganizing columns...")
    priority_cols = []
    
    for col in df.columns:
        base_col = re.sub(r'\[\d+\]$', '', str(col))
        if base_col not in priority_cols and base_col in final_df.columns:
            priority_cols.append(base_col)
    
    standard_fields = ["url", "status", "error", "CompanyNameEN", "CompanyNameFA", 
                      "CompanyNameFA_translated", "Industry", "Phone1", "Phone2", 
                      "Email", "Website", "AddressEN", "AddressFA", "AddressFA_translated",
                      "ProductName", "ProductNameFA", "ProductCategory", "ProductCategoryFA",
                      "Description", "DescriptionFA"]
    
    for field in standard_fields:
        if field not in priority_cols and field in final_df.columns:
            priority_cols.append(field)
    
    for col in final_df.columns:
        if col not in priority_cols:
            priority_cols.append(col)
    
    final_df = final_df[[c for c in priority_cols if c in final_df.columns]]
    
    print(f"\nSaving final Excel...")
    
    # extra cleanup
    def clean_dataframe_before_excel(df):
        """Remove formulas and errors"""
        import numpy as np
        
        for col in df.columns:
            if df[col].dtype == 'object':
                # 1. remove Excel formulas
                df[col] = df[col].apply(
                    lambda x: str(x)[1:] if isinstance(x, str) and str(x).startswith('=') else x
                )
                
                # 2. remove errors
                df[col] = df[col].apply(
                    lambda x: "" if isinstance(x, str) and str(x).startswith('#') else x
                )
                
                # 3. convert Persian digits
                persian_digits = '۰۱۲۳۴۵۶۷۸۹'
                english_digits = '0123456789'
                trans_table = str.maketrans(persian_digits, english_digits)
                df[col] = df[col].apply(
                    lambda x: str(x).translate(trans_table) if isinstance(x, str) else x
                )
        
        return df
    
    final_df = clean_dataframe_before_excel(final_df)
    print(f"   Cleaned {len(final_df.columns)} columns")
    
    try:
        final_df.to_excel(TEMP_EXCEL, index=False)
        shutil.move(str(TEMP_EXCEL), str(OUTPUT_EXCEL))
        print(f"    Saved: {OUTPUT_EXCEL}")
    except Exception as e:
        print(f"    Save failed: {e}")
        try:
            final_df.to_excel(OUTPUT_EXCEL, index=False)
            print(f"    Saved (direct): {OUTPUT_EXCEL}")
        except Exception as e2:
            print(f"    Direct save also failed: {e2}")
    
    success = sum(1 for r in results if r.get('status') == 'SUCCESS')
    failed = len(results) - success
    
    print(f"\n{'='*70}")
    print("FINAL STATISTICS")
    print(f"{'='*70}")
    print(f"Successfully scraped: {success}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")
    print(f"Output saved: {OUTPUT_EXCEL}")
    print(f"Final size: {len(final_df)} rows x {len(final_df.columns)} columns")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()