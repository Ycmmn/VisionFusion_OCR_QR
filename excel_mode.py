# -*- coding: utf-8 -*-
"""
excel web scraper - professional edition
professional web scraping from excel + smart gemini analysis + translation
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


# gemini sdk import
try:
    import google.genai as genai
    from google.genai import types
    print("gemini sdk loaded successfully")
except Exception as e:
    print(f"gemini sdk error: {e}")
    import sys
    sys.exit(1)


# dynamic paths
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))

# input: automatic excel file search
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


# ^^^^^^^^^^^^^^^^^^^^^ settings
# api key - single key only
GOOGLE_API_KEY = "AIza***WI"

MODEL_NAME = "gemini-2.0-flash-exp"
THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3
MAX_RETRIES_GEMINI = 3
IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']

# fields to extract
FIELDS = [
    "CompanyNameEN", "CompanyNameFA", "Logo", "Industry", "Certifications",
    "ContactName", "PositionEN", "PositionFA", "Department",
    "Phone1", "Phone2", "Fax", "WhatsApp", "Telegram", "Instagram", "LinkedIn",
    "Website", "Email", "OtherEmails",
    "AddressEN", "AddressFA", "Country", "City",
    "ProductName", "ProductCategory", "ProductDescription", "Applications",
    "Brands", "Description", "History", "Employees", "ClientsPartners", "Markets"
]

# fields that need translation (en -> fa)
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
print("excel web scraper - professional edition")
print(f"{'='*70}")
print(f"api key: {GOOGLE_API_KEY[:20]}...")
print(f"input: {INPUT_EXCEL}")
print(f"output: {OUTPUT_EXCEL}")
print(f"{'='*70}\n")


# helper functions
def normalize_url(url):
    """url normalization"""
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
    """detect iranian domain"""
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
    """check if two values are the same"""
    if not v1 or not v2:
        return False
    return str(v1).strip().lower() == str(v2).strip().lower()



# web scraping with smart ssl
def fetch(url):
    """fetch page content with smart ssl management"""
    verify_ssl = not is_iranian_domain(url)
    ssl_status = "ssl on" if verify_ssl else "ssl off (iranian)"
    
    for i in range(MAX_RETRIES_HTTP):
        try:
            print(f"       attempt {i+1}/{MAX_RETRIES_HTTP} [{ssl_status}]")
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
    """complete site crawl"""
    print(f"   crawling: {root}")
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
            print(f"       extracted {len(txt)} chars")
        
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
    
    print(f"      total: {len(combined)} chars from {len(texts)} pages")
    return (combined, "")


# gemini extraction & translation
PROMPT_EXTRACT = """
you are a bilingual (persian-english) company information extractor.
extract the following json fields from the provided website text.
return only strict json object. if a field has no value, return empty string "".

fields:
{fields}

website text (mixed fa/en):
---
{text}
---
"""

PROMPT_TRANSLATE_EN2FA = """
translate the following english fields into formal persian.
return only valid json with the same keys and persian values.

fields json:
{json_chunk}
"""

def gemini_json(prompt, schema):
    """request to gemini with json output"""
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
            print(f"       gemini error (attempt {i+1}): {str(e)[:100]}")
            if i == MAX_RETRIES_GEMINI - 1:
                return {}
            time.sleep(2 * (i + 1))
    return {}

def extract_with_gemini(text):
    """extract information with gemini"""
    fields = "\n".join([f"- {f}" for f in FIELDS])
    prompt = PROMPT_EXTRACT.format(fields=fields, text=text[:8000])
    schema = {f: types.Schema(type=types.Type.STRING, nullable=True) for f in FIELDS}
    data = gemini_json(prompt, schema)
    return {f: (data.get(f) or "") for f in FIELDS}

def translate_fields(data):
    """translate english fields to persian"""
    to_translate = {en: data.get(en) for en, _ in TRANSLATABLE_FIELDS if data.get(en)}
    
    # add empty fa columns
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


# smart merge with cleanup
def clean_duplicate_columns(df):
    """remove and merge duplicate columns"""
    print("\ncleaning duplicate columns...")
    
    # group columns by base name
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
        
        print(f"    merging {len(cols)} versions of '{base}'")
        
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
    
    print(f"   reduced from {len(df.columns)} to {len(cleaned_df.columns)} columns")
    return cleaned_df

def smart_merge(original_df, scraped_data):
    """smart data merge"""
    print("\nsmart merging data...")
    
    scraped_df = pd.DataFrame(scraped_data)
    
    if scraped_df.empty:
        print("    no scraped data to merge")
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
                try:
                    print(f"    [{idx+1}] {col} = {str(new_val)[:50]}")
                except:
                    print(f"    [{idx+1}] {col} = [updated]")
            elif not are_values_same(old_val, new_val):
                if col in ['Phone1', 'Phone2', 'Email', 'OtherEmails', 'ProductName', 'Brands']:
                    result_df.at[idx, col] = f"{old_val}, {new_val}"
                else:
                    result_df.at[idx, col] = f"{old_val} | {new_val}"
                try:
                    print(f"    [{idx+1}] {col} += {str(new_val)[:50]}")
                except:
                    print(f"    [{idx+1}] {col} += [added]")
    
    print(f"    merged: {len(result_df)} rows x {len(result_df.columns)} columns")
    return result_df


# worker thread
def worker(q, results):
    while True:
        try:
            item = q.get_nowait()
        except:
            break
        
        idx, url = item
        
        try:
            print(f"\n{'='*60}")
            print(f"[{idx+1}] processing: {url}")
            print(f"{'='*60}")
            
            text, error = crawl_site(url)
            
            if error or not text:
                data = {
                    "url": url,
                    "error": error or "NO_CONTENT",
                    "status": "FAILED"
                }
                print(f"    failed: {error or 'NO_CONTENT'}")
            else:
                print(f"    analyzing with gemini...")
                data = extract_with_gemini(text)
                
                print(f"    translating to persian...")
                data = translate_fields(data)
                
                data["url"] = url
                data["status"] = "SUCCESS"
                data["error"] = ""
                
                print(f"    success: {data.get('CompanyNameEN') or data.get('CompanyNameFA', 'unknown')}")
            
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
            print(f"    exception: {str(e)[:100]}")
            data = {
                "url": url,
                "error": f"EXCEPTION: {str(e)[:100]}",
                "status": "EXCEPTION"
            }
            with lock:
                results.append(data)
        
        q.task_done()
        time.sleep(random.uniform(*SLEEP_BETWEEN))


# main
def main():
    print("loading excel file...")
    if not INPUT_EXCEL.exists():
        print(f"file not found: {INPUT_EXCEL}")
        return
    
    df = pd.read_excel(INPUT_EXCEL)
    print(f"    loaded {len(df)} rows, {len(df.columns)} columns")
    
    url_col = None
    for col in df.columns:
        col_lower = str(col).strip().lower()
        if 'url' in col_lower or 'website' in col_lower or 'site' in col_lower:
            url_col = col
            break
    
    if not url_col:
        print("no url column found!")
        return
    
    print(f"    url column: '{url_col}'")
    
    urls = []
    for idx, row in df.iterrows():
        url = normalize_root(row[url_col])
        if url and domain_exists(url):
            urls.append((idx, url))
    
    print(f"    found {len(urls)} valid urls")
    
    if not urls:
        print("no valid urls to scrape!")
        return
    
    print(f"\nstarting web scraping ({THREAD_COUNT} threads)...")
    
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
    
    print("\norganizing columns...")
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
    
    print(f"\nsaving final excel...")
    
    # extra cleanup
    def clean_dataframe_before_excel(df):
        """remove formulas and errors"""
        import numpy as np
        
        for col in df.columns:
            if df[col].dtype == 'object':
                # 1. remove excel formulas
                df[col] = df[col].apply(
                    lambda x: str(x)[1:] if isinstance(x, str) and str(x).startswith('=') else x
                )
                
                # 2. remove errors
                df[col] = df[col].apply(
                    lambda x: "" if isinstance(x, str) and str(x).startswith('#') else x
                )
                
                # 3. convert persian digits
                persian_digits = '۰۱۲۳۴۵۶۷۸۹'
                english_digits = '0123456789'
                trans_table = str.maketrans(persian_digits, english_digits)
                df[col] = df[col].apply(
                    lambda x: str(x).translate(trans_table) if isinstance(x, str) else x
                )
        
        return df
    
    final_df = clean_dataframe_before_excel(final_df)
    print(f"   cleaned {len(final_df.columns)} columns")
    
    try:
        final_df.to_excel(TEMP_EXCEL, index=False)
        shutil.move(str(TEMP_EXCEL), str(OUTPUT_EXCEL))
        print(f"    saved: {OUTPUT_EXCEL}")
    except Exception as e:
        print(f"    save failed: {e}")
        try:
            final_df.to_excel(OUTPUT_EXCEL, index=False)
            print(f"    saved (direct): {OUTPUT_EXCEL}")
        except Exception as e2:
            print(f"    direct save also failed: {e2}")
    
    success = sum(1 for r in results if r.get('status') == 'SUCCESS')
    failed = len(results) - success
    
    print(f"\n{'='*70}")
    print("final statistics")
    print(f"{'='*70}")
    print(f"successfully scraped: {success}/{len(results)}")
    print(f"failed: {failed}/{len(results)}")
    print(f"output saved: {OUTPUT_EXCEL}")
    print(f"final size: {len(final_df)} rows x {len(final_df.columns)} columns")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()