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
# 🧩 مسیرهای داینامیک سشن
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))
OUT_JSON = Path(os.getenv("OUT_JSON", SESSION_DIR / "gemini_scrap_output.json"))
QR_RAW_JSON = Path(os.getenv("QR_RAW_JSON", SESSION_DIR / "final_superqr_v6_raw.json"))
QR_CLEAN_JSON = Path(os.getenv("QR_CLEAN_JSON", SESSION_DIR / "final_superqr_v6_clean.json"))
MIX_OCR_QR_JSON = Path(os.getenv("MIX_OCR_QR_JSON", SESSION_DIR / "mix_ocr_qr.json"))
WEB_ANALYSIS_XLSX = Path(os.getenv("WEB_ANALYSIS_XLSX", SESSION_DIR / "web_analysis.xlsx"))


# 🔧 Configuration
GOOGLE_API_KEY = "AIzaSyAhuC9Grg_FlxwDwYUW-_CpNaFzjwUg24w"
MODEL_NAME = "gemini-2.5-flash"

THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3  # ✅ افزایش retry
MAX_RETRIES_GEMINI = 3
CHECK_DOMAIN_EXISTENCE = True

# ✅ لیست دامنه‌های ایرانی که SSL ممکنه مشکل داشته باشند
IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']

client = genai.Client(api_key=GOOGLE_API_KEY)
lock = threading.Lock()

# =========================================================
# 🔧 مسیرهای ورودی و خروجی داینامیک
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
# 🔹 Utility Functions
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

# =============================================================
# 🔹 Extract URLs (from OCR + QR + Excel)
# =============================================================
def extract_urls_from_mix(input_path: str, output_path: str):
    print("🌐 Extracting all URLs from mix_cor_qr.json (OCR + QR + Excel)...")
    try:
        raw = json.loads(Path(input_path).read_text(encoding="utf-8"))
    except Exception as e:
        print(f"❌ Error reading input JSON: {e}")
        return []

    urls = set()
    SOCIAL_EXCLUDE = ("instagram.com", "linkedin.com", "twitter.com", "x.com",
                      "facebook.com", "t.me", "wa.me", "youtube.com", "gmail.com", "mail.")
    url_pattern = re.compile(r"(https?://[^\s\"'<>]+|www\.[^\s\"'<>]+)", re.I)

    def collect(obj):
        if isinstance(obj, str):
            for m in url_pattern.findall(obj):
                u = m.strip().rstrip(".,)")
                if any(u.lower().endswith(ext) for ext in 
                       [".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf", ".zip", ".rar", ".xls", ".xlsx"]):
                    continue
                if not u.lower().startswith("http"):
                    u = "https://" + u
                r = normalize_root(u)
                if any(s in r for s in SOCIAL_EXCLUDE): continue
                urls.add(r)
        elif isinstance(obj, list):
            for v in obj: collect(v)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if k == "raw_excel_data":
                    sheets = v.get("sheets", [])
                    for sh in sheets:
                        for row in sh.get("data", []):
                            for val in row.values():
                                collect(val)
                else:
                    collect(v)

    collect(raw)
    roots = sorted(urls)
    if CHECK_DOMAIN_EXISTENCE:
        roots = [u for u in roots if domain_exists(u)]

    Path(output_path).write_text(json.dumps(roots, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Found {len(roots)} clean root URLs → {output_path}")
    return roots

# =============================================================
# 🔹 Web Crawling & Cleaning (FIXED)
# =============================================================
def fetch(url: str) -> tuple[str, str]:
    """
    Returns: (html_content, error_message)
    Smart SSL handling: Iranian domains = no verify, others = verify
    """
    # ✅ تشخیص هوشمند SSL
    verify_ssl = not is_iranian_domain(url)
    ssl_status = "🔒 SSL ON" if verify_ssl else "🔓 SSL OFF (Iranian)"
    
    for i in range(MAX_RETRIES_HTTP):
        try:
            print(f"  🔄 Attempt {i+1}/{MAX_RETRIES_HTTP} [{ssl_status}]: {url}")
            r = requests.get(
                url, 
                headers=HEADERS, 
                timeout=REQUEST_TIMEOUT, 
                verify=verify_ssl,  # ✅ داینامیک
                allow_redirects=True
            )
            if r.status_code == 200:
                print(f"  ✅ Success: {url}")
                return (r.text, "")
            else:
                print(f"  ⚠️ Status {r.status_code}: {url}")
                if i == MAX_RETRIES_HTTP - 1:
                    return ("", f"HTTP_{r.status_code}")
        except requests.exceptions.SSLError as e:
            # ✅ اگر سایت خارجی بود و SSL خطا داد، یک بار دیگه بدون verify امتحان کن
            if verify_ssl and i == 0:
                print(f"  🔄 SSL Error, retrying without verification: {url}")
                try:
                    r = requests.get(
                        url, 
                        headers=HEADERS, 
                        timeout=REQUEST_TIMEOUT, 
                        verify=False,
                        allow_redirects=True
                    )
                    if r.status_code == 200:
                        print(f"  ✅ Success (SSL disabled): {url}")
                        return (r.text, "")
                except:
                    pass
            print(f"  🔐 SSL Error: {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "SSL_ERROR")
        except requests.exceptions.Timeout:
            print(f"  ⏰ Timeout: {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "TIMEOUT")
        except requests.exceptions.ConnectionError:
            print(f"  🔌 Connection Error: {url}")
            if i == MAX_RETRIES_HTTP - 1:
                return ("", "CONNECTION_ERROR")
        except Exception as e:
            print(f"  ❌ Error: {url} -> {str(e)[:100]}")
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

def crawl_site(root: str, max_depth=MAX_DEPTH, max_pages=MAX_PAGES_PER_SITE) -> tuple[str, str]:
    """
    Returns: (combined_text, error_message)
    """
    print(f"\n🕷️ Starting crawl: {root}")
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
            print(f"  📄 Extracted {len(txt)} chars from {url}")
        else:
            errors.append(f"{url}: EMPTY_CONTENT")
        
        # Only continue crawling if we got content
        if html:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                nxt = urljoin(root, a["href"])
                if nxt.startswith(root) and nxt not in seen and len(seen) < max_pages:
                    q.append((nxt, d+1))
        
        time.sleep(random.uniform(*SLEEP_BETWEEN))
    
    combined = "\n".join(texts)[:180000]
    
    if not combined:
        error_summary = "; ".join(errors[:3])
        print(f"  ❌ No content extracted from {root}")
        return ("", error_summary or "NO_CONTENT")
    
    print(f"  ✅ Total extracted: {len(combined)} chars from {len(texts)} pages")
    return (combined, "")

# =============================================================
# 🔹 Gemini + Translation
# =============================================================
def gemini_json(prompt: str, schema: dict):
    schema = types.Schema(type=types.Type.OBJECT, properties=schema, required=[])
    for i in range(MAX_RETRIES_GEMINI):
        try:
            resp = client.models.generate_content(
                model=MODEL_NAME,
                contents=[types.Part(text=prompt)],
                config=types.GenerateContentConfig(
                    temperature=0.1,
                    response_mime_type="application/json",
                    response_schema=schema
                )
            )
            return json.loads(resp.text)
        except Exception as e:
            print(f"⚠️ Gemini error (attempt {i+1}): {str(e)[:100]}")
            if i == MAX_RETRIES_GEMINI-1: 
                return {}
            time.sleep(1.5*(i+1))
    return {}

def extract_with_gemini(text: str):
    fields = "\n".join([f"- {f}" for f in FIELDS])
    prompt = PROMPT_EXTRACT.format(fields=fields, text=text)
    schema = {f: types.Schema(type=types.Type.STRING, nullable=True) for f in FIELDS}
    data = gemini_json(prompt, schema)
    return {f: (data.get(f) or "") for f in FIELDS}

def translate_fields(data: dict):
    to_translate = {en: data.get(en) for en, _ in TRANSLATABLE_FIELDS if data.get(en)}
    if not to_translate:
        # ✅ حتی اگر محتوایی برای ترجمه نبود، ستون‌های خالی FA رو اضافه کن
        for en, fa_col in TRANSLATABLE_FIELDS:
            data[fa_col] = ""
        return data
    
    prompt = PROMPT_TRANSLATE_EN2FA.format(json_chunk=json.dumps(to_translate, ensure_ascii=False))
    schema = {k: types.Schema(type=types.Type.STRING, nullable=True) for k in to_translate.keys()}
    tr = gemini_json(prompt, schema)
    
    # ✅ برای همه فیلدهای قابل ترجمه، چه پر باشند چه خالی، ستون FA رو اضافه کن
    for en, fa_col in TRANSLATABLE_FIELDS:
        data[fa_col] = tr.get(en, "")
    
    return data

# =============================================================
# 🔹 Worker & Main (FIXED)
# =============================================================
def worker(q: Queue, results: list):
    while True:
        try:
            root = q.get_nowait()
        except:
            break
        
        try:
            print(f"\n{'='*60}")
            print(f"🌐 Processing: {root}")
            print(f"{'='*60}")
            
            text, error = crawl_site(root)
            
            if error or not text:
                data = {
                    "url": root, 
                    "error": error or "NO_CONTENT",
                    "status": "FAILED"
                }
                print(f"❌ Failed: {root} - {error or 'NO_CONTENT'}")
            else:
                print(f"🧠 Analyzing with Gemini: {root}")
                data = extract_with_gemini(text)
                data = translate_fields(data)
                data["url"] = root
                data["status"] = "SUCCESS"
                data["error"] = ""
                print(f"✅ Success: {root}")
                
        except Exception as e:
            data = {
                "url": root, 
                "error": f"EXCEPTION: {str(e)[:100]}",
                "status": "EXCEPTION"
            }
            print(f"❌ Exception for {root}: {str(e)[:100]}")
        
        with lock:
            results.append(data)
            # Save after each URL
            try:
                Path(OUTPUT_JSON).write_text(
                    json.dumps(results, ensure_ascii=False, indent=2), 
                    encoding="utf-8"
                )
            except Exception as e:
                print(f"⚠️ Failed to save JSON: {e}")
        
        q.task_done()
        time.sleep(random.uniform(*SLEEP_BETWEEN))

def main():
    print("\n" + "="*60)
    print("🚀 Starting Web Scraping Process")
    print("="*60 + "\n")
    
    roots = extract_urls_from_mix(RAW_INPUT, CLEAN_URLS)
    if not roots:
        print("⚠️ No URLs found.")
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
    print("📊 Creating Excel Report")
    print("="*60 + "\n")

    df = pd.DataFrame(results)
    
    # ✅ ترتیب ستون‌ها: ستون اصلی + ستون فارسی کنارش
    ordered_cols = ["url", "status", "error"]
    
    for field in FIELDS:
        ordered_cols.append(field)
        # اگر این فیلد ترجمه داره، ستون فارسی رو کنارش اضافه کن
        for en_field, fa_field in TRANSLATABLE_FIELDS:
            if en_field == field:
                ordered_cols.append(fa_field)
                break
    
    # اضافه کردن ستون‌هایی که ترجمه دارند ولی در FIELDS نیستند (مثل CompanyNameEN)
    for en_field, fa_field in TRANSLATABLE_FIELDS:
        if en_field not in FIELDS and en_field not in ordered_cols:
            ordered_cols.append(en_field)
            ordered_cols.append(fa_field)
    
    # اطمینان از وجود همه ستون‌ها
    for col in ordered_cols:
        if col not in df.columns:
            df[col] = ""
    
    df = df[ordered_cols]
    
    try:
        tmp = TEMP_EXCEL
        df.to_excel(tmp, index=False)
        shutil.move(tmp, OUTPUT_EXCEL)
        print(f"✅ Excel saved: {OUTPUT_EXCEL}")
    except Exception as e:
        print(f"❌ Failed to save Excel: {e}")
    
    # Print summary
    success = len([r for r in results if r.get("status") == "SUCCESS"])
    failed = len([r for r in results if r.get("status") != "SUCCESS"])
    
    print("\n" + "="*60)
    print(f"✅ Success: {success}/{len(results)}")
    print(f"❌ Failed: {failed}/{len(results)}")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()