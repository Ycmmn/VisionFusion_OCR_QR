# -*- coding: utf-8 -*-
"""
🔗 OCR + QR Merger - Auto File Finder Edition
Automatically finds OCR and QR files regardless of their location
"""

from pathlib import Path
import os
import json
import sys

# =========================================================
# 🔍 SMART PATH DETECTION
# =========================================================
def get_base_dir():
    """پیدا کردن مسیر اصلی (Streamlit یا Local)"""
    # اول سعی می‌کنیم از SESSION_DIR استفاده کنیم
    session_dir = os.environ.get("SESSION_DIR")
    if session_dir:
        base = Path(session_dir)
        print(f"✅ Using SESSION_DIR: {base}")
        return base
    
    # اگر نبود، از مسیر فعلی استفاده می‌کنیم
    base = Path(__file__).resolve().parent.parent
    print(f"✅ Using BASE_DIR: {base}")
    return base

BASE_DIR = get_base_dir()
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================================================
# 🔍 AUTO FILE FINDER
# =========================================================
def find_file_smart(patterns, search_dirs):
    """
    فایل رو با چندین الگو و در چندین مسیر پیدا می‌کنه
    patterns: لیست الگوهای فایل مثل ['*ocr*.json', 'gemini*.json']
    search_dirs: لیست مسیرهایی که باید جستجو بشن
    """
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        
        for pattern in patterns:
            files = list(search_dir.glob(pattern))
            if files:
                # جدیدترین فایل رو برمی‌گردونه
                latest = max(files, key=lambda f: f.stat().st_mtime)
                print(f"   ✅ Found: {latest.name} in {search_dir}")
                return latest
    
    return None

def find_ocr_file():
    """پیدا کردن فایل OCR"""
    print("\n🔍 Searching for OCR file...")
    
    # الگوهای مختلف برای فایل OCR
    patterns = [
        'gemini_output.json',
        '*ocr*.json',
        'gemini*.json'
    ]
    
    # مسیرهای مختلف برای جستجو
    search_dirs = [
        OUTPUT_DIR,                           # data/output
        BASE_DIR,                             # root
        BASE_DIR / "output",                  # output
        Path.cwd() / "data" / "output",      # current/data/output
    ]
    
    ocr_file = find_file_smart(patterns, search_dirs)
    
    if ocr_file:
        print(f"   ✅ OCR File: {ocr_file}")
    else:
        print(f"   ⚠️ No OCR file found!")
    
    return ocr_file

def find_qr_file():
    """پیدا کردن فایل QR"""
    print("\n🔍 Searching for QR file...")
    
    # الگوهای مختلف برای فایل QR
    patterns = [
        'final_superqr_v6_clean.json',
        '*qr*clean*.json',
        '*superqr*.json',
        '*qr*.json'
    ]
    
    # مسیرهای مختلف برای جستجو
    search_dirs = [
        OUTPUT_DIR,
        BASE_DIR,
        BASE_DIR / "output",
        Path.cwd() / "data" / "output",
    ]
    
    qr_file = find_file_smart(patterns, search_dirs)
    
    if qr_file:
        print(f"   ✅ QR File: {qr_file}")
    else:
        print(f"   ⚠️ No QR file found!")
    
    return qr_file

# =========================================================
# PATHS
# =========================================================
OCR_FILE = find_ocr_file()
QR_FILE = find_qr_file()
OUTPUT_FILE = OUTPUT_DIR / "mix_ocr_qr.json"

print(f"\n{'='*70}")
print("🔗 OCR + QR Merger (Smart Auto-Finder)")
print(f"{'='*70}")
print(f"📥 OCR Input: {OCR_FILE if OCR_FILE else 'NOT FOUND'}")
print(f"📥 QR Input:  {QR_FILE if QR_FILE else 'NOT FOUND'}")
print(f"📤 Output:    {OUTPUT_FILE}")
print(f"{'='*70}\n")

# =========================================================
# Helper Functions
# =========================================================
def read_json(path):
    """Safe JSON reading"""
    try:
        if not path or not path.exists():
            print(f"⚠️ File not found: {path}")
            return []
        
        data = json.loads(path.read_text(encoding="utf-8"))
        print(f"   ✅ Loaded: {len(data)} items from {path.name}")
        return data
    except Exception as e:
        print(f"❌ Error reading {path}: {e}")
        return []

def merge_single_image(item, qr_result):
    """Merge image data"""
    qr_links = [p.get("qr_link") for p in qr_result if p.get("qr_link")]
    
    if isinstance(item.get("result"), dict):
        item["result"]["qr_links"] = qr_links if qr_links else None
    else:
        item["result"] = {"qr_links": qr_links if qr_links else None}
    
    return item

def merge_pdf_pages(item, qr_result):
    """Merge multi-page PDF data"""
    if not isinstance(item.get("result"), list):
        return item
    
    for page_obj in item["result"]:
        page_num = page_obj.get("page")
        qr_match = next((p.get("qr_link") for p in qr_result if p.get("page") == page_num), None)
        page_obj["qr_link"] = qr_match
    
    return item

def merge_ocr_qr(ocr_data, qr_data):
    """Merge complete OCR and QR data"""
    if not ocr_data and not qr_data:
        print("\n❌ No data to merge!")
        return []
    
    # اگر فقط یکی وجود داشته باشه
    if not ocr_data:
        print("\n⚠️ No OCR data, returning QR data only")
        return qr_data
    
    if not qr_data:
        print("\n⚠️ No QR data, returning OCR data only")
        return ocr_data
    
    # ادغام کامل
    qr_lookup = {item["file_name"]: item.get("result", []) for item in qr_data}
    merged = []
    
    for item in ocr_data:
        file_name = item.get("file_name", "")
        qr_result = qr_lookup.get(file_name, [])
        
        # Image mode
        if file_name.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp")):
            item = merge_single_image(item, qr_result)
        
        # PDF mode
        elif file_name.lower().endswith(".pdf"):
            item = merge_pdf_pages(item, qr_result)
        
        # Other
        else:
            if not isinstance(item.get("result"), dict):
                item["result"] = {}
            item["result"]["qr_links"] = None
        
        merged.append(item)
    
    return merged

# =========================================================
# Main Execution
# =========================================================
def main():
    print("\n🚀 Starting OCR + QR merge process...\n")
    
    # بررسی وجود فایل‌ها
    if not OCR_FILE and not QR_FILE:
        print("❌ ERROR: No OCR or QR files found!")
        print("💡 Hint: Make sure ocr_dyn.py and qr_dyn.py ran successfully")
        return 1
    
    ocr_data = read_json(OCR_FILE) if OCR_FILE else []
    qr_data = read_json(QR_FILE) if QR_FILE else []
    
    if not ocr_data:
        print(f"⚠️ OCR file is empty or not found → continuing with QR data only")
    
    if not qr_data:
        print(f"⚠️ QR file is empty or not found → continuing with OCR data only")
    
    print(f"\n📊 Data Summary:")
    print(f"   📄 OCR items: {len(ocr_data)}")
    print(f"   🔗 QR items:  {len(qr_data)}")
    
    merged_results = merge_ocr_qr(ocr_data, qr_data)
    
    if not merged_results:
        print("\n❌ No data to save!")
        return 1
    
    OUTPUT_FILE.write_text(
        json.dumps(merged_results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    print(f"\n✅ Merge completed successfully!")
    print(f"📁 Output: {OUTPUT_FILE}")
    print(f"📊 Total records: {len(merged_results)}\n")
    
    return 0

def run_mix_ocr_qr():
    """Run merge (for import)"""
    print("🔗 Starting OCR+QR merge...")
    code = main()
    if code == 0:
        return str(OUTPUT_FILE)
    return None

if __name__ == "__main__":
    sys.exit(main())