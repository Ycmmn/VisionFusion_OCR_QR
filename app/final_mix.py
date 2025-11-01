# -*- coding: utf-8 -*-
"""
🚀 Complete JSON + Excel Merger - Final Version
Smart merging of JSON and Excel with full cleaning and optimization
"""

from pathlib import Path
import os, json, re, pandas as pd
from collections import defaultdict
import time

# =========================================================
#  dynamic paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
INPUT_JSON = Path(os.getenv("INPUT_JSON", SESSION_DIR / "mix_ocr_qr.json"))
INPUT_EXCEL = Path(os.getenv("INPUT_EXCEL", SESSION_DIR / "web_analysis.xlsx"))
timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_EXCEL = Path(os.getenv("OUTPUT_EXCEL", SESSION_DIR / f"merged_final_{timestamp}.xlsx"))

print("\n" + "="*70)
print("🚀 Complete JSON + Excel Merger (Optimized)")
print("="*70)
print(f"📂 Session: {SESSION_DIR}")
print(f"📥 JSON: {INPUT_JSON}")
print(f"📥 Excel: {INPUT_EXCEL}")
print(f"📤 Output: {OUTPUT_EXCEL}")
print("="*70 + "\n")

# =========================================================
#  helper functions
# =========================================================
def is_persian(text):
    if not text or pd.isna(text):
        return False
    return bool(re.search(r"[\u0600-\u06FF]", str(text)))

def normalize_value(val):
    if val is None or pd.isna(val):
        return ""
    return str(val).strip().lower()

def are_values_same(val1, val2):
    return normalize_value(val1) == normalize_value(val2)

def normalize_website(url):
    if not url or pd.isna(url):
        return ""
    u = str(url).strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    u = u.split("/")[0].split("?")[0]
    return u.rstrip(".")

def normalize_phone(phone):
    if not phone or pd.isna(phone):
        return ""
    return re.sub(r"[^\d+]", "", str(phone))

def normalize_company_name(name):
    if not name or pd.isna(name):
        return ""
    n = str(name).strip().lower()
    stopwords = ["شرکت", "company", "co.", "co", "ltd", "inc", "corp",
                 "سهامی", "خاص", "عام", "private", "public", "holding",
                 "international", "بین المللی", "گروه", "group"]
    for word in stopwords:
        n = n.replace(word, " ")
    n = re.sub(r"[^\w\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

def extract_key_identifier(record):
    website = normalize_website(record.get("Website") or record.get("urls") or record.get("url", ""))
    if website:
        return ("website", website)
    
    phone_fields = ["phones", "Phone1", "Phone2", "Phone3", "Phone4", "WhatsApp", "Telegram", "Fax"]
    for pf in phone_fields:
        phone = normalize_phone(record.get(pf, ""))
        if phone and len(phone) >= 8:
            return ("phone", phone)
    
    email = normalize_value(record.get("Email") or record.get("emails", ""))
    if email and "@" in email:
        return ("email", email)
    
    for name_field in ["CompanyNameEN", "CompanyNameFA", "company_names"]:
        name = normalize_company_name(record.get(name_field, ""))
        if name and len(name) > 3:
            return ("company", name)
    
    file_id = record.get("file_id", "")
    page = record.get("page", "")
    if file_id and page:
        return ("unique", f"{file_id}_page{page}")
    
    return ("unique", str(id(record)))


# =========================================================
#  load JSON
# =========================================================
def load_json_records(json_path):
    print("\n📥 Loading JSON...")
    if not json_path.exists():
        print(f"   ⚠️ Not found: {json_path}")
        return []
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        records = []
        if isinstance(raw_data, list):
            for file_item in raw_data:
                if not isinstance(file_item, dict):
                    continue
                
                file_id = file_item.get("file_id", "")
                file_name = file_item.get("file_name", "")
                result_array = file_item.get("result")
                
                if isinstance(result_array, list):
                    for page_data in result_array:
                        if not isinstance(page_data, dict):
                            continue
                        
                        page_num = page_data.get("page", 0)
                        page_result = page_data.get("result", {})
                        
                        if not isinstance(page_result, dict):
                            continue
                        
                        record = {"file_id": file_id, "file_name": file_name, "page": page_num}
                        
                        for key, value in page_result.items():
                            if value is None:
                                continue
                            if isinstance(value, list):
                                if not value:
                                    continue
                                record[key] = value[0]
                                for idx, v in enumerate(value[1:], 2):
                                    record[f"{key}[{idx}]"] = v
                            else:
                                if str(value).strip():
                                    record[key] = value
                        
                        if len(record) > 3:
                            records.append(record)
        
        print(f"   ✅ Loaded {len(records)} page records")
        return records
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []

# =========================================================
#  load Excel
# =========================================================
def load_excel_records(excel_path):
    print("\n📥 Loading Excel...")
    if not excel_path.exists():
        print(f"   ⚠️ Not found: {excel_path}")
        return []
    
    try:
        df = pd.read_excel(excel_path)
        print(f"   ✓ Size: {df.shape[0]} rows × {df.shape[1]} columns")
        
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.dropna(how='all')
        df = df.drop_duplicates()
        df = df.dropna(axis=1, how='all')
        df.columns = [str(col).strip() for col in df.columns]
        
        records = df.to_dict('records')
        cleaned = []
        for rec in records:
            clean = {k: v for k, v in rec.items() if not (pd.isna(v) or str(v).strip() == "")}
            if clean:
                cleaned.append(clean)
        
        print(f"   ✅ Loaded {len(cleaned)} clean records")
        return cleaned
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []


# =========================================================
#  merge two records
# =========================================================
def merge_two_records(r1, r2):
    merged = {}
    for key in set(r1.keys()) | set(r2.keys()):
        v1, v2 = r1.get(key), r2.get(key)
        if not v1 and not v2:
            continue
        if not v1:
            merged[key] = v2
            continue
        if not v2:
            merged[key] = v1
            continue
        if are_values_same(v1, v2):
            merged[key] = v1
        else:
            merged[key] = v1
            counter = 2
            while f"{key}[{counter}]" in merged:
                counter += 1
            merged[f"{key}[{counter}]"] = v2
    return merged


# =========================================================
# 🔗 ادغام هوشمند
# =========================================================