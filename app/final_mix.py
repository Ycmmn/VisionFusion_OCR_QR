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
