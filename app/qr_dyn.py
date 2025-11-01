# -*- coding: utf-8 -*-
from __future__ import annotations
import cv2
import numpy as np
import re
import os
import json
import socket
import concurrent.futures
import time
from pathlib import Path
from pdf2image import convert_from_path
from PIL import Image
from typing import Union, List, Dict, Any
from urllib.parse import urlparse, unquote
import warnings, ctypes, os
warnings.filterwarnings("ignore")
os.environ["ZBAR_LOG_LEVEL"] = "0"

# =========================================================
# Dynamic Paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))

#inputs: If uploads is empty, use SESSION_DIR path
IMAGES_FOLDER = SESSION_DIR / "uploads"
if not IMAGES_FOLDER.exists() or not any(IMAGES_FOLDER.glob("*")):
    IMAGES_FOLDER = SESSION_DIR
print(f"📂 Using IMAGES_FOLDER → {IMAGES_FOLDER}")

#outputs (Dynamic)
OUTPUT_JSON_RAW = Path(os.getenv("QR_RAW_JSON", SESSION_DIR / "final_superqr_v6_raw.json"))
OUTPUT_JSON_CLEAN = Path(os.getenv("QR_CLEAN_JSON", SESSION_DIR / "final_superqr_v6_clean.json"))
DEBUG_DIR = SESSION_DIR / "_debug"
os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)


## dpi for pdf
PDF_IMG_DPI = int(os.getenv("PDF_IMG_DPI", "200"))

## poppler path (for windows)
POPPLER_PATH = os.getenv("POPPLER_PATH", r"C:\poppler\Library\bin").strip()
if POPPLER_PATH and os.path.exists(POPPLER_PATH):
    os.environ["PATH"] += os.pathsep + POPPLER_PATH

## debug mode
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"
print("🚀 SuperQR v6.1 (Clean URLs + vCard Support) ready\n")


# ----------------------------------------------------------
# QR fallbacks
# ----------------------------------------------------------
try:
    from pyzbar import pyzbar
    HAS_PYZBAR = True
    print("✅ pyzbar loaded")
except ImportError:
    HAS_PYZBAR = False
    print("⚠️ pyzbar not available")

try:
    from pyzxing import BarCodeReader
    zxing_reader = BarCodeReader()
    HAS_ZXING = True
    print("✅ pyzxing loaded")
except ImportError:
    HAS_ZXING = False
    print("⚠️ pyzxing not available")

# ----------------------------------------------------------
def clean_url(url):
    """clean url and remove extra parts"""
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip()
    
    # decode url if it contains encoded characters
    try:
        # keep only the main domain and path
        parsed = urlparse(url)
        
        # if path exists and is encoded, clean it
        if parsed.path and '%' in parsed.path:
            # فقط domain + / را برمی‌گردانیم
            clean = f"{parsed.scheme}://{parsed.netloc}"
            if DEBUG_MODE:
                print(f"      🧹 Cleaned: {url} → {clean}")
            return clean
        
        # اگر query string دارد، حذف می‌کنیم
        if parsed.query:
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if DEBUG_MODE:
                print(f"      🧹 Cleaned: {url} → {clean}")
            return clean
        
        return url
    except Exception as e:
        if DEBUG_MODE:
            print(f"      ⚠️ URL cleaning error: {e}")
        return url

def extract_url_from_vcard(data):
    """استخراج URL از vCard"""
    if not data or not isinstance(data, str):
        return None
    
    # بررسی اینکه آیا vCard است
    if not (data.upper().startswith("BEGIN:VCARD") or "VCARD" in data.upper()):
        return None
    
    if DEBUG_MODE:
        print(f"      📇 Detected vCard format")
    
    # جستجوی URL در vCard
    url_patterns = [
        r"URL[;:]([^\r\n]+)",
        r"URL;[^:]+:([^\r\n]+)",
        r"item\d+\.URL[;:]([^\r\n]+)",
        r"https?://[^\s\r\n]+",
    ]
    
    for pattern in url_patterns:
        matches = re.findall(pattern, data, re.IGNORECASE | re.MULTILINE)
        if matches:
            for match in matches:
                url = match.strip()
                if url.lower().startswith("http"):
                    if DEBUG_MODE:
                        print(f"      ✓ Found URL in vCard: {url}")
                    return clean_url(url)
    
    return None