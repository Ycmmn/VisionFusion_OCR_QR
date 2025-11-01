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
            # return only domain + /
            clean = f"{parsed.scheme}://{parsed.netloc}"
            if DEBUG_MODE:
                print(f"      🧹 Cleaned: {url} → {clean}")
            return clean
        
        # remove query string if it exists
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
    """extract url from vcard"""
    if not data or not isinstance(data, str):
        return None
    
    # check if it is a vcard
    if not (data.upper().startswith("BEGIN:VCARD") or "VCARD" in data.upper()):
        return None
    
    if DEBUG_MODE:
        print(f"      📇 Detected vCard format")
    
    # search for url in vcard
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


def is_low_contrast(img, sharp_thresh=85, contrast_thresh=25):
    """check for low image contrast"""
    g = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    sharpness = cv2.Laplacian(g, cv2.CV_64F).var()
    contrast = g.std()
    if DEBUG_MODE:
        print(f"   📊 Sharpness: {sharpness:.1f}, Contrast: {contrast:.1f}")
    return sharpness < sharp_thresh or contrast < contrast_thresh


def enhance_image_aggressive(img):
    """advanced preprocessing to enhance QR readability"""
    # 1. Denoise
    denoised = cv2.fastNlMeansDenoisingColored(img, None, 10, 10, 7, 21)
    
    # 2. Convert to LAB for better processing
    lab = cv2.cvtColor(denoised, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    
    # 3. strong CLAHE to enhance contrast
    clahe = cv2.createCLAHE(clipLimit=5.0, tileGridSize=(8, 8))
    l = clahe.apply(l)
    
    # 4. Merge back
    enhanced = cv2.merge([l, a, b])
    enhanced = cv2.cvtColor(enhanced, cv2.COLOR_LAB2BGR)
    
    # 5. unsharp masking for increased sharpness
    gaussian = cv2.GaussianBlur(enhanced, (0, 0), 3.0)
    enhanced = cv2.addWeighted(enhanced, 2.0, gaussian, -1.0, 0)
    
    # 6. Contrast boost
    enhanced = cv2.convertScaleAbs(enhanced, alpha=1.3, beta=15)
    
    return enhanced


# ----------------------------------------------------------
# qr detection - advanced version
# ----------------------------------------------------------
def detect_qr_payloads_enhanced(img, img_name="image"):
    """تشخیص QR با چندین روش مختلف"""
    detector = cv2.QRCodeDetector()
    payloads = []
    methods_tried = 0