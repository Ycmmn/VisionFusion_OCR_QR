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