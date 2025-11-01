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
GOOGLE_API_KEY = "AIzaSyBzVNw34fbQRcxCSZDouR35hoZNxqsW6pc"

MODEL_NAME = "gemini-2.0-flash-exp"
THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3
MAX_RETRIES_GEMINI = 3
IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']