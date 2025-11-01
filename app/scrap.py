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
# dynamic session paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))
OUT_JSON = Path(os.getenv("OUT_JSON", SESSION_DIR / "gemini_scrap_output.json"))
QR_RAW_JSON = Path(os.getenv("QR_RAW_JSON", SESSION_DIR / "final_superqr_v6_raw.json"))
QR_CLEAN_JSON = Path(os.getenv("QR_CLEAN_JSON", SESSION_DIR / "final_superqr_v6_clean.json"))
MIX_OCR_QR_JSON = Path(os.getenv("MIX_OCR_QR_JSON", SESSION_DIR / "mix_ocr_qr.json"))
WEB_ANALYSIS_XLSX = Path(os.getenv("WEB_ANALYSIS_XLSX", SESSION_DIR / "web_analysis.xlsx"))



#  Configuration
GOOGLE_API_KEY = "AIzaSyAhuC9Grg_FlxwDwYUW-_CpNaFzjwUg24w"
MODEL_NAME = "gemini-2.5-flash"

THREAD_COUNT = 5
MAX_DEPTH = 2
MAX_PAGES_PER_SITE = 25
REQUEST_TIMEOUT = (8, 20)
SLEEP_BETWEEN = (0.8, 2.0)
MAX_RETRIES_HTTP = 3  #  increase retry attempts
MAX_RETRIES_GEMINI = 3
CHECK_DOMAIN_EXISTENCE = True

# list of Iranian domains that may have SSL issues
IRANIAN_TLDS = ['.ir', '.ac.ir', '.co.ir', '.org.ir', '.gov.ir', '.id.ir', '.net.ir']

client = genai.Client(api_key=GOOGLE_API_KEY)
lock = threading.Lock()


# =========================================================
#  dynamic input and output paths
# =========================================================