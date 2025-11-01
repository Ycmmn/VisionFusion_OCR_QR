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
# 🧩 مسیرهای داینامیک
# =========================================================