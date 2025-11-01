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