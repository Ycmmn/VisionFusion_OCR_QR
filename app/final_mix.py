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
