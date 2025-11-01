# -*- coding: utf-8 -*-
from pathlib import Path
import os
import json

# =========================================================
#  dynamic paths
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))

OCR_FILE = Path(os.getenv("OCR_FILE", SESSION_DIR / "gemini_output.json"))
QR_FILE = Path(os.getenv("QR_FILE", SESSION_DIR / "final_superqr_v6_clean.json"))
OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE", SESSION_DIR / "mix_ocr_qr.json"))