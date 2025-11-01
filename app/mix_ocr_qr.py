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

# =========================================================
#  helper functions
def read_json(path: Path):
    """safe json file reading"""
    try:
        if not path.exists():
            print(f"⚠️ File not found: {path}")
            return []
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"❌ Error reading {path}: {e}")
        return []


def merge_single_image(item, qr_result):
    """ادغام داده‌های تصویر"""
    qr_links = [p.get("qr_link") for p in qr_result if p.get("qr_link")]
    if isinstance(item.get("result"), dict):
        item["result"]["qr_links"] = qr_links if qr_links else None
    else:
        item["result"] = {"qr_links": qr_links if qr_links else None}
    return item
