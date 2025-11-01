# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import os, sys, json, time, io
from typing import Any, Dict, List, Union
from PIL import Image
# =========================================================
# 🔹 Gemini SDK Import
# =========================================================
try:
    import google.genai as _genai_new
    from google.genai import types as _genai_types
    print("✅ Gemini SDK loaded successfully (google-genai).")
except Exception as e:
    print("❌ Gemini SDK failed to load:", e)
    sys.exit(1)

# =========================================================
# 🧩 Dynamic Paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
OUT_JSON = Path(os.getenv("OUT_JSON", SESSION_DIR / "gemini_output.json"))

#path to Poppler for converting PDF to images
POPPLER_PATH = os.getenv("POPPLER_PATH", r"C:\poppler\Library\bin")
os.environ["PATH"] += os.pathsep + POPPLER_PATH

# =========================================================
# General Settings
# =========================================================
MODEL_NAME = "gemini-2.5-flash"
TEMPERATURE = 0.0
PDF_IMG_DPI = 150
BATCH_SIZE_PDF = 1
BATCH_SIZE_IMAGES = 3

# =========================================================
# Set API Key (only one key)
# =========================================================
API_KEY = "AIzaSyC......JDGGXI....rt61Cl2ZTs"
CLIENT = _genai_new.Client(api_key=API_KEY)


# =========================================================
# Gemini Prompt
# =========================================================
JSON_INSTRUCTIONS = """
You are an information extraction engine. Extract OCR text and structured fields from the scanned document.
Return ONLY valid JSON matching the schema. Keep original Persian text exactly as-is.
If a field has no value, return null.
"""

# =========================================================
# Define JSON Output Structure
# =========================================================
def build_newsdk_schema():
    P = _genai_types
    return P.Schema(
        type=P.Type.OBJECT,
        properties={
            "addresses":  P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "phones":     P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "faxes":      P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "emails":     P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "urls":       P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "telegram":   P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "instagram":  P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "linkedin":   P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "company_names": P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "services":      P.Schema(type=P.Type.ARRAY, items=P.Schema(type=P.Type.STRING), nullable=True),
            "persons":    P.Schema(type=P.Type.ARRAY, items=P.Schema(
                type=P.Type.OBJECT,
                properties={
                    "name": P.Schema(type=P.Type.STRING),
                    "position": P.Schema(type=P.Type.STRING, nullable=True)
                },
                required=["name"]
            ), nullable=True),
            "notes":    P.Schema(type=P.Type.STRING, nullable=True),
            "ocr_text": P.Schema(type=P.Type.STRING)
        },
        required=["ocr_text"]
    )


# =========================================================
# Helper Functions
# =========================================================

def list_files(path: Union[str, Path]) -> List[Path]:
    exts = {".jpg", ".jpeg", ".png", ".pdf"}
    return sorted([f for f in Path(path).rglob("*") if f.suffix.lower() in exts])

def to_pil(image_path: Path) -> Image.Image:
    return Image.open(image_path).convert("RGB")