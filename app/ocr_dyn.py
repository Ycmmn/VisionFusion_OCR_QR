# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import os, sys, json, time, io
from typing import Any, Dict, List, Union
from PIL import Image

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
#  Dynamic Paths (Fixed for Render/GitHub) + SESSION_DIR Support
# =========================================================
# 🔥 اگر SESSION_DIR تنظیم شده، از اون استفاده کن
SESSION_DIR = os.environ.get("SESSION_DIR")
if SESSION_DIR:
    SESSION_PATH = Path(SESSION_DIR)
    SOURCE_FOLDER = SESSION_PATH / "uploads"
    OUTPUT_DIR = SESSION_PATH / "data" / "output"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"✅ Using SESSION_DIR: {SESSION_DIR}")
else:
    SOURCE_FOLDER = INPUT_DIR
    print(f"✅ Using default INPUT_DIR: {INPUT_DIR}")

OUT_JSON = OUTPUT_DIR / "gemini_output.json"

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
API_KEY = "AIzaSyCKoaWh0jM4oCJDGGXIt_sJqiBHy1rt61Cl2ZTs"
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

def ensure_nulls(obj: Dict[str, Any]) -> Dict[str, Any]:
    fields = ["addresses","phones","faxes","emails","urls","telegram","instagram","linkedin","company_names","services"]
    for f in fields:
        if f not in obj or not obj[f]:
            obj[f] = None
    if "persons" not in obj or not obj["persons"]:
        obj["persons"] = None
    if "notes" not in obj or obj["notes"] == "":
        obj["notes"] = None
    if "ocr_text" not in obj or obj["ocr_text"] is None:
        obj["ocr_text"] = ""
    return obj


# =========================================================
# Single-Key Send Function
# =========================================================
def call_gemini_single_key(data: Image.Image, source_path: Path) -> Dict[str, Any]:
    schema = build_newsdk_schema()
    cfg = _genai_types.GenerateContentConfig(
        temperature=TEMPERATURE,
        response_mime_type="application/json",
        response_schema=schema,
    )

    buffer = io.BytesIO()
    data.save(buffer, format="JPEG", quality=85)
    image_bytes = buffer.getvalue()

    if len(image_bytes) > 10_000_000:
        raise RuntimeError(f"Image too large ({len(image_bytes)/1_000_000:.1f} MB).")

    parts = [
        _genai_types.Part(text=JSON_INSTRUCTIONS),
        _genai_types.Part(inline_data=_genai_types.Blob(mime_type="image/jpeg", data=image_bytes))
    ]

    try:
        resp = CLIENT.models.generate_content(model=MODEL_NAME, contents=parts, config=cfg)
        txt = getattr(resp, "text", None)
        if not txt and getattr(resp, "candidates", None):
            txt = "\n".join(p.text for p in resp.candidates[0].content.parts if getattr(p, "text", None))
        if not txt:
            raise RuntimeError("Empty response from Gemini.")
        print("✅ Gemini response received successfully.")
        return ensure_nulls(json.loads(txt))
    except Exception as e:
        raise RuntimeError(f"Gemini API Error: {e}")


# =========================================================
# Process PDF into Images and Send
# =========================================================
def pdf_to_images_and_process(pdf_path: Path) -> List[Dict[str, Any]]:
    from pdf2image import convert_from_path
    print(f"📑 Converting PDF: {pdf_path.name}")
    images = convert_from_path(pdf_path, dpi=PDF_IMG_DPI)
    results = []

    for i, img in enumerate(images, start=1):
        print(f"📄 Page {i}/{len(images)} of {pdf_path.name}")
        try:
            data = call_gemini_single_key(img, pdf_path)
            results.append({"page": i, "result": data})
        except Exception as e:
            results.append({"page": i, "error": str(e)})
        time.sleep(1)

    print(f"✅ {len(results)} page(s) processed from {pdf_path.name}")
    return results


# =========================================================
# Main Program 
# =========================================================
def main():
    print(f"\n{'='*70}")
    print("🔍 OCR Extraction - Debug Mode")
    print(f"{'='*70}")
    
    # 🔥 DEBUG: نمایش کامل مسیرها
    print(f"📂 SESSION_DIR env: {os.environ.get('SESSION_DIR', 'NOT SET')}")
    print(f"📂 SOURCE_FOLDER: {SOURCE_FOLDER}")
    print(f"   → Exists: {SOURCE_FOLDER.exists()}")
    print(f"📂 OUTPUT_DIR: {OUTPUT_DIR}")
    print(f"   → Exists: {OUTPUT_DIR.exists()}")
    print(f"📂 OUT_JSON: {OUT_JSON}")
    print(f"{'='*70}\n")
    
    if not SOURCE_FOLDER.exists():
        print(f"❌ پوشه ورودی پیدا نشد: {SOURCE_FOLDER}")
        # 🔥 لیست کردن فایل‌های موجود
        print(f"\n🔍 Listing files in parent directory:")
        parent = SOURCE_FOLDER.parent
        if parent.exists():
            for item in parent.iterdir():
                print(f"   - {item.name} ({'dir' if item.is_dir() else 'file'})")
        sys.exit(1)

    files = list_files(SOURCE_FOLDER)
    
    # 🔥 DEBUG: نمایش فایل‌های پیدا شده
    print(f"📁 Files found: {len(files)}")
    for f in files:
        print(f"   - {f.name} ({f.suffix})")
    print()
    
    if not files:
        print("❌ هیچ فایلی برای پردازش وجود ندارد.")
        sys.exit(0)

    all_out = []

    image_files = [f for f in files if f.suffix.lower() in [".jpg", ".jpeg", ".png"]]
    pdf_files = [f for f in files if f.suffix.lower() == ".pdf"]

    print(f"📊 Found: {len(image_files)} images, {len(pdf_files)} PDFs\n")

    for idx, p in enumerate(image_files, start=1):
        print(f"🖼 Processing image [{idx}/{len(image_files)}]: {p.name}")
        try:
            img = to_pil(p)
            res = call_gemini_single_key(img, p)
            all_out.append({"file_id": f"{idx:03d}", "file_name": p.name, "result": res})
        except Exception as e:
            all_out.append({"file_id": f"{idx:03d}", "file_name": p.name, "error": str(e)})
        time.sleep(1)

    for p in pdf_files:
        print(f"\n📑 Processing PDF: {p.name}")
        try:
            res = pdf_to_images_and_process(p)
            all_out.append({"file_id": p.stem, "file_name": p.name, "result": res})
        except Exception as e:
            all_out.append({"file_id": p.stem, "file_name": p.name, "error": str(e)})
        time.sleep(1)

    OUT_JSON.write_text(json.dumps(all_out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ پردازش کامل شد. نتیجه: {OUT_JSON}")
    print(f"📊 Total items saved: {len(all_out)}")


def run_ocr_extraction():
    """OCR extraction"""
    print("🔍 Starting OCR extraction...")
    main()
    return str(OUTPUT_DIR / "gemini_output.json")


if __name__ == "__main__":
    main()