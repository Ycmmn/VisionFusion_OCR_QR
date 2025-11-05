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

# ✅ مسیر Poppler برای PDF → Image
POPPLER_PATH = os.getenv("POPPLER_PATH", r"C:\poppler\Library\bin")
os.environ["PATH"] += os.pathsep + POPPLER_PATH

# =========================================================
# ⚙️ تنظیمات عمومی
# =========================================================
MODEL_NAME = "gemini-2.5-flash"
TEMPERATURE = 0.0
PDF_IMG_DPI = 150
BATCH_SIZE_PDF = 1
BATCH_SIZE_IMAGES = 3

# =========================================================
# 🔑 تنظیم کلید API (فقط یک کلید)
# =========================================================
API_KEY = "AIzaSyCKoaSP6Wgj5FCJDGGXIBHy1rt61Cl2ZTs"
CLIENT = _genai_new.Client(api_key=API_KEY)

# =========================================================
# 🧩 Gemini Prompt
# =========================================================
JSON_INSTRUCTIONS = """
You are an information extraction engine. Extract OCR text and structured fields from the scanned document.
Return ONLY valid JSON matching the schema. Keep original Persian text exactly as-is.
If a field has no value, return null.
"""

# =========================================================
# 🔹 تعریف ساختار خروجی JSON
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
# 🧩 توابع کمکی
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
# 🔁 تابع ارسال با یک کلید (بدون چرخش)
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
# 📄 پردازش PDF به تصاویر و ارسال
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
# 🚀 اجرای اصلی برنامه
# =========================================================
def main():
    print(f"🔑 Using single API key.\n")
    
    if not SOURCE_FOLDER.exists():
        print(f"❌ پوشه ورودی پیدا نشد: {SOURCE_FOLDER}")
        sys.exit(1)

    files = list_files(SOURCE_FOLDER)
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

if __name__ == "__main__":
    main()
