# -*- coding: utf-8 -*-
from pathlib import Path
import os
import json

# =========================================================
# 🧩 مسیرهای پویا
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))

OCR_FILE = Path(os.getenv("OCR_FILE", SESSION_DIR / "gemini_output.json"))
QR_FILE = Path(os.getenv("QR_FILE", SESSION_DIR / "final_superqr_v6_clean.json"))
OUTPUT_FILE = Path(os.getenv("OUTPUT_FILE", SESSION_DIR / "mix_ocr_qr.json"))

# =========================================================
# 📦 توابع کمکی
def read_json(path: Path):
    """خواندن امن فایل JSON"""
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

def merge_pdf_pages(item, qr_result):
    """ادغام داده‌های PDF چندصفحه‌ای"""
    if not isinstance(item.get("result"), list):
        return item

    for page_obj in item["result"]:
        page_num = page_obj.get("page")
        qr_match = next((p.get("qr_link") for p in qr_result if p.get("page") == page_num), None)
        page_obj["qr_link"] = qr_match
    return item

def merge_ocr_qr(ocr_data, qr_data):
    """ادغام کامل داده‌های OCR و QR"""
    qr_lookup = {item["file_name"]: item.get("result", []) for item in qr_data}
    merged = []

    for item in ocr_data:
        file_name = item.get("file_name")
        qr_result = qr_lookup.get(file_name, [])

        # 🖼 حالت تصویر
        if file_name.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp")):
            item = merge_single_image(item, qr_result)

        # 📄 حالت PDF
        elif file_name.lower().endswith(".pdf"):
            item = merge_pdf_pages(item, qr_result)

        # 🧩 سایر فرمت‌ها
        else:
            item["result"] = item.get("result", {})
            item["result"]["qr_links"] = None

        merged.append(item)

    return merged

# =========================================================
# 🚀 اجرای اصلی
def main():
    print("\n🚀 Starting OCR + QR merge process...\n")

    ocr_data = read_json(OCR_FILE)
    qr_data = read_json(QR_FILE)

    if not ocr_data:
        print(f"⚠️ OCR file is empty or not found → continuing with QR data only.")

    if not qr_data:
        print(f"⚠️ QR file is empty or not found → continuing with OCR data only.")

    print(f"📄 Loaded OCR: {len(ocr_data)} items")
    print(f"🔗 Loaded QR : {len(qr_data)} items")

    merged_results = merge_ocr_qr(ocr_data, qr_data)

    OUTPUT_FILE.write_text(json.dumps(merged_results, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n✅ Merge completed successfully!")
    print(f"📁 Final output saved to → {OUTPUT_FILE}")
    print(f"📊 Total merged records: {len(merged_results)}\n")


if __name__ == "__main__":
    main()
