# -*- coding: utf-8 -*-

from pathlib import Path
import os
import json

# =========================================================
# 🔧 Dynamic Path Resolution (Works on Streamlit Cloud)
# =========================================================
SESSION_DIR = os.getenv("SESSION_DIR")

if SESSION_DIR:
    BASE_DIR = Path(SESSION_DIR)
else:
    BASE_DIR = Path.cwd() / "session_current"

INPUT_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR
DATA_DIR = BASE_DIR
RENAMED_DIR = DATA_DIR / "renamed"

for folder in [INPUT_DIR, OUTPUT_DIR, RENAMED_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# فایل‌های ورودی
OCR_FILE = OUTPUT_DIR / "gemini_output.json"
QR_FILE = OUTPUT_DIR / "final_superqr_v6_clean.json"
OUTPUT_FILE = OUTPUT_DIR / "mix_ocr_qr.json"

print(f"📂 SESSION_DIR: {SESSION_DIR or 'Not Set'}")
print(f"📂 OUTPUT_DIR: {OUTPUT_DIR}")


# =========================================================
# 🧩 Dynamic Paths (Fixed for Render/GitHub)
# =========================================================
SOURCE_FOLDER = INPUT_DIR
RENAMED_DIR = DATA_DIR / "renamed"


OCR_FILE = OUTPUT_DIR / "gemini_output.json"
QR_FILE = OUTPUT_DIR / "final_superqr_v6_clean.json"
OUTPUT_FILE = OUTPUT_DIR / "mix_ocr_qr.json"

os.makedirs(SOURCE_FOLDER, exist_ok=True)
os.makedirs(RENAMED_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
    """merge image data"""
    qr_links = [p.get("qr_link") for p in qr_result if p.get("qr_link")]
    if isinstance(item.get("result"), dict):
        item["result"]["qr_links"] = qr_links if qr_links else None
    else:
        item["result"] = {"qr_links": qr_links if qr_links else None}
    return item


def merge_pdf_pages(item, qr_result):
    """merge multi-page pdf data"""
    if not isinstance(item.get("result"), list):
        return item

    for page_obj in item["result"]:
        page_num = page_obj.get("page")
        qr_match = next((p.get("qr_link") for p in qr_result if p.get("page") == page_num), None)
        page_obj["qr_link"] = qr_match
    return item



def merge_ocr_qr(ocr_data, qr_data):
    """merge complete ocr and qr data"""
    qr_lookup = {item["file_name"]: item.get("result", []) for item in qr_data}
    merged = []

    for item in ocr_data:
        file_name = item.get("file_name")
        qr_result = qr_lookup.get(file_name, [])

        #  image mode
        if file_name.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp")):
            item = merge_single_image(item, qr_result)

        # pdf mode
        elif file_name.lower().endswith(".pdf"):
            item = merge_pdf_pages(item, qr_result)

        # other formats
        else:
            item["result"] = item.get("result", {})
            item["result"]["qr_links"] = None

        merged.append(item)

    return merged

# =========================================================
# main execution
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


def run_mix_ocr_qr(session_dir_path):
    """ادغام OCR + QR"""
    global SESSION_DIR, BASE_DIR, OUTPUT_DIR, OCR_FILE, QR_FILE, OUTPUT_FILE
    
    BASE_DIR = Path(session_dir_path)
    OUTPUT_DIR = BASE_DIR
    OCR_FILE = OUTPUT_DIR / "gemini_output.json"
    QR_FILE = OUTPUT_DIR / "final_superqr_v6_clean.json"
    OUTPUT_FILE = OUTPUT_DIR / "mix_ocr_qr.json"
    
    print(f"📂 Mix Session: {BASE_DIR}")
    
    main()
    
    if OUTPUT_FILE.exists():
        print(f"✅ Mix output created: {OUTPUT_FILE}")
        return str(OUTPUT_FILE)
    else:
        raise FileNotFoundError(f"Mix output not found: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()