# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import os, sys, json, time, io
from typing import Any, Dict, List, Union
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
# =========================================================
# Gemini SDK Import
# =========================================================
try:
    import google.genai as _genai_new
    from google.genai import types as _genai_types
    print("Gemini SDK loaded successfully (google-genai).")
except Exception as e:
    print("Gemini SDK failed to load:", e)
    sys.exit(1)

# =========================================================
# Dynamic Paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
OUT_JSON = Path(os.getenv("OUT_JSON", SESSION_DIR / "gemini_output.json"))

# Poppler path for PDF -> Image
POPPLER_PATH = os.getenv("POPPLER_PATH", r"C:\poppler\Library\bin")
os.environ["PATH"] += os.pathsep + POPPLER_PATH

# =========================================================
# General settings
# =========================================================

TEMPERATURE = 0.0
PDF_IMG_DPI = 150
BATCH_SIZE_PDF = 1
BATCH_SIZE_IMAGES = 3

MAX_IMAGES = 30      # max 30 images
MAX_PDF = 10         # max 10 PDFs
MAX_EXCEL = 5        # max 5 Excel files (if added later)
MAX_WORKERS = 3      # number of parallel threads

# =========================================================
# API key setup (only one key)
# =========================================================

# Cascading Fallback: key + model
FALLBACK_CHAIN = [
    {"key": "AIz*******************8f0jpHQ", "model": "gemini-3.1-flash-lite"},  # A + lite
    {"key": "AIza********************nKTag", "model": "gemini-3.1-flash-lite"},  # B + lite


    {"key": "AIza****************_f-rzEkw", "model": "gemini-3-flash"},       # A + flash
    {"key": "AIzaS****************************_8xio", "model": "gemini-3-flash"},       # B + flash
  
]
_current_slot = 0
_slot_lock = threading.Lock()

# =========================================================
# Gemini Prompt
# =========================================================
JSON_INSTRUCTIONS = """
You are an information extraction engine. Extract OCR text and structured fields from the scanned document.
Return ONLY valid JSON matching the schema. Keep original Persian text exactly as-is.
If a field has no value, return null.
"""

# =========================================================
# Define the JSON output structure
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
# Helper functions
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
# Function to send with a single key (no rotation)
# =========================================================
def call_gemini_single_key(data: Image.Image, source_path: Path) -> Dict[str, Any]:
    global _current_slot

    schema = build_newsdk_schema()
    buffer = io.BytesIO()
    data.save(buffer, format="JPEG", quality=85)
    image_bytes = buffer.getvalue()

    if len(image_bytes) > 10_000_000:
        raise RuntimeError(f"Image too large ({len(image_bytes)/1_000_000:.1f} MB).")

    parts = [
        _genai_types.Part(text=JSON_INSTRUCTIONS),
        _genai_types.Part(inline_data=_genai_types.Blob(mime_type="image/jpeg", data=image_bytes))
    ]

    

    while True:
        with _slot_lock:
            slot = _current_slot
            if slot >= len(FALLBACK_CHAIN):
                raise RuntimeError("All keys and models have reached their limit.")
            entry = FALLBACK_CHAIN[slot]

        client = _genai_new.Client(api_key=entry["key"])
        model  = entry["model"]
        cfg = _genai_types.GenerateContentConfig(
            temperature=TEMPERATURE,
            response_mime_type="application/json",
            response_schema=schema,
        )

        try:
            print(f"   Slot {slot+1}/4 | model={model} | key=...{entry['key'][-6:]}")
            resp = client.models.generate_content(model=model, contents=parts, config=cfg)
            txt = getattr(resp, "text", None)
            if not txt and getattr(resp, "candidates", None):
                txt = "\n".join(p.text for p in resp.candidates[0].content.parts if getattr(p, "text", None))
            if not txt:
                raise RuntimeError("Empty response from Gemini.")
            print("   Success")
            return ensure_nulls(json.loads(txt))

        except Exception as e:
            err = str(e)
            if "429" in err or "RESOURCE_EXHAUSTED" in err or "503" in err or "UNAVAILABLE" in err:
                print(f"   Slot {slot+1} exhausted  switching...")
                with _slot_lock:
                    if _current_slot == slot:  # only one thread should advance
                        _current_slot += 1
                time.sleep(2)
            else:
                raise RuntimeError(f"Gemini API Error: {err}")

# =========================================================
# Convert PDF to images and process
# =========================================================
def pdf_to_images_and_process(pdf_path: Path) -> List[Dict[str, Any]]:
    from pdf2image import convert_from_path
    print(f"Converting PDF: {pdf_path.name}")
    images = convert_from_path(pdf_path, dpi=PDF_IMG_DPI)
    results = []

    for i, img in enumerate(images, start=1):
        print(f"Page {i}/{len(images)} of {pdf_path.name}")
        try:
            data = call_gemini_single_key(img, pdf_path)
            results.append({"page": i, "result": data})
        except Exception as e:
            results.append({"page": i, "error": str(e)})
        time.sleep(1)

    print(f"{len(results)} page(s) processed from {pdf_path.name}")
    return results




# =========================================================
# Parallel processing of images
# =========================================================
def process_images_parallel(image_files: List[Path], max_workers=3) -> List[Dict[str, Any]]:
    """
    Process images in parallel with Gemini
    
    Args:
        image_files: list of image files
        max_workers: number of concurrent threads
    
    Returns:
        results: list of processed results
    """
    results = []
    errors = []
    lock = threading.Lock()
    
    def process_single_image(file_data):
        idx, img_path = file_data
        thread_name = threading.current_thread().name
        
        try:
            print(f"[{thread_name}] Processing image [{idx}/{len(image_files)}]: {img_path.name}")
            
            # delay for rate limiting
            time.sleep(2)
            
            # process the image
            
            img = to_pil(img_path)
            for attempt in range(3):  # 3 attempts
                try:
                    res = call_gemini_single_key(img, img_path)
                    break
                except Exception as retry_err:
                    if attempt < 2:
                        print(f"[{thread_name}] Retry {attempt+1}/3: {retry_err}")
                        time.sleep(3)
                    else:
                        raise

            
            with lock:
                results.append({
                    "index": idx,
                    "file_id": f"{idx:03d}",
                    "file_name": img_path.name,
                    "result": res
                })
            
            print(f"[{thread_name}] Success: {img_path.name}")
            return True
            
        except Exception as e:
            print(f"[{thread_name}] Error: {img_path.name} - {e}")
            with lock:
                errors.append({
                    "index": idx,
                    "file_id": f"{idx:03d}",
                    "file_name": img_path.name,
                    "error": str(e)
                })
            return False
    
    # run in parallel
    print(f"\nProcessing {len(image_files)} images with {max_workers} threads...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        file_data = [(idx, img) for idx, img in enumerate(image_files, start=1)]
        list(executor.map(process_single_image, file_data))
    
    # sort by index
    results.sort(key=lambda x: x['index'])
    
    # remove index from results
    for r in results:
        r.pop('index', None)
    
    # show summary
    print(f"\nImages Summary:")
    print(f"   Success: {len(results)}/{len(image_files)}")
    if errors:
        print(f"   Errors: {len(errors)}/{len(image_files)}")
        for err in errors[:3]:  # show the first 3 errors
            print(f"      - {err['file_name']}: {err['error']}")
    
    # combine results and errors
    all_results = results + errors
    all_results.sort(key=lambda x: x.get('file_id', '999'))
    
    return all_results

# =========================================================
# Parallel processing of PDFs (PDFs serial, pages within each PDF parallel)
# =========================================================
def process_pdfs_parallel(pdf_files: List[Path]) -> List[Dict[str, Any]]:
    """
    Process PDFs (each PDF serially, but pages within each PDF in parallel)
    
    Args:
        pdf_files: list of PDF files
    
    Returns:
        results: list of results
    """
    all_results = []
    
    for pdf_path in pdf_files:
        print(f"\nProcessing PDF: {pdf_path.name}")
        try:
            # process the PDF (pages inside are processed in parallel)
            res = pdf_to_images_and_process(pdf_path)
            all_results.append({
                "file_id": pdf_path.stem,
                "file_name": pdf_path.name,
                "result": res
            })
        except Exception as e:
            all_results.append({
                "file_id": pdf_path.stem,
                "file_name": pdf_path.name,
                "error": str(e)
            })
        
        # delay between PDFs
        time.sleep(1)
    
    return all_results


# =========================================================
# Main program execution (with parallel processing)
# =========================================================
def main():
    print("="*70)
    print("Smart OCR Pipeline - Parallel Processing")
    print("="*70)
    print(f"Fallback chain: {len(FALLBACK_CHAIN)} slots configured")
    print(f"Max parallel threads: {MAX_WORKERS}")
    print(f"Limits: {MAX_IMAGES} images, {MAX_PDF} PDFs")
    print("="*70 + "\n")
    
    if not SOURCE_FOLDER.exists():
        print(f"Source folder not found: {SOURCE_FOLDER}")
        sys.exit(1)

    files = list_files(SOURCE_FOLDER)
    if not files:
        print("No files found for processing.")
        sys.exit(0)

    # separate the files
    image_files = [f for f in files if f.suffix.lower() in [".jpg", ".jpeg", ".png"]]
    pdf_files = [f for f in files if f.suffix.lower() == ".pdf"]

    print(f"Found: {len(image_files)} images, {len(pdf_files)} PDFs\n")

    # apply limits
    if len(image_files) > MAX_IMAGES:
        print(f"Too many images ({len(image_files)}) - limiting to {MAX_IMAGES}")
        image_files = image_files[:MAX_IMAGES]
    
    if len(pdf_files) > MAX_PDF:
        print(f"Too many PDFs ({len(pdf_files)}) - limiting to {MAX_PDF}")
        pdf_files = pdf_files[:MAX_PDF]

    all_out = []

    # parallel processing of images
    if image_files:
        print("\n" + "="*70)
        print("PROCESSING IMAGES (Parallel)")
        print("="*70)
        
        image_results = process_images_parallel(image_files, max_workers=MAX_WORKERS)
        all_out.extend(image_results)
    
    # process PDFs
    if pdf_files:
        print("\n" + "="*70)
        print("PROCESSING PDFs (Serial)")
        print("="*70)
        
        pdf_results = process_pdfs_parallel(pdf_files)
        all_out.extend(pdf_results)

    # save results
    OUT_JSON.write_text(json.dumps(all_out, ensure_ascii=False, indent=2), encoding="utf-8")
    
    # final summary
    print("\n" + "="*70)
    print("PROCESSING COMPLETED")
    print("="*70)
    print(f"Total files processed: {len(all_out)}")
    print(f"   Images: {len(image_files)}")
    print(f"   PDFs: {len(pdf_files)}")
    print(f"Output: {OUT_JSON}")
    print("="*70)

if __name__ == "__main__":
    main()