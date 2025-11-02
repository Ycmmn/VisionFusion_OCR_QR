# -*- coding: utf-8 -*-
from __future__ import annotations
import cv2
import numpy as np
import re
import os
import json
import socket
import concurrent.futures
import time
from pathlib import Path
from pdf2image import convert_from_path
from PIL import Image
from typing import Union, List, Dict, Any
from urllib.parse import urlparse, unquote
import warnings
import tempfile
import logging

warnings.filterwarnings("ignore")
os.environ["ZBAR_LOG_LEVEL"] = "0"

# =========================================================
# 🔧 Setup Logging
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================================================
# 🔧 Cloud-Ready Paths
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================================================
# 🔧 Dynamic Paths (Session-aware)
# =========================================================
SESSION_DIR = os.environ.get("SESSION_DIR")
if SESSION_DIR:
    SESSION_PATH = Path(SESSION_DIR)
    IMAGES_FOLDER = SESSION_PATH / "uploads"
    OUTPUT_DIR = SESSION_PATH / "data" / "output"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"✅ Using SESSION_DIR: {SESSION_DIR}")
else:
    IMAGES_FOLDER = INPUT_DIR
    logger.info(f"✅ Using default INPUT_DIR: {INPUT_DIR}")

OUTPUT_JSON_RAW = OUTPUT_DIR / "final_superqr_v6_raw.json"
OUTPUT_JSON_CLEAN = OUTPUT_DIR / "final_superqr_v6_clean.json"
DEBUG_DIR = OUTPUT_DIR / "_debug"

os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

logger.info(f"📂 Using IMAGES_FOLDER → {IMAGES_FOLDER}")
logger.info(f"📂 Output JSONs will be saved in → {OUTPUT_DIR}")

# =========================================================
# ⚙️ Configuration
# =========================================================
PDF_IMG_DPI = int(os.getenv("PDF_IMG_DPI", "200"))

# Poppler path (Cloud-compatible)
POPPLER_PATH = os.getenv("POPPLER_PATH", "/usr/bin").strip()
if POPPLER_PATH and os.path.exists(POPPLER_PATH):
    os.environ["PATH"] += os.pathsep + POPPLER_PATH
    logger.info(f"✅ Poppler path set: {POPPLER_PATH}")
else:
    POPPLER_PATH = None
    logger.warning("⚠️ Poppler not found, using system default")

# Debug mode
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"
logger.info("🚀 SuperQR v6.1 (Clean URLs + vCard Support) ready\n")

# =========================================================
# 🔧 QR Library Detection
# =========================================================
try:
    from pyzbar import pyzbar
    HAS_PYZBAR = True
    logger.info("✅ pyzbar loaded")
except ImportError:
    HAS_PYZBAR = False
    logger.warning("⚠️ pyzbar not available")

# Disable pyzxing for cloud (not available)
HAS_ZXING = False
zxing_reader = None
logger.warning("⚠️ pyzxing disabled (not available in cloud)")

# =========================================================
# 📦 Helper Functions
# =========================================================
def clean_url(url):
    """Clean URL and remove extra parts"""
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip()
    
    try:
        parsed = urlparse(url)
        
        # If path has encoded characters, clean it
        if parsed.path and '%' in parsed.path:
            clean = f"{parsed.scheme}://{parsed.netloc}"
            if DEBUG_MODE:
                logger.debug(f"🧹 Cleaned: {url} → {clean}")
            return clean
        
        # Remove query string if exists
        if parsed.query:
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if DEBUG_MODE:
                logger.debug(f"🧹 Cleaned: {url} → {clean}")
            return clean
        
        return url
    except Exception as e:
        if DEBUG_MODE:
            logger.warning(f"⚠️ URL cleaning error: {e}")
        return url

def extract_url_from_vcard(data):
    """Extract URL from vCard"""
    if not data or not isinstance(data, str):
        return None
    
    # Check if it's a vCard
    if not (data.upper().startswith("BEGIN:VCARD") or "VCARD" in data.upper()):
        return None
    
    if DEBUG_MODE:
        logger.debug(f"📇 Detected vCard format")
    
    # Search for URL in vCard
    url_patterns = [
        r"URL[;:]([^\r\n]+)",
        r"URL;[^:]+:([^\r\n]+)",
        r"item\d+\.URL[;:]([^\r\n]+)",
        r"https?://[^\s\r\n]+",
    ]
    
    for pattern in url_patterns:
        matches = re.findall(pattern, data, re.IGNORECASE | re.MULTILINE)
        if matches:
            for match in matches:
                url = match.strip()
                if url.lower().startswith("http"):
                    if DEBUG_MODE:
                        logger.debug(f"✓ Found URL in vCard: {url}")
                    return clean_url(url)
    
    return None

def is_low_contrast(img, sharp_thresh=85, contrast_thresh=25):
    """Check for low image contrast"""
    g = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    sharpness = cv2.Laplacian(g, cv2.CV_64F).var()
    contrast = g.std()
    if DEBUG_MODE:
        logger.debug(f"📊 Sharpness: {sharpness:.1f}, Contrast: {contrast:.1f}")
    return sharpness < sharp_thresh or contrast < contrast_thresh

def enhance_image_aggressive(img):
    """Advanced preprocessing to enhance QR readability"""
    # 1. Denoise
    denoised = cv2.fastNlMeansDenoisingColored(img, None, 10, 10, 7, 21)
    
    # 2. Convert to LAB for better processing
    lab = cv2.cvtColor(denoised, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    
    # 3. Strong CLAHE to enhance contrast
    clahe = cv2.createCLAHE(clipLimit=5.0, tileGridSize=(8, 8))
    l = clahe.apply(l)
    
    # 4. Merge back
    enhanced = cv2.merge([l, a, b])
    enhanced = cv2.cvtColor(enhanced, cv2.COLOR_LAB2BGR)
    
    # 5. Unsharp masking for increased sharpness
    gaussian = cv2.GaussianBlur(enhanced, (0, 0), 3.0)
    enhanced = cv2.addWeighted(enhanced, 2.0, gaussian, -1.0, 0)
    
    # 6. Contrast boost
    enhanced = cv2.convertScaleAbs(enhanced, alpha=1.3, beta=15)
    
    return enhanced

# =========================================================
# 🔍 QR Detection - Advanced Version
# =========================================================
def detect_qr_payloads_enhanced(img, img_name="image"):
    """Detect QR using multiple methods"""
    detector = cv2.QRCodeDetector()
    payloads = []
    methods_tried = 0

    def try_decode(frame, method_name=""):
        nonlocal methods_tried
        methods_tried += 1
        try:
            # Try with detectAndDecode
            val, pts, _ = detector.detectAndDecode(frame)
            if val and val.strip():
                if DEBUG_MODE:
                    logger.debug(f"✓ Found with {method_name}")
                payloads.append(val.strip())
                return True
            
            # If decoding fails but detection succeeds, try again
            if pts is not None and len(pts) > 0:
                val, _ = detector.decode(frame, pts)
                if val and val.strip():
                    if DEBUG_MODE:
                        logger.debug(f"✓ Found with {method_name} (2nd attempt)")
                    payloads.append(val.strip())
                    return True
        except Exception as e:
            if DEBUG_MODE:
                logger.debug(f"✗ {method_name} failed: {e}")
        return False

    if DEBUG_MODE:
        logger.debug(f"🔍 Trying multiple detection methods...")

    # 1. Original image
    try_decode(img, "Original")
    
    # 2. Grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    try_decode(cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR), "Grayscale")
    
    # 3. Adaptive Threshold
    thresh_adapt = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
        cv2.THRESH_BINARY, 51, 10
    )
    try_decode(cv2.cvtColor(thresh_adapt, cv2.COLOR_GRAY2BGR), "Adaptive Threshold")
    
    # 4. Otsu Threshold
    _, thresh_otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    try_decode(cv2.cvtColor(thresh_otsu, cv2.COLOR_GRAY2BGR), "Otsu Threshold")
    
    # 5. Invert image
    try_decode(cv2.bitwise_not(img), "Inverted")
    
    # 6. CLAHE enhancement
    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=4.0, tileGridSize=(8, 8))
    l2 = clahe.apply(l)
    enhanced = cv2.cvtColor(cv2.merge((l2, a, b)), cv2.COLOR_LAB2BGR)
    try_decode(enhanced, "CLAHE")
    
    # 7. Sharpening
    kernel_sharp = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
    sharp = cv2.filter2D(img, -1, kernel_sharp)
    try_decode(sharp, "Sharpened")
    
    # 8. Morphological operations
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
    morph = cv2.morphologyEx(gray, cv2.MORPH_CLOSE, kernel)
    try_decode(cv2.cvtColor(morph, cv2.COLOR_GRAY2BGR), "Morphological")
    
    # 9. Multi-scale
    for scale in [0.5, 0.75, 1.5, 2.0]:
        w = int(img.shape[1] * scale)
        h = int(img.shape[0] * scale)
        if w > 50 and h > 50:
            resized = cv2.resize(img, (w, h), interpolation=cv2.INTER_CUBIC)
            try_decode(resized, f"Scale {scale}x")
    
    # 10. Rotation
    rotation_map = {
        90: cv2.ROTATE_90_CLOCKWISE,
        180: cv2.ROTATE_180,
        270: cv2.ROTATE_90_COUNTERCLOCKWISE
    }
    for angle, rotate_code in rotation_map.items():
        rotated = cv2.rotate(img, rotate_code)
        try_decode(rotated, f"Rotated {angle}°")
    
    # 11. Use pyzbar
    if HAS_PYZBAR:
        for method_img, method_name in [
            (gray, "Pyzbar-Gray"),
            (thresh_adapt, "Pyzbar-Adaptive"),
            (thresh_otsu, "Pyzbar-Otsu")
        ]:
            try:
                barcodes = pyzbar.decode(method_img)
                for barcode in barcodes:
                    data = barcode.data.decode("utf-8", errors="ignore").strip()
                    if data:
                        if DEBUG_MODE:
                            logger.debug(f"✓ Found with {method_name}")
                        payloads.append(data)
            except Exception as e:
                if DEBUG_MODE:
                    logger.debug(f"✗ {method_name} failed: {e}")
    
    # Remove duplicates
    payloads = list(dict.fromkeys(p for p in payloads if p and isinstance(p, str)))
    
    if DEBUG_MODE:
        logger.debug(f"📈 Tried {methods_tried} methods, found {len(payloads)} unique payload(s)")
    
    # Process and extract URLs
    out = []
    for p in payloads:
        # Check if it's a vCard
        vcard_url = extract_url_from_vcard(p)
        if vcard_url:
            out.append(vcard_url)
            continue
        
        # Search for direct URL
        p = p.strip()
        urls = re.findall(r"(https?://[^\s\"'<>\[\]]+|www\.[^\s\"'<>\[\]]+)", p, re.IGNORECASE)
        
        if urls:
            for url in urls:
                url = url.strip()
                # Remove extra characters from the end
                url = re.sub(r'[,;.!?\)\]]+$', '', url)
                
                if not url.lower().startswith("http"):
                    url = "https://" + url.lower()
                
                # Clean URL
                cleaned = clean_url(url)
                if cleaned:
                    out.append(cleaned)
        elif re.search(r"(HTTPS?://|WWW\.)", p.upper()):
            if not p.lower().startswith("http"):
                p = "https://" + p.lower()
            cleaned = clean_url(p)
            if cleaned:
                out.append(cleaned)
    
    # Remove duplicate URLs
    out = list(dict.fromkeys(out))
    
    return out if out else None

# =========================================================
# 🖼️ Image Processing
# =========================================================
def process_image_for_qr(image_path: Path) -> Union[List[str], None]:
    """Process image for QR detection"""
    if DEBUG_MODE:
        logger.debug(f"\n🖼️  Loading: {image_path.name}")
    
    img = cv2.imread(str(image_path))
    if img is None:
        logger.error(f"❌ Cannot read {image_path.name}")
        return None
    
    if DEBUG_MODE:
        logger.debug(f"📐 Size: {img.shape[1]}x{img.shape[0]}")
        cv2.imwrite(str(DEBUG_DIR / f"{image_path.stem}_01_original.jpg"), img)
    
    # Enhancement
    enhanced = enhance_image_aggressive(img)
    
    if DEBUG_MODE:
        cv2.imwrite(str(DEBUG_DIR / f"{image_path.stem}_02_enhanced.jpg"), enhanced)
    
    # QR detection
    result = detect_qr_payloads_enhanced(enhanced, image_path.stem)
    
    if result:
        logger.info(f"✅ Found {len(result)} clean URL(s)")
        for i, qr in enumerate(result, 1):
            logger.info(f"   {i}. {qr}")
    else:
        logger.warning(f"⚠️  No QR code detected")
    
    return result

# =========================================================
# 📄 PDF Processing
# =========================================================
def process_pdf_for_qr(pdf_path: Path) -> Dict[str, Any]:
    """Process PDF and convert to images"""
    logger.info(f"\n📄 Processing PDF: {pdf_path.name}")
    temp_dir = OUTPUT_DIR / "_pdf_pages"
    os.makedirs(temp_dir, exist_ok=True)
    
    kwargs = {}
    if POPPLER_PATH and os.path.exists(POPPLER_PATH):
        kwargs["poppler_path"] = POPPLER_PATH
    
    try:
        images = convert_from_path(pdf_path, dpi=PDF_IMG_DPI, **kwargs)
    except Exception as e:
        logger.error(f"❌ PDF conversion failed: {e}")
        if "poppler" in str(e).lower():
            logger.info(f"💡 Hint: Install Poppler and set POPPLER_PATH environment variable")
        return {
            "file_id": pdf_path.stem,
            "file_name": pdf_path.name,
            "error": str(e),
            "result": []
        }
    
    total_pages = len(images)
    logger.info(f"📑 Total pages: {total_pages}")
    results = []

    for i, img in enumerate(images, start=1):
        page_image_path = temp_dir / f"{pdf_path.stem}_page_{i:03d}.jpg"
        img.save(page_image_path, "JPEG", quality=95)
        logger.info(f"\n🧩 Page {i}/{total_pages}")

        qr_links = process_image_for_qr(page_image_path)
        page_result = {"page": i, "qr_link": qr_links[0] if qr_links else None}
        results.append(page_result)

    return {"file_id": pdf_path.stem, "file_name": pdf_path.name, "result": results}

def process_image_file(image_path: Path) -> Dict[str, Any]:
    """Process image file"""
    qr_links = process_image_for_qr(image_path)
    return {
        "file_id": image_path.stem,
        "file_name": image_path.name,
        "result": [{"page": 1, "qr_link": qr_links[0] if qr_links else None}]
    }

# =========================================================
# 💾 Save JSON
# =========================================================
def save_json(path, data):
    """Save JSON with proper encoding"""
    path.parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        json.dumps(data, indent=4, ensure_ascii=False), 
        encoding="utf-8"
    )

# =========================================================
# 🧹 Clean and Validate URLs
# =========================================================
def extract_urls(entry):
    """Extract URLs from results"""
    urls = []
    for item in entry.get("result", []):
        link = item.get("qr_link")
        if link:
            urls.append(link)
    return list(dict.fromkeys(urls))

def is_domain_alive(url, timeout=5):
    """Check if domain is live"""
    try:
        host = re.sub(r"^https?://(www\.)?", "", url).split("/")[0]
        socket.setdefaulttimeout(timeout)
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def clean_qr_json(input_file, output_file):
    """Clean and validate URLs"""
    logger.info("\n🧹 Cleaning and validating extracted QR URLs...")
    
    if not Path(input_file).exists():
        logger.error(f"❌ Input file not found: {input_file}")
        return
    
    data = json.loads(Path(input_file).read_text(encoding="utf-8"))
    final_results = []
    
    for entry in data:
        if "error" in entry:
            final_results.append(entry)
            continue
            
        urls = extract_urls(entry)
        valid_urls = []
        
        if urls:
            logger.info(f"🔍 Validating {len(urls)} URL(s) from {entry.get('file_name')}...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(is_domain_alive, u): u for u in urls}
                for f in concurrent.futures.as_completed(futures):
                    u = futures[f]
                    try:
                        if f.result():
                            valid_urls.append(u)
                            logger.info(f"   ✅ {u}")
                        else:
                            logger.warning(f"   ❌ {u} (domain unreachable)")
                    except Exception as e:
                        logger.warning(f"   ⚠️  {u} (check failed: {e})")
        
        result_pages = []
        for item in entry.get("result", []):
            page = item.get("page", 1)
            link = item.get("qr_link")
            
            if link and link in valid_urls:
                result_pages.append({"page": page, "qr_link": link})
            else:
                result_pages.append({"page": page, "qr_link": None})
        
        final_results.append({
            "file_id": entry.get("file_id"),
            "file_name": entry.get("file_name"),
            "result": result_pages
        })
    
    save_json(output_file, final_results)
    logger.info(f"\n✅ Cleaned results saved → {output_file}")

# =========================================================
# 🚀 Main Function
# =========================================================
def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("🚀 Starting SuperQR v6.1 Processing")
    logger.info("=" * 60)
    
    results = []
    files = sorted([
        f for f in Path(IMAGES_FOLDER).rglob("*")
        if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".pdf"]
        and "_pdf_pages" not in str(f)
        and "_debug" not in str(f)
    ])
    
    if not files:
        logger.warning(f"\n⚠️  No image/PDF files found in {IMAGES_FOLDER}")
        logger.info("   Supported formats: .jpg, .jpeg, .png, .pdf")
        return False
    
    logger.info(f"\n📂 Found {len(files)} file(s) to process\n")

    for idx, f in enumerate(files, 1):
        logger.info("=" * 60)
        logger.info(f"🔎 [{idx}/{len(files)}] Processing: {f.name}")
        logger.info("=" * 60)
        start_time = time.time()
        
        try:
            if f.suffix.lower() == ".pdf":
                res = process_pdf_for_qr(f)
            else:
                res = process_image_file(f)
            
            results.append(res)
            elapsed = time.time() - start_time
            logger.info(f"\n✅ Completed {f.name} in {elapsed:.1f}s")
            
        except Exception as e:
            logger.error(f"\n❌ Error processing {f.name}: {e}")
            import traceback
            if DEBUG_MODE:
                logger.error(traceback.format_exc())
            results.append({
                "file_id": f.stem,
                "file_name": f.name,
                "error": str(e),
                "result": []
            })
    
    # Save raw results
    logger.info("\n" + "=" * 60)
    save_json(OUTPUT_JSON_RAW, results)
    logger.info(f"✅ Raw results saved → {OUTPUT_JSON_RAW}")
    
    # Clean and validate
    clean_qr_json(OUTPUT_JSON_RAW, OUTPUT_JSON_CLEAN)
    
    logger.info("\n" + "=" * 60)
    logger.info(f"✨ Processing completed!")
    logger.info(f"📊 Final output → {OUTPUT_JSON_CLEAN}")
    logger.info("=" * 60)
    
    # Summary
    total_qr = sum(
        1 for entry in results 
        for item in entry.get("result", []) 
        if item.get("qr_link")
    )
    logger.info(f"\n📈 Summary: Found {total_qr} QR code(s) in {len(files)} file(s)")
    
    if DEBUG_MODE:
        logger.info(f"🐛 Debug images saved in: {DEBUG_DIR}")
    
    return True

def run_qr_detection():
    """QR detection wrapper"""
    logger.info("📷 Starting QR detection...")
    success = main()
    return str(OUTPUT_DIR / "final_superqr_v6_clean.json") if success else None

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)