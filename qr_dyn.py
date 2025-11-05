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
import warnings, ctypes, os
warnings.filterwarnings("ignore")
os.environ["ZBAR_LOG_LEVEL"] = "0"

# =========================================================
# 🧩 مسیرهای داینامیک سشن (Dynamic Paths)
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))

# ورودی‌ها: اگر uploads خالی بود، مسیر خود SESSION_DIR
IMAGES_FOLDER = SESSION_DIR / "uploads"
if not IMAGES_FOLDER.exists() or not any(IMAGES_FOLDER.glob("*")):
    IMAGES_FOLDER = SESSION_DIR
print(f"📂 Using IMAGES_FOLDER → {IMAGES_FOLDER}")

# خروجی‌ها (داینامیک)
OUTPUT_JSON_RAW = Path(os.getenv("QR_RAW_JSON", SESSION_DIR / "final_superqr_v6_raw.json"))
OUTPUT_JSON_CLEAN = Path(os.getenv("QR_CLEAN_JSON", SESSION_DIR / "final_superqr_v6_clean.json"))
DEBUG_DIR = SESSION_DIR / "_debug"
os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

# DPI برای PDF
PDF_IMG_DPI = int(os.getenv("PDF_IMG_DPI", "200"))

# مسیر Poppler (برای ویندوز)
POPPLER_PATH = os.getenv("POPPLER_PATH", r"C:\poppler\Library\bin").strip()
if POPPLER_PATH and os.path.exists(POPPLER_PATH):
    os.environ["PATH"] += os.pathsep + POPPLER_PATH

# حالت دیباگ
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"
print("🚀 SuperQR v6.1 (Clean URLs + vCard Support) ready\n")

# ----------------------------------------------------------
# QR fallbacks
# ----------------------------------------------------------
try:
    from pyzbar import pyzbar
    HAS_PYZBAR = True
    print("✅ pyzbar loaded")
except ImportError:
    HAS_PYZBAR = False
    print("⚠️ pyzbar not available")

try:
    from pyzxing import BarCodeReader
    zxing_reader = BarCodeReader()
    HAS_ZXING = True
    print("✅ pyzxing loaded")
except ImportError:
    HAS_ZXING = False
    print("⚠️ pyzxing not available")

# ----------------------------------------------------------
def clean_url(url):
    """تمیز کردن URL و حذف قسمت‌های اضافی"""
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip()
    
    # اگر URL شامل کاراکترهای encode شده است، decode کنیم
    try:
        # فقط domain و path اصلی را نگه می‌داریم
        parsed = urlparse(url)
        
        # اگر path دارد و encode شده، تمیز می‌کنیم
        if parsed.path and '%' in parsed.path:
            # فقط domain + / را برمی‌گردانیم
            clean = f"{parsed.scheme}://{parsed.netloc}"
            if DEBUG_MODE:
                print(f"      🧹 Cleaned: {url} → {clean}")
            return clean
        
        # اگر query string دارد، حذف می‌کنیم
        if parsed.query:
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if DEBUG_MODE:
                print(f"      🧹 Cleaned: {url} → {clean}")
            return clean
        
        return url
    except Exception as e:
        if DEBUG_MODE:
            print(f"      ⚠️ URL cleaning error: {e}")
        return url

def extract_url_from_vcard(data):
    """استخراج URL از vCard"""
    if not data or not isinstance(data, str):
        return None
    
    # بررسی اینکه آیا vCard است
    if not (data.upper().startswith("BEGIN:VCARD") or "VCARD" in data.upper()):
        return None
    
    if DEBUG_MODE:
        print(f"      📇 Detected vCard format")
    
    # جستجوی URL در vCard
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
                        print(f"      ✓ Found URL in vCard: {url}")
                    return clean_url(url)
    
    return None

def is_low_contrast(img, sharp_thresh=85, contrast_thresh=25):
    """بررسی کنتراست پایین تصویر"""
    g = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    sharpness = cv2.Laplacian(g, cv2.CV_64F).var()
    contrast = g.std()
    if DEBUG_MODE:
        print(f"   📊 Sharpness: {sharpness:.1f}, Contrast: {contrast:.1f}")
    return sharpness < sharp_thresh or contrast < contrast_thresh

def enhance_image_aggressive(img):
    """پیش‌پردازش قوی برای بهبود خوانایی QR"""
    # 1. Denoise
    denoised = cv2.fastNlMeansDenoisingColored(img, None, 10, 10, 7, 21)
    
    # 2. Convert to LAB for better processing
    lab = cv2.cvtColor(denoised, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    
    # 3. CLAHE قوی برای افزایش کنتراست
    clahe = cv2.createCLAHE(clipLimit=5.0, tileGridSize=(8, 8))
    l = clahe.apply(l)
    
    # 4. Merge back
    enhanced = cv2.merge([l, a, b])
    enhanced = cv2.cvtColor(enhanced, cv2.COLOR_LAB2BGR)
    
    # 5. Unsharp masking برای وضوح بیشتر
    gaussian = cv2.GaussianBlur(enhanced, (0, 0), 3.0)
    enhanced = cv2.addWeighted(enhanced, 2.0, gaussian, -1.0, 0)
    
    # 6. Contrast boost
    enhanced = cv2.convertScaleAbs(enhanced, alpha=1.3, beta=15)
    
    return enhanced

# ----------------------------------------------------------
# 🔍 QR Detection - نسخه پیشرفته
# ----------------------------------------------------------
def detect_qr_payloads_enhanced(img, img_name="image"):
    """تشخیص QR با چندین روش مختلف"""
    detector = cv2.QRCodeDetector()
    payloads = []
    methods_tried = 0

    def try_decode(frame, method_name=""):
        nonlocal methods_tried
        methods_tried += 1
        try:
            # تلاش با detectAndDecode
            val, pts, _ = detector.detectAndDecode(frame)
            if val and val.strip():
                if DEBUG_MODE:
                    print(f"      ✓ Found with {method_name}")
                payloads.append(val.strip())
                return True
            
            # اگر نتوانست decode کند ولی detect کرد، تلاش مجدد
            if pts is not None and len(pts) > 0:
                val, _ = detector.decode(frame, pts)
                if val and val.strip():
                    if DEBUG_MODE:
                        print(f"      ✓ Found with {method_name} (2nd attempt)")
                    payloads.append(val.strip())
                    return True
        except Exception as e:
            if DEBUG_MODE:
                print(f"      ✗ {method_name} failed: {e}")
        return False

    if DEBUG_MODE:
        print(f"   🔍 Trying multiple detection methods...")

    # 1. تصویر اصلی
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
    
    # 5. معکوس تصویر
    try_decode(cv2.bitwise_not(img), "Inverted")
    
    # 6. CLAHE enhancement
    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=4.0, tileGridSize=(8, 8))
    l2 = clahe.apply(l)
    enhanced = cv2.cvtColor(cv2.merge((l2, a, b)), cv2.COLOR_LAB2BGR)
    try_decode(enhanced, "CLAHE")
    
    # 7. Sharpening قوی
    kernel_sharp = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
    sharp = cv2.filter2D(img, -1, kernel_sharp)
    try_decode(sharp, "Sharpened")
    
    # 8. Morphological operations
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
    morph = cv2.morphologyEx(gray, cv2.MORPH_CLOSE, kernel)
    try_decode(cv2.cvtColor(morph, cv2.COLOR_GRAY2BGR), "Morphological")
    
    # 9. Multi-scale (مقیاس‌های مختلف)
    for scale in [0.5, 0.75, 1.5, 2.0]:
        w = int(img.shape[1] * scale)
        h = int(img.shape[0] * scale)
        if w > 50 and h > 50:
            resized = cv2.resize(img, (w, h), interpolation=cv2.INTER_CUBIC)
            try_decode(resized, f"Scale {scale}x")
    
    # 10. Rotation (چرخش)
    rotation_map = {
        90: cv2.ROTATE_90_CLOCKWISE,
        180: cv2.ROTATE_180,
        270: cv2.ROTATE_90_COUNTERCLOCKWISE
    }
    for angle, rotate_code in rotation_map.items():
        rotated = cv2.rotate(img, rotate_code)
        try_decode(rotated, f"Rotated {angle}°")
    
    # 11. استفاده از pyzbar
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
                            print(f"      ✓ Found with {method_name}")
                        payloads.append(data)
            except Exception as e:
                if DEBUG_MODE:
                    print(f"      ✗ {method_name} failed: {e}")
    
    # 12. استفاده از zxing
    if HAS_ZXING:
        try:
            temp_path = DEBUG_DIR / f"_temp_zxing_{img_name}.jpg"
            cv2.imwrite(str(temp_path), img)
            results = zxing_reader.decode(str(temp_path), try_harder=True)
            
            if results:
                if isinstance(results, list):
                    for res in results:
                        txt = res.get("parsed", "") or res.get("raw", "")
                        if txt:
                            if DEBUG_MODE:
                                print(f"      ✓ Found with ZXing")
                            payloads.append(txt.strip())
                elif isinstance(results, dict):
                    txt = results.get("parsed", "") or results.get("raw", "")
                    if txt:
                        if DEBUG_MODE:
                            print(f"      ✓ Found with ZXing")
                        payloads.append(txt.strip())
            
            temp_path.unlink(missing_ok=True)
        except Exception as e:
            if DEBUG_MODE:
                print(f"      ✗ ZXing failed: {e}")
    
    # حذف تکراری‌ها
    payloads = list(dict.fromkeys(p for p in payloads if p and isinstance(p, str)))
    
    if DEBUG_MODE:
        print(f"   📈 Tried {methods_tried} methods, found {len(payloads)} unique payload(s)")
    
    # پردازش و استخراج URL
    out = []
    for p in payloads:
        # بررسی اینکه آیا vCard است
        vcard_url = extract_url_from_vcard(p)
        if vcard_url:
            out.append(vcard_url)
            continue
        
        # جستجوی URL مستقیم
        p = p.strip()
        urls = re.findall(r"(https?://[^\s\"'<>\[\]]+|www\.[^\s\"'<>\[\]]+)", p, re.IGNORECASE)
        
        if urls:
            for url in urls:
                url = url.strip()
                # حذف کاراکترهای اضافی از انتها
                url = re.sub(r'[,;.!?\)\]]+$', '', url)
                
                if not url.lower().startswith("http"):
                    url = "https://" + url.lower()
                
                # تمیز کردن URL
                cleaned = clean_url(url)
                if cleaned:
                    out.append(cleaned)
        elif re.search(r"(HTTPS?://|WWW\.)", p.upper()):
            if not p.lower().startswith("http"):
                p = "https://" + p.lower()
            cleaned = clean_url(p)
            if cleaned:
                out.append(cleaned)
    
    # حذف تکراری URL
    out = list(dict.fromkeys(out))
    
    return out if out else None

# ----------------------------------------------------------
def process_image_for_qr(image_path: Path) -> Union[List[str], None]:
    """پردازش تصویر برای تشخیص QR"""
    if DEBUG_MODE:
        print(f"\n   🖼️  Loading: {image_path.name}")
    
    img = cv2.imread(str(image_path))
    if img is None:
        print(f"   ❌ Cannot read {image_path.name}")
        return None
    
    if DEBUG_MODE:
        print(f"   📐 Size: {img.shape[1]}x{img.shape[0]}")
        cv2.imwrite(str(DEBUG_DIR / f"{image_path.stem}_01_original.jpg"), img)
    
    # بررسی کنتراست
    low = is_low_contrast(img)
    
    # Enhancement
    enhanced = enhance_image_aggressive(img)
    
    if DEBUG_MODE:
        cv2.imwrite(str(DEBUG_DIR / f"{image_path.stem}_02_enhanced.jpg"), enhanced)
    
    # تشخیص QR
    result = detect_qr_payloads_enhanced(enhanced, image_path.stem)
    
    if result:
        print(f"   ✅ Found {len(result)} clean URL(s)")
        for i, qr in enumerate(result, 1):
            print(f"      {i}. {qr}")
    else:
        print(f"   ⚠️  No QR code detected")
    
    return result

# ----------------------------------------------------------
def process_pdf_for_qr(pdf_path: Path) -> Dict[str, Any]:
    """پردازش PDF و تبدیل به تصویر"""
    print(f"\n📄 Processing PDF: {pdf_path.name}")
    temp_dir = SESSION_DIR / "_pdf_pages"
    os.makedirs(temp_dir, exist_ok=True)
    
    kwargs = {}
    if POPPLER_PATH and os.path.exists(POPPLER_PATH):
        kwargs["poppler_path"] = POPPLER_PATH
    
    try:
        images = convert_from_path(pdf_path, dpi=PDF_IMG_DPI, **kwargs)
    except Exception as e:
        print(f"   ❌ PDF conversion failed: {e}")
        if "poppler" in str(e).lower():
            print(f"   💡 Hint: Install Poppler and set POPPLER_PATH environment variable")
        return {
            "file_id": pdf_path.stem,
            "file_name": pdf_path.name,
            "error": str(e),
            "result": []
        }
    
    total_pages = len(images)
    print(f"   📑 Total pages: {total_pages}")
    results = []

    for i, img in enumerate(images, start=1):
        page_image_path = temp_dir / f"{pdf_path.stem}_page_{i:03d}.jpg"
        img.save(page_image_path, "JPEG", quality=95)
        print(f"\n   🧩 Page {i}/{total_pages}")

        qr_links = process_image_for_qr(page_image_path)
        page_result = {"page": i, "qr_link": qr_links[0] if qr_links else None}
        results.append(page_result)

    return {"file_id": pdf_path.stem, "file_name": pdf_path.name, "result": results}

# ----------------------------------------------------------
def process_image_file(image_path: Path) -> Dict[str, Any]:
    """پردازش فایل تصویری"""
    qr_links = process_image_for_qr(image_path)
    return {
        "file_id": image_path.stem,
        "file_name": image_path.name,
        "result": [{"page": 1, "qr_link": qr_links[0] if qr_links else None}]
    }

# ----------------------------------------------------------
def save_json(path, data):
    """ذخیره JSON با encoding مناسب"""
    Path(path).write_text(
        json.dumps(data, indent=4, ensure_ascii=False), 
        encoding="utf-8"
    )

# ----------------------------------------------------------
def extract_urls(entry):
    """استخراج URLها از نتایج"""
    urls = []
    for item in entry.get("result", []):
        link = item.get("qr_link")
        if link:
            urls.append(link)
    return list(dict.fromkeys(urls))

def is_domain_alive(url, timeout=5):
    """بررسی زنده بودن دامنه"""
    try:
        host = re.sub(r"^https?://(www\.)?", "", url).split("/")[0]
        socket.setdefaulttimeout(timeout)
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def clean_qr_json(input_file, output_file):
    """پاکسازی و اعتبارسنجی URLها"""
    print("\n🧹 Cleaning and validating extracted QR URLs...")
    
    if not Path(input_file).exists():
        print(f"   ❌ Input file not found: {input_file}")
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
            print(f"   🔍 Validating {len(urls)} URL(s) from {entry.get('file_name')}...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(is_domain_alive, u): u for u in urls}
                for f in concurrent.futures.as_completed(futures):
                    u = futures[f]
                    try:
                        if f.result():
                            valid_urls.append(u)
                            print(f"      ✅ {u}")
                        else:
                            print(f"      ❌ {u} (domain unreachable)")
                    except Exception as e:
                        print(f"      ⚠️  {u} (check failed: {e})")
        
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
    print(f"\n✅ Cleaned results saved → {output_file}")

# ----------------------------------------------------------
def main():
    """تابع اصلی"""
    print("=" * 60)
    print("🚀 Starting SuperQR v6.1 Processing")
    print("=" * 60)
    
    results = []
    files = sorted([
        f for f in Path(IMAGES_FOLDER).rglob("*")
        if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".pdf"]
        and "_pdf_pages" not in str(f)
        and "_debug" not in str(f)
    ])
    
    if not files:
        print(f"\n⚠️  No image/PDF files found in {IMAGES_FOLDER}")
        print("   Supported formats: .jpg, .jpeg, .png, .pdf")
        return
    
    print(f"\n📂 Found {len(files)} file(s) to process\n")

    for idx, f in enumerate(files, 1):
        print("=" * 60)
        print(f"🔎 [{idx}/{len(files)}] Processing: {f.name}")
        print("=" * 60)
        start_time = time.time()
        
        try:
            if f.suffix.lower() == ".pdf":
                res = process_pdf_for_qr(f)
            else:
                res = process_image_file(f)
            
            results.append(res)
            elapsed = time.time() - start_time
            print(f"\n✅ Completed {f.name} in {elapsed:.1f}s")
            
        except Exception as e:
            print(f"\n❌ Error processing {f.name}: {e}")
            import traceback
            if DEBUG_MODE:
                traceback.print_exc()
            results.append({
                "file_id": f.stem,
                "file_name": f.name,
                "error": str(e),
                "result": []
            })
    
    # ذخیره نتایج خام
    print("\n" + "=" * 60)
    save_json(OUTPUT_JSON_RAW, results)
    print(f"✅ Raw results saved → {OUTPUT_JSON_RAW}")
    
    # پاکسازی و اعتبارسنجی
    clean_qr_json(OUTPUT_JSON_RAW, OUTPUT_JSON_CLEAN)
    
    print("\n" + "=" * 60)
    print(f"✨ Processing completed!")
    print(f"📊 Final output → {OUTPUT_JSON_CLEAN}")
    print("=" * 60)
    
    # خلاصه نتایج
    total_qr = sum(
        1 for entry in results 
        for item in entry.get("result", []) 
        if item.get("qr_link")
    )
    print(f"\n📈 Summary: Found {total_qr} QR code(s) in {len(files)} file(s)")
    
    if DEBUG_MODE:
        print(f"🐛 Debug images saved in: {DEBUG_DIR}")

# ----------------------------------------------------------
if __name__ == "__main__":
    main()