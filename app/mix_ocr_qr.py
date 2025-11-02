# -*- coding: utf-8 -*-
"""
🌐 OCR + QR Merger - Cloud-Ready Version
Compatible with: Streamlit Cloud, Render, Railway
"""

from pathlib import Path
import os
import json
import tempfile
import logging
from typing import List, Dict, Any, Optional

# =========================================================
# 🔧 Cloud-Ready Configuration
# =========================================================

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_session_dir() -> Path:
    """
    Get session directory - cloud compatible
    Uses temp directory if SESSION_DIR not set
    """
    session_dir = os.getenv("SESSION_DIR")
    
    if session_dir:
        path = Path(session_dir)
    else:
        # Cloud environment - use temp directory
        path = Path(tempfile.gettempdir()) / "exhibition_session"
    
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_file_path(env_var: str, default_name: str) -> Path:
    """
    Get file path with cloud fallback
    """
    custom_path = os.getenv(env_var)
    
    if custom_path and Path(custom_path).exists():
        return Path(custom_path)
    
    # Fallback to session directory
    session_dir = get_session_dir()
    return session_dir / default_name

# File paths
SESSION_DIR = get_session_dir()
OCR_FILE = get_file_path("OCR_FILE", "gemini_output.json")
QR_FILE = get_file_path("QR_FILE", "final_superqr_v6_clean.json")
OUTPUT_FILE = get_file_path("OUTPUT_FILE", "mix_ocr_qr.json")

logger.info(f"📂 Session Directory: {SESSION_DIR}")
logger.info(f"📥 OCR File: {OCR_FILE}")
logger.info(f"📥 QR File: {QR_FILE}")
logger.info(f"📤 Output File: {OUTPUT_FILE}")

# =========================================================
# 📦 Helper Functions
# =========================================================

def read_json(path: Path) -> List[Dict[str, Any]]:
    """
    Safely read JSON file with error handling
    
    Args:
        path: Path to JSON file
        
    Returns:
        List of data or empty list on error
    """
    try:
        if not path.exists():
            logger.warning(f"⚠️ File not found: {path}")
            return []
        
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"✅ Loaded {len(data)} items from {path.name}")
        return data
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error in {path}: {e}")
        return []
    except Exception as e:
        logger.error(f"❌ Error reading {path}: {e}")
        return []

def write_json(path: Path, data: List[Dict[str, Any]]) -> bool:
    """
    Safely write JSON file
    
    Args:
        path: Output path
        data: Data to write
        
    Returns:
        True if successful
    """
    try:
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ Saved {len(data)} items to {path.name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error writing {path}: {e}")
        return False

def merge_single_image(item: Dict[str, Any], qr_result: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge data for single image file
    
    Args:
        item: OCR data item
        qr_result: QR detection results
        
    Returns:
        Merged item
    """
    qr_links = [p.get("qr_link") for p in qr_result if p.get("qr_link")]
    
    if isinstance(item.get("result"), dict):
        item["result"]["qr_links"] = qr_links if qr_links else None
    else:
        item["result"] = {"qr_links": qr_links if qr_links else None}
    
    return item

def merge_pdf_pages(item: Dict[str, Any], qr_result: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge data for multi-page PDF
    
    Args:
        item: OCR data item
        qr_result: QR detection results per page
        
    Returns:
        Merged item with page-level QR links
    """
    if not isinstance(item.get("result"), list):
        logger.warning(f"⚠️ PDF result is not a list: {item.get('file_name')}")
        return item
    
    for page_obj in item["result"]:
        page_num = page_obj.get("page")
        
        # Find matching QR result for this page
        qr_match = next(
            (p.get("qr_link") for p in qr_result if p.get("page") == page_num),
            None
        )
        
        page_obj["qr_link"] = qr_match
    
    return item

def merge_ocr_qr(ocr_data: List[Dict[str, Any]], qr_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Main merge function for OCR and QR data
    
    Args:
        ocr_data: List of OCR results
        qr_data: List of QR detection results
        
    Returns:
        Merged data list
    """
    # Create lookup dictionary for QR results
    qr_lookup = {
        item["file_name"]: item.get("result", [])
        for item in qr_data
        if "file_name" in item
    }
    
    merged = []
    processed_count = 0
    skipped_count = 0
    
    for item in ocr_data:
        file_name = item.get("file_name")
        
        if not file_name:
            logger.warning(f"⚠️ Item without file_name, skipping")
            skipped_count += 1
            continue
        
        qr_result = qr_lookup.get(file_name, [])
        
        # Determine file type and merge accordingly
        file_ext = file_name.lower().split('.')[-1]
        
        if file_ext in ['jpg', 'jpeg', 'png', 'webp', 'bmp', 'gif']:
            # Image file
            item = merge_single_image(item, qr_result)
            processed_count += 1
            
        elif file_ext == 'pdf':
            # PDF file
            item = merge_pdf_pages(item, qr_result)
            processed_count += 1
            
        else:
            # Unknown format
            logger.warning(f"⚠️ Unknown file format: {file_name}")
            item["result"] = item.get("result", {})
            item["result"]["qr_links"] = None
            skipped_count += 1
        
        merged.append(item)
    
    logger.info(f"📊 Processed: {processed_count}, Skipped: {skipped_count}")
    
    return merged

def cleanup_temp_files(keep_output: bool = True):
    """
    Clean up temporary files (for cloud memory management)
    
    Args:
        keep_output: Keep the final output file
    """
    try:
        if keep_output:
            files_to_remove = [OCR_FILE, QR_FILE]
        else:
            files_to_remove = [OCR_FILE, QR_FILE, OUTPUT_FILE]
        
        for file_path in files_to_remove:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"🗑️ Removed temp file: {file_path.name}")
                
    except Exception as e:
        logger.warning(f"⚠️ Cleanup error: {e}")

# =========================================================
# 🚀 Main Execution
# =========================================================

def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("🚀 Starting OCR + QR merge process (Cloud Mode)")
    logger.info("=" * 60)
    
    # Read input files
    ocr_data = read_json(OCR_FILE)
    qr_data = read_json(QR_FILE)
    
    # Validate inputs
    if not ocr_data and not qr_data:
        logger.error("❌ Both OCR and QR files are empty or missing!")
        return False
    
    if not ocr_data:
        logger.warning("⚠️ OCR file is empty - continuing with QR data only")
    
    if not qr_data:
        logger.warning("⚠️ QR file is empty - continuing with OCR data only")
    
    logger.info(f"📄 Loaded OCR: {len(ocr_data)} items")
    logger.info(f"🔗 Loaded QR: {len(qr_data)} items")
    
    # Perform merge
    try:
        merged_results = merge_ocr_qr(ocr_data, qr_data)
        
        if not merged_results:
            logger.error("❌ Merge resulted in empty data!")
            return False
        
        logger.info(f"✅ Merged {len(merged_results)} items")
        
    except Exception as e:
        logger.error(f"❌ Merge error: {e}")
        return False
    
    # Write output
    success = write_json(OUTPUT_FILE, merged_results)
    
    if success:
        logger.info("=" * 60)
        logger.info("✅ Merge completed successfully!")
        logger.info(f"📁 Output saved to: {OUTPUT_FILE}")
        logger.info(f"📊 Total records: {len(merged_results)}")
        logger.info("=" * 60)
        
        # Optional: Cleanup temp files to save memory
        # Uncomment if running in memory-constrained environment
        # cleanup_temp_files(keep_output=True)
        
        return True
    else:
        logger.error("❌ Failed to save output file!")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)