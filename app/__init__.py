"""
───────────────────────────────────────────────────────────────
 Yabix VisionFusion OCR + QR Intelligence Suite
───────────────────────────────────────────────────────────────
 An AI-powered document automation toolkit combining:
   • OCR extraction via Google Gemini
   • Advanced multi-engine QR decoding
   • Web scraping and content enrichment
   • Excel-based data merging and deduplication
   • Streamlit interface for real-time automation

 Modules:
   - ocr_dyn.py        → Intelligent OCR extraction
   - qr_dyn.py         → Multi-engine QR code decoding
   - mix_ocr_qr.py     → Merge OCR & QR results into unified JSON
   - scrap.py          → AI web scraping & translation
   - excel_mode.py     → Excel enrichment mode
   - final_mix.py      → Final data fusion and cleaning

 Author:  Yabix AI
 Email:   yasa.aidv@gmail.com
 Version: 1.0.0
 License: Proprietary
───────────────────────────────────────────────────────────────
"""

__version__ = "1.0.0"
__author__ = "Yabix AI"
__email__ = "yasa.aidv@gmail.com"
__license__ = "Proprietary"

# Expose key modules for direct import
from .ocr_dyn import *
from .qr_dyn import *
from .mix_ocr_qr import *
from .scrap import *
from .excel_mode import *
from .final_mix import *

# Optional convenience list
__all__ = [
    "ocr_dyn",
    "qr_dyn",
    "mix_ocr_qr",
    "scrap",
    "excel_mode",
    "final_mix"
]
