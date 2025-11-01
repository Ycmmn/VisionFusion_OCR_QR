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