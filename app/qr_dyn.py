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