# -*- coding: utf-8 -*-
from pathlib import Path
import os
import json

# =========================================================
#  dynamic paths
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
SOURCE_FOLDER = Path(os.getenv("SOURCE_FOLDER", SESSION_DIR / "uploads"))
RENAMED_DIR = Path(os.getenv("RENAMED_DIR", SESSION_DIR / "renamed"))