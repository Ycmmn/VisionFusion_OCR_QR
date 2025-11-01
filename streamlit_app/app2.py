# -*- coding: utf-8 -*-
"""
🎯 smart exhibition pipeline — final unified edition + google sheets  
a full merge of the two apps: "ultimate smart exhibition pipeline" + "smart data pipeline"  
- cool ui from version 1 + logic, logging, and quota management from version 2  
- excel mode and ocr/qr mode with auto detection  
- smart metadata injection (exhibition + source + smart position)  
- fast mode, debug mode, rate limiting, daily quota  
- ✨ batch processing: images (5), pdfs (4), excel (1)  
- ✨ quality control tracking: user name, role, date, time  
- ☁️ google sheets integration: auto-save data to google drive  

run:  
    streamlit run smart_exhibition_pipeline_final.py

"""

import streamlit as st
import subprocess
import os
import sys
import json
import time
import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import re
import shutil

from supabase import create_client, Client