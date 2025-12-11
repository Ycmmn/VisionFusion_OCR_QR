# -*- coding: utf-8 -*-
"""
smart exhibition pipeline — final unified edition + google sheets
complete merge of two apps: «ultimate smart exhibition pipeline» + «smart data pipeline»
- awesome ui from version 1 + logic and logging and quota management from version 2
- excel mode and ocr/qr mode with automatic detection
- smart metadata injection (exhibition + source + smart position)
- fast mode, debug mode, rate limiting, daily quota
- batch processing: images(5), pdfs(4), excel(1)
- quality control tracking: user name, role, date, time
- google sheets integration: automatic data saving to google drive

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


# --------------------------------page settings
st.set_page_config(
    page_title="Smart Exhibition Pipeline",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)



 # ---------------------------Permanent Google Sheets Link (Always Visible)
 FIXED_SHEET_URL = "https://docs.google.com/spreadsheets/d/1OeQbiqvo6v58rcxaoSUidOk0IxSGmL8YCpLnyh27yuE/edit"

st.markdown(f"""
<div style="
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1.2rem; border-radius: 15px; text-align: center; color: white;
    box-shadow: 0 6px 20px rgba(102,126,234,0.4); margin-bottom: 1.5rem;">
    <h3 style="margin: 0;">📊 Central Data Sheet</h3>
    <a href="{FIXED_SHEET_URL}" target="_blank"
       style="color: white; background: rgba(255,255,255,0.2);
              padding: 0.6rem 1.2rem; border-radius: 10px;
              text-decoration: none; display: inline-block; margin-top: 0.5rem;">
        🔗 Open in Google Sheets
    </a>
    <p style="margin-top: 0.5rem; font-size: 0.85rem; opacity: 0.9;">
        All processed data are automatically saved here
    </p>
</div>
""", unsafe_allow_html=True)


#---------------------------------- UI with gradients
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;700&display=swap');
    * { font-family: 'Vazirmatn', sans-serif; }
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 3rem 2rem; border-radius: 20px; text-align: center; margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.3); animation: slideDown 0.6s ease-out;
    }
    @keyframes slideDown { from { opacity: 0; transform: translateY(-30px);} to { opacity:1; transform: translateY(0);} }
    .main-header h1 { color: white; font-size: 2.8rem; margin: 0; text-shadow: 2px 2px 4px rgba(0,0,0,0.2); }
    .main-header p { color: rgba(255,255,255,0.9); font-size: 1.2rem; margin: 0.5rem 0 0 0; }
    .metric-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 2rem; border-radius: 15px; text-align: center; color: white;
        box-shadow: 0 8px 32px rgba(240, 147, 251, 0.3); transition: transform .3s, box-shadow .3s;
        animation: fadeIn .8s ease-out;
    }
    .metric-card:hover { transform: translateY(-5px); box-shadow: 0 12px 48px rgba(240,147,251,.4); }
    @keyframes fadeIn { from { opacity:0; transform: scale(.9);} to { opacity:1; transform: scale(1);} }
    .metric-card h3 { font-size:1rem; margin:0 0 .5rem 0; opacity:.9; }
    .metric-card h2 { font-size:2rem; margin:0; font-weight:bold; }
    .quota-card {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        padding:1.5rem; border-radius:15px; color:white; box-shadow:0 8px 32px rgba(79,172,254,.3); margin-bottom:1rem;
    }
    .quota-number { font-size:3rem; font-weight:bold; margin:.5rem 0; }
    .status-box { padding:1.5rem; border-radius:15px; margin:1rem 0; animation: slideIn .5s ease-out; box-shadow:0 4px 20px rgba(0,0,0,.1); }
    @keyframes slideIn { from { opacity:0; transform: translateX(-20px);} to { opacity:1; transform: translateX(0);} }
    .status-success { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color:white; }
    .status-warning { background: linear-gradient(135deg, #f7971e 0%, #ffd200 100%); color:#333; }
    .status-error { background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%); color:white; }
    .status-info { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color:white; }
    .stButton>button {
        width:100%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color:white; border:none; padding:1rem 2rem; font-size:1.1rem; font-weight:bold;
        border-radius:12px; box-shadow:0 6px 24px rgba(102,126,234,.3); transition: all .3s ease;
    }
    .stButton>button:hover { transform: translateY(-2px); box-shadow:0 8px 32px rgba(102,126,234,.4); }
    .stProgress > div > div { background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); }
    .loading-spinner {
        display:inline-block; width:20px; height:20px; border:3px solid rgba(255,255,255,.3);
        border-radius:50%; border-top-color:#fff; animation:spin 1s ease-in-out infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    .badge {
        display:inline-block; padding:.5rem 1rem; border-radius:20px; font-size:.9rem; font-weight:bold; margin:.2rem;
    }
    .badge-success { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color:white; }
    .badge-warning { background: linear-gradient(135deg, #f7971e 0%, #ffd200 100%); color:#333; }
    .badge-error { background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%); color:white; }
    .file-display {
        padding:1rem; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-radius:10px; margin:.5rem 0;
    }
    .file-display h4 { margin:0; color:#333; }
    .file-display p { margin:.5rem 0 0 0; color:#666; font-size:.9rem; }
    .qc-card {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        padding: 1.5rem; border-radius: 15px; margin-bottom: 1rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    .qc-card h4 { color: #333; margin: 0 0 0.5rem 0; }
    .qc-card p { color: #666; margin: 0.25rem 0; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)




# ------------------------  api keys
API_KEYS = {
    "excel": "AIzaSyA***xJWI",
    "ocr": "AIza***XEEca4gxY",
    "scrap": "AIzaSyDMU****NPzB70"
}
for key_name, key_value in API_KEYS.items():
    os.environ[f"GOOGLE_API_KEY_{key_name.upper()}"] = key_value
    os.environ["GOOGLE_API_KEY"] = key_value
    os.environ["GEMINI_API_KEY"] = key_value



#---------------------------------------- google sheets integration
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

@st.cache_resource
def get_google_services():
    # Google Drive & Sheets
    try:
        SERVICE_ACCOUNT_FILE = Path("service-account.json")
        if SERVICE_ACCOUNT_FILE.exists():
            creds = service_account.Credentials.from_service_account_file(
                str(SERVICE_ACCOUNT_FILE),
                scopes=GOOGLE_SCOPES
            )
            
        else:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"],
                scopes=GOOGLE_SCOPES
            )
        drive_service = build('drive', 'v3', credentials=creds)
        sheets_service = build('sheets', 'v4', credentials=creds)
        return drive_service, sheets_service
    except Exception as e:
        st.error(f"❌ connection error to Google: {e}")
        return None, None

def _col_index_to_letter(col_index):
    # convert index to excel letter (0->a, 25->z, 26->aa)
    result = ""
    while col_index >= 0:
        result = chr(col_index % 26 + 65) + result
        col_index = col_index // 26 - 1
    return result

def find_or_create_data_table(drive_service, sheets_service, folder_id=None):
    # find or create table in drive
    try:
        table_name = "Exhibition_Data_Table"
        query = f"name='{table_name}' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        
        results = drive_service.files().list(
            q=query, spaces='drive', fields='files(id, name, webViewLink)', pageSize=1
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            file_id = files[0]['id']
            file_url = files[0].get('webViewLink', f"https://docs.google.com/spreadsheets/d/{file_id}/edit")
            print(f"   ✅ existing table: {file_id}")
            return file_id, file_url, True
        
        print(f"   📝 creating new table...")
        spreadsheet = sheets_service.spreadsheets().create(
            body={
                'properties': {'title': table_name},
                'sheets': [{'properties': {'title': 'Data', 'gridProperties': {'frozenRowCount': 1}}}]
            },
            fields='spreadsheetId'
        ).execute()
        
        file_id = spreadsheet.get('spreadsheetId')
        file_url = f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
        
        if folder_id:
            drive_service.files().update(fileId=file_id, addParents=folder_id, fields='id, parents').execute()
        
        print(f"   ✅ new table created: {file_id}")
        return file_id, file_url, False
        
    except Exception as e:
        print(f"   ❌ error: {e}")
        return None, None, False




# ---------------------------- generate permanent company id 
import hashlib
import re

def generate_company_id(company_name_fa=None, company_name_en=None):
    
    # انتخاب نام شرکت
    company_name = None
    
    if company_name_fa and str(company_name_fa).strip() not in ['', 'nan', 'None']:
        company_name = str(company_name_fa).strip()
    elif company_name_en and str(company_name_en).strip() not in ['', 'nan', 'None']:
        company_name = str(company_name_en).strip()
    
    if not company_name:
        # اگه اسم شرکت نبود، ID تصادفی بده
        import random
        random_hash = hashlib.md5(str(random.random()).encode()).hexdigest()[:12].upper()
        return f"COMP_UNKNOWN_{random_hash}"
    
    # نرمالسازی نام شرکت (حذف کلمات اضافی)
    normalized = company_name.lower()
    
    # حذف کلمات رایج
    for word in ['شرکت', 'company', 'co.', 'co', 'ltd', 'inc', 'group', 'گروه', 
                 'corporation', 'corp', '.', ',', '-', '_']:
        normalized = normalized.replace(word, ' ')
    
    # حذف فاصله‌های اضافی
    normalized = ' '.join(normalized.split())
    normalized = normalized.strip()
    
    # اگه بعد از نرمالسازی خالی شد
    if not normalized or len(normalized) < 2:
        normalized = company_name.lower()
    
    # ساخت hash دائمی
    hash_object = hashlib.sha256(normalized.encode('utf-8'))
    hash_hex = hash_object.hexdigest()[:12].upper()
    
    # فرمت نهایی
    company_id = f"COMP_{hash_hex}"
    
    return company_id


