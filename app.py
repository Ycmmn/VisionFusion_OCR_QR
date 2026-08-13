# -*- coding: utf-8 -*-
"""
🎯 Smart Exhibition Pipeline — Final Unified Edition + Google Sheets
Full integration of two apps: "Ultimate Smart Exhibition Pipeline" + "Smart Data Pipeline"
- Version 1's cool UI + Version 2's logic, logging, and quota management
- Excel Mode and OCR/QR Mode with automatic detection
- Smart Metadata Injection (Exhibition + Source + Smart Position)
- Fast Mode, Debug Mode, Rate Limiting, Daily Quota
- ✨ Batch Processing: Images(5), PDFs(4), Excel(1)
- ✨ Quality Control Tracking: User Name, Role, Date, Time
- ☁️ Google Sheets Integration: Automatically save data to Google Drive
Run:
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
import time
import socket
import google.generativeai as genai


#--------------------- Page settings
st.set_page_config(
    page_title="Smart Exhibition Pipeline",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)


#---------------------------- Permanent Google Sheets Link (Always Visible)

FIXED_SHEET_URL = "https://docs.google.com/spre********E/edit"

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




#------------------------------------------ UI خفن با گرادیانت‌های حرفه‌ای

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




#------------------------- API keys


API_KEYS = {
    "excel": "AIz************************Tag",
    "ocr": "AIz**************************8xio",
    "scrap": "AIza***********************4F3M"
}
for key_name, key_value in API_KEYS.items():
    os.environ[f"GOOGLE_API_KEY_{key_name.upper()}"] = key_value
    os.environ["GOOGLE_API_KEY"] = key_value
    os.environ["GEMINI_API_KEY"] = key_value


#----------------------- GOOGLE SHEETS INTEGRATION

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

@st.cache_resource
def get_google_services():
    """    Google Drive و Sheets  """
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
        st.error(f"❌ error connection to Google: {e}")
        return None, None

def _col_index_to_letter(col_index):
    """ index Excel (0->A, 25->Z, 26->AA)"""
    result = ""
    while col_index >= 0:
        result = chr(col_index % 26 + 65) + result
        col_index = col_index // 26 - 1
    return result

def find_or_create_data_table(drive_service, sheets_service, folder_id=None):
    """Find or create the sheet/table in Drive"""
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
            print(f"   ✅ : {file_id}")
            return file_id, file_url, True
        
        print(f"   📝 ...")
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
        
        print(f"   ✅ new table: {file_id}")
        return file_id, file_url, False
        
    except Exception as e:
        print(f"   ❌ error: {e}")
        return None, None, False





# ------------------------------ Generate Company ID

import hashlib
import re

def generate_company_id(company_name_fa=None, company_name_en=None):
    
    # Select company name
    company_name = None
    
    if company_name_fa and str(company_name_fa).strip() not in ['', 'nan', 'None']:
        company_name = str(company_name_fa).strip()
    elif company_name_en and str(company_name_en).strip() not in ['', 'nan', 'None']:
        company_name = str(company_name_en).strip()
    
    if not company_name:
        # If there's no company name, assign a random ID
        import random
        random_hash = hashlib.md5(str(random.random()).encode()).hexdigest()[:12].upper()
        return f"COMP_UNKNOWN_{random_hash}"
    

    # Normalize company name (remove extra words)
    normalized = company_name.lower()
    

    # Remove common words
    for word in ['شرکت', 'company', 'co.', 'co', 'ltd', 'inc', 'group', 'گروه', 
                 'corporation', 'corp', '.', ',', '-', '_']:
        normalized = normalized.replace(word, ' ')
    

    # Remove extra spaces
    normalized = ' '.join(normalized.split())
    normalized = normalized.strip()
    

    # If it becomes empty after normalization
    if not normalized or len(normalized) < 2:
        normalized = company_name.lower()
    

    # Generate permanent hash
    hash_object = hashlib.sha256(normalized.encode('utf-8'))
    hash_hex = hash_object.hexdigest()[:12].upper()
    

    # Final format
    company_id = f"COMP_{hash_hex}"
    
    return company_id






def add_company_id_to_dataframe(df, log_details=True):
    """
    Add CompanyID column to DataFrame
    
    Args:
        df: DataFrame input
        log_details: Show details in Console
    
    Returns:
        DataFrame with column CompanyID
    """
    import pandas as pd
    
    if df.empty:
        print("   ⚠️ DataFrame is empty, skipping CompanyID")
        return df
    
    print(f"\n🆔 Generating Hash-based Company IDs...")
    print(f"   📊 Processing {len(df)} rows...")
    
    company_ids = []
    id_mapping = {}  ## For tracking duplicates
    
    for idx, row in df.iterrows():

        #  Extract company name from row
        company_name_fa = None
        company_name_en = None

        for col in ['CompanyNameFA', 'CompanyNameEN', 'company_name_fa', 'company_name_en']:
            if col in row and row[col]:
                if 'FA' in col or 'fa' in col:
                    company_name_fa = row[col]
                else:
                    company_name_en = row[col]

        # Generate Company ID
        company_id = generate_company_id(company_name_fa, company_name_en)



        
        company_ids.append(company_id)
        
        # Track duplicates
        if company_id not in id_mapping:
            id_mapping[company_id] = []
        id_mapping[company_id].append(idx + 1)
        
        # Show first 5 samples
        if log_details and idx < 5:
            company_name = ""
            for col in ['CompanyNameFA', 'CompanyNameEN', 'company_name_fa', 'company_name_en']:
                if col in row and row[col]:
                    company_name = str(row[col])[:20]
                    break
            
            print(f"      Row {idx + 1}: {company_id} → {company_name}")
    
    # Add to DataFrame (first column)
    df.insert(0, 'CompanyID', company_ids)
    
    # Statistics
    unique_count = len(set(company_ids))
    duplicate_count = len(company_ids) - unique_count
    
    print(f"\n   ✅ CompanyID Statistics:")
    print(f"      • Total Records: {len(company_ids)}")
    print(f"      • Unique IDs: {unique_count}")
    print(f"      • Duplicate IDs: {duplicate_count}")
    
    if duplicate_count > 0:
        print(f"\n   📋 Companies with multiple records:")
        duplicate_ids = {k: v for k, v in id_mapping.items() if len(v) > 1}
        
        for comp_id, row_indices in list(duplicate_ids.items())[:5]:
            print(f"      • {comp_id}: appears in rows {row_indices}")
        
        if len(duplicate_ids) > 5:
            print(f"      ... and {len(duplicate_ids) - 5} more")
    
    return df


def merge_all_data_sources(session_dir, pipeline_type):
    """
    Merge all data sources (for both Modes)::
    
    OCR/QR Mode:
        - mix_ocr_qr.json (always)
        - gemini_scrap_output.json (if available)
    
    Excel Mode:
        - web_analysis.xlsx (always)
    
    Returns:
         Path: Path to the final Excel file
    """

    import pandas as pd
    import numpy as np
    from datetime import datetime
    

    print(f"\nStarting data merge for {pipeline_type.upper()} mode...")

    
    # -------------------------Paths
    mix_json = Path(session_dir) / "mix_ocr_qr.json"
    scrap_json = Path(session_dir) / "gemini_scrap_output.json"
    web_excel = Path(session_dir) / "web_analysis.xlsx"
    output_enriched = list(Path(session_dir).glob("output_enriched_*.xlsx"))
    

    # ========== EXCEL MODE ==========
    if pipeline_type == 'excel':
        print("   Excel Mode detected")
        
        ## 1. First check output_enriched
        if output_enriched:
            excel_file = output_enriched[0]
            print(f"   Using output_enriched: {excel_file.name}")
        
        ## 2. If not found, check web_analysis
        elif web_excel.exists():
            excel_file = web_excel
            print(f"   Using web_analysis: {excel_file.name}")
        
        else:
            print(f"   No Excel output found!")
            return None
        
        ## Reading and cleaning
        df = pd.read_excel(excel_file)
        
        # Cleaning
        df = df.fillna("")
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({
                    'nan': '', 'None': '', 'NaT': '', '<NA>': '', 'null': '', 'NULL': ''
                })

        # ==========------------------- TRANSLATION---------------- ==========
        print(f"\n🌐 Starting automatic translation...")
        df = translate_all_columns(df)
        # =========------------------------------------================

        # save
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(session_dir) / f"merged_complete_{timestamp}.xlsx"
        df.to_excel(output_path, index=False, engine='openpyxl')
        
        print(f"\n   Excel Mode completed!")
        print(f"      Rows: {len(df)}")
        print(f"      Columns: {len(df.columns)}")
        print(f"      Saved to: {output_path.name}")
        
        return output_path
    
    # ========== -----------------OCR/QR MODE---------------- ==========
    elif pipeline_type == 'ocr_qr':
        print("   OCR/QR Mode detected")
        
        ## 1. Read mix_ocr_qr.json (required)
        if not mix_json.exists():
            print(f"   {mix_json.name} not found!")
            return None
        
        
        print(f"   Reading {mix_json.name}...")
        try:
            with open(mix_json, 'r', encoding='utf-8') as f:
                mix_data = json.load(f)
            
            ## Convert to DataFrame (each file = one record, even if it has multiple PDF pages)
            records = []
            for file_item in mix_data:
                if not isinstance(file_item, dict):
                    continue
                
                result_data = file_item.get("result")
                
                if isinstance(result_data, dict):
                    page_results = [result_data]
                elif isinstance(result_data, list):
                    page_results = []
                    for page_data in result_data:
                        if isinstance(page_data, dict):
                            page_results.append(page_data.get("result", {}))
                else:
                    continue
                
                ## Merge all pages of this file into a single record
                record = {}
                
                for page_result in page_results:
                    if not isinstance(page_result, dict) or not page_result:
                        continue
                    
                    for key, value in page_result.items():
                        if key in ['ocr_text']:
                            continue
                        
                        if isinstance(value, list):
                            if not value:
                                continue
                            
                            for item in value:
                                if item is None:
                                    continue
                                item_str = str(item)
                                
                                if key not in record or str(record.get(key, '')).strip() in ['', 'nan', 'None']:
                                    record[key] = item_str
                                else:
                                    existing_values = str(record[key]).split('|')
                                    existing_values = [v.strip() for v in existing_values]
                                    if item_str.strip() in existing_values:
                                        continue
                                    i = 2
                                    while True:
                                        new_col = f"{key}{i}"
                                        if new_col not in record or str(record.get(new_col, '')).strip() in ['', 'nan', 'None']:
                                            record[new_col] = item_str
                                            break
                                        i += 1
                        
                        elif value is not None and str(value).strip():
                            value_str = str(value)
                            
                            if key not in record or str(record.get(key, '')).strip() in ['', 'nan', 'None']:
                                record[key] = value_str
                            elif str(record[key]).strip() == value_str.strip():
                                continue
                            else:
                                i = 2
                                while True:
                                    new_col = f"{key}{i}"
                                    if new_col not in record or str(record.get(new_col, '')).strip() in ['', 'nan', 'None']:
                                        record[new_col] = value_str
                                        break
                                    i += 1
                
                ## Make sure file_name is taken from file_item
                if 'file_name' not in record or not record.get('file_name'):
                    record['file_name'] = file_item.get('file_name', 'Unknown')

                if record:
                    records.append(record)
            
            if not records:
                print(f"   mix_ocr_qr: No valid records")
                return None
            
            df_mix = pd.DataFrame(records)
            print(f"   mix_ocr_qr: {len(df_mix)} rows x {len(df_mix.columns)} columns")
        
        except Exception as e:
            print(f"   Error reading mix_ocr_qr.json: {e}")
            return None
        
        #2. Read gemini_scrap_output.json (optional)
        if not scrap_json.exists():
            print(f"   {scrap_json.name} not found - using only OCR/QR data")
            
            #  mix_ocr_qr
            df_mix = df_mix.fillna("")
            for col in df_mix.columns:
                if df_mix[col].dtype == 'object':
                    df_mix[col] = df_mix[col].astype(str).str.strip()
                    df_mix[col] = df_mix[col].replace({
                        'nan': '', 'None': '', 'NaT': '', '<NA>': '', 'null': '', 'NULL': ''
                    })
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path(session_dir) / f"merged_complete_{timestamp}.xlsx"
            df_mix.to_excel(output_path, index=False, engine='openpyxl')
            
            print(f"\n   Saved (OCR/QR only): {output_path.name}")
            print(f"      Total rows: {len(df_mix)}")
            
            return output_path
        
        # ==========----------- 3.Read and process scraping data -----------==========

        print(f"   Reading {scrap_json.name}...")
        try:
            with open(scrap_json, 'r', encoding='utf-8') as f:
                scrap_data = json.load(f)
            
            if not isinstance(scrap_data, list):
                print(f"   Invalid scrap data format")
                df_scrap = pd.DataFrame()
            else:
                df_scrap = pd.DataFrame(scrap_data)
                
                # Only the successful ones
                if 'status' in df_scrap.columns:
                    df_scrap = df_scrap[df_scrap['status'] == 'SUCCESS'].copy()
                
                # Remove extra columns
                for col in ['status', 'error']:
                    if col in df_scrap.columns:
                        df_scrap.drop(columns=[col], inplace=True)
                
                # Add file_name from OCR/QR to scraping
                if not df_scrap.empty:
                    print(f"   🔗 Matching file_names from OCR/QR to Scraping...")
                    
                    # normalize_url
                    def normalize_url(url):
                        if not url or pd.isna(url):
                            return ""
                        url = str(url).strip().lower()
                        url = url.replace('http://', '').replace('https://', '').replace('www.', '')
                        return url.split('/')[0].split('?')[0]
                    
                    # Website → file_name
                    url_to_filename = {}
                    for idx, row in df_mix.iterrows():
                        for col in ['Website', 'Website2', 'Website3', 'urls', 'url']:
                            if col in row and row[col] and not pd.isna(row[col]):
                                url = normalize_url(row[col])
                                if url:
                                    filename = row.get('file_name', '')
                                    if filename:
                                        url_to_filename[url] = filename
                                        break
                    
                    print(f"      📋 Found {len(url_to_filename)} URL→file_name mappings")
                    
                    # file_name to scraping
                    matched_count = 0
                      
                    if 'file_name' not in df_scrap.columns:
                        df_scrap['file_name'] = ''

                    for idx in df_scrap.index:
                        scrap_url = None
                        for col in ['Website', 'urls', 'url']:
                            if col in df_scrap.columns and df_scrap.at[idx, col]:
                                scrap_url = normalize_url(df_scrap.at[idx, col])
                                break
                        
                        if scrap_url and scrap_url in url_to_filename:
                            df_scrap.at[idx, 'file_name'] = url_to_filename[scrap_url]
                            matched_count += 1


                        
                        print(f"      ✅ Matched {matched_count}/{len(df_scrap)} scraping records with file_name")

                    
                        print(f"\n   🔧 Filling empty file_names for Web rows...")

                        if 'file_name' in df_scrap.columns:
                            # If some rows don't have file_name, use the first available file_name
                            if url_to_filename:
                                ## Get the first file_name from the dictionary
                                default_filename = list(url_to_filename.values())[0]
                                
                                empty_count = 0
                                for idx in df_scrap.index:
                                    fname = df_scrap.at[idx, 'file_name']
                                    if not fname or pd.isna(fname) or str(fname).strip() in ['', 'Unknown']:
                                        df_scrap.at[idx, 'file_name'] = default_filename
                                        empty_count += 1
        
                                print(f"      ✅ Filled {empty_count} empty file_names with: {default_filename}")
                    
                    print(f"      ✅ Matched {matched_count}/{len(df_scrap)} scraping records with file_name")
                    
                    # Remove duplicate scraping rows
                    print(f"\n   🧹 Removing duplicate scraping records...")
                    
                    initial_count = len(df_scrap)
                    
                    if 'Website' in df_scrap.columns or 'urls' in df_scrap.columns:
                        url_col = 'Website' if 'Website' in df_scrap.columns else 'urls'
                        
                        # Normalize  URL
                        df_scrap['_normalized_url'] = df_scrap[url_col].apply(normalize_url)
                        
                        # Remove duplicates (keep the first)
                        df_scrap = df_scrap.drop_duplicates(subset=['_normalized_url'], keep='first')
                        
                        # Remove helper column
                        df_scrap.drop(columns=['_normalized_url'], inplace=True)
                        
                        removed_count = initial_count - len(df_scrap)
                        print(f"      ✅ Removed {removed_count} duplicate scraping records")
                        print(f"      📊 Remaining: {len(df_scrap)} unique scraping records")
            
            if df_scrap.empty:
                print(f"   No successful scraping data - using only OCR/QR")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = Path(session_dir) / f"merged_complete_{timestamp}.xlsx"
                df_mix.to_excel(output_path, index=False, engine='openpyxl')
                return output_path
            
            print(f"   gemini_scrap: {len(df_scrap)} rows x {len(df_scrap.columns)} columns")

        
        except Exception as e:
            print(f"   Error reading scrap data: {e} - using only OCR/QR")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path(session_dir) / f"merged_complete_{timestamp}.xlsx"
            df_mix.to_excel(output_path, index=False, engine='openpyxl')
            return output_path
        

        

        print(f"\n   🔗 Merging OCR/QR + Scraping into single rows...")

        all_records = {}

        for _, row in df_mix.iterrows():
            fname = str(row.get('file_name', '')).strip()
            if fname and fname not in ['', 'nan', 'Unknown']:
                all_records[fname] = row.to_dict()
            else:
                all_records[f"_ocr_{_}"] = row.to_dict()

        unmatched_scrap = []
        for _, row in df_scrap.iterrows():
            fname = str(row.get('file_name', '')).strip()
            if fname and fname in all_records:
                base = all_records[fname]
                for col, val in row.items():
                    if val is None or str(val).strip() in ['', 'nan', 'None']:
                        continue
                    if col not in base or str(base.get(col, '')).strip() in ['', 'nan', 'None']:
                        base[col] = val
                    else:
                        i = 2
                        while True:
                            new_col = f"{col}{i}"
                            if new_col not in base or str(base.get(new_col, '')).strip() in ['', 'nan', 'None']:
                                base[new_col] = val
                                break
                            i += 1
                all_records[fname] = base
            else:
                unmatched_scrap.append(row.to_dict())

        final_list = list(all_records.values()) + unmatched_scrap
        df_final = pd.DataFrame(final_list)
        print(f"   ✅ Before: OCR/QR={len(df_mix)}, Scrap={len(df_scrap)}")
        print(f"   ✅ After merge: {len(df_final)} rows")






        
        # Cleaning
        df_final = df_final.fillna("")
        for col in df_final.columns:
            if df_final[col].dtype == 'object':
                df_final[col] = df_final[col].astype(str).str.strip()
                df_final[col] = df_final[col].replace({
                    'nan': '', 'None': '', 'NaT': '', '<NA>': '', 'null': '', 'NULL': ''
                })
        
        #-------------Generate unique CompanyID for each file_name ----
        print(f"\n🆔 Generating unique CompanyID for each file_name...")
        
        if 'file_name' in df_final.columns:
            #  file_name → CompanyID
            file_to_company_id = {}
            
            for idx, row in df_final.iterrows():
                fname = row.get('file_name', '')
                
                if not fname or pd.isna(fname) or str(fname).strip() in ['', 'Unknown', 'web_only']:
                    #  If there's no file_name, assign a unique ID
                    company_id = generate_company_id(
                        row.get('CompanyNameFA'),
                        row.get('CompanyNameEN')
                    )
                else:
                    # If it has a file_name, check whether it was already created
                    fname_str = str(fname).strip()
                    
                    if fname_str not in file_to_company_id:
                        # First time we're seeing this file_name
                        company_id = generate_company_id(
                            row.get('CompanyNameFA'),
                            row.get('CompanyNameEN')
                        )
                        file_to_company_id[fname_str] = company_id
                        print(f"      {fname_str} → {company_id}")
                    else:
                        # Already seen, use the same ID
                        company_id = file_to_company_id[fname_str]
                
                #  add CompanyID
                df_final.at[idx, 'CompanyID'] = company_id
            
            # move CompanyID to  front
            cols = ['CompanyID'] + [col for col in df_final.columns if col != 'CompanyID']
            df_final = df_final[cols]
            
            print(f"   ✅ Generated {len(file_to_company_id)} unique CompanyIDs for {len(df_final)} rows")
        
        # ==========         Sort by file_name  ==========
        print(f"\n📑 Sorting by file_name...")
        
        if 'file_name' in df_final.columns:
            #  Sort: first by file_name, then by CompanyID
            df_final = df_final.sort_values(
                by=['file_name', 'CompanyID'], 
                ascending=[True, True]
            ).reset_index(drop=True)
            
            print(f"   ✅ Sorted {len(df_final)} rows by file_name")
            
            # Show statistics
            file_counts = df_final['file_name'].value_counts()
            print(f"\n   📊 File Distribution:")
            for fname, count in list(file_counts.items())[:5]:
                if fname and str(fname) not in ['', 'nan', 'Unknown']:
                    print(f"      • {fname}: {count} rows")
        
        # Save
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(session_dir) / f"merged_complete_{timestamp}.xlsx"
        df_final.to_excel(output_path, index=False, engine='openpyxl')
        
        print(f"\n   Concatenated successfully!")
        print(f"      OCR/QR: {len(df_mix)} rows")
        print(f"      Web Scraping: {len(df_scrap)} rows")
        print(f"      Final (separate rows): {len(df_final)} rows")
        print(f"      Saved to: {output_path.name}")
        
        return output_path
        
    else:
        print(f"   Unknown pipeline type: {pipeline_type}")
        return None


def translate_all_columns(df, api_key="AIz*****************B70"):
    """
    Translate all columns in the DataFrame
    - English → Persian only

    """
    
    
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.5-flash-lite')
    
    print(f"\n🌐 Starting translation for {len(df)} rows...")
    
    # Columns that should not be translated
    skip_columns = [
        'file_name', 'Exhibition', 'Source', 
        'QC_Supervisor', 'QC_Role', 'QC_Date', 'QC_Time', 'QC_Timestamp',
        'Phone1', 'Phone2', 'Phone3', 'Phone4', 'Phone5',
        'Email', 'Email2', 'Email3', 'Email4',
        'Website', 'Website2', 'Website3',
        'Fax', 'Fax2', 'WhatsApp', 'Telegram', 'Instagram', 'LinkedIn',
        'PostalCode', 'CompanyCode', 'Logo', 'QRCodes',
    ]
    
    def detect_language(text):
        """Detect language: fa or en"""
        if not text or pd.isna(text) or str(text).strip() == '':
            return None
        
        text = str(text).strip()
        
        #Check for Persian characters
        persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
        has_persian = any(c in persian_chars for c in text)
        
        if has_persian:
            return 'fa'
        else:
            return 'en'
    
    def translate_text(text):
        """Translate English text to Persian using Gemini"""
        if not text or pd.isna(text) or str(text).strip() == '':
            return ""
        
        text = str(text).strip()
        
        try:
            prompt = f"Translate this English text to Persian. Only return the translation, no explanations:\n\n{text}"
            
            response = model.generate_content(prompt)
            translation = response.text.strip()
            
            # Remove  markdown and quotes
            translation = translation.replace('*', '').replace('`', '').strip('"').strip("'")
            
            return translation
        
        except Exception as e:
            print(f"   ⚠️ Translation error: {e}")
            return ""
    
    # Process each column
    for col in df.columns:
        # Skip specific columns
        if col in skip_columns:
            continue
        
        #Check whether the column has already been translated
        if col.endswith('_translated') or col.endswith('FA') or col.endswith('EN'):
            continue
        
        print(f"\n   🔄 Processing column: {col}")
        
        #Count non-empty cells
        non_empty = df[col].notna() & (df[col].astype(str).str.strip() != '')
        total_cells = non_empty.sum()
        
        if total_cells == 0:
            print(f"      ⏭️ Empty column, skipping")
            continue
        
        print(f"      📊 {total_cells} non-empty cells")
        
        #Process each row
        translated_count = 0
        
        for idx in df.index:
            cell_value = df.at[idx, col]
            
            if not cell_value or pd.isna(cell_value) or str(cell_value).strip() == '':
                continue
            
            # Detect language
            lang = detect_language(cell_value)
            
            if lang != 'en':
                #  Only process English text
                continue
            
            #  Translate English → Persian
            translated = translate_text(cell_value)
            
            if translated:
                # Save in new column
                new_col = f"{col}FA" if not col.endswith('EN') else col.replace('EN', 'FA')
                df.at[idx, new_col] = translated
                translated_count += 1
            
            # Rate limiting
            time.sleep(1)
        
        print(f"      ✅ Translated {translated_count} cells")
    
    print(f"\n   ✅ Translation completed!")
    return df

def retry_sheets_append(sheets_service, file_id, sheet_name, chunk_values, max_retries=5, base_wait=10):
    
    NETWORK_ERRORS = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, TimeoutError, socket.timeout, socket.error, OSError)
    for attempt in range(1, max_retries + 1):
        try:
            result = sheets_service.spreadsheets().values().append(
                spreadsheetId=file_id,
                range=f'{sheet_name}!A:A',
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': chunk_values}
            ).execute()
            return result
        except NETWORK_ERRORS as e:
            wait_time = base_wait * (2 ** (attempt - 1))
            print(f"   ⚠️ Network error attempt {attempt}/{max_retries}: {e}")
            if attempt == max_retries:
                raise
            print(f"   ⏳ Waiting {wait_time}s...")
            time.sleep(wait_time)
        except HttpError as e:
            if e.resp.status in [400, 403, 404]:
                raise
            wait_time = base_wait * (2 ** attempt)
            print(f"   ⚠️ HTTP {e.resp.status} attempt {attempt}/{max_retries}, waiting {wait_time}s...")
            if attempt == max_retries:
                raise
            time.sleep(wait_time)



def retry_sheets_execute(request, max_retries=5, base_wait=10):
    """Execute any type of Google Sheets request (get, update, ...) with retry"""
    
    NETWORK_ERRORS = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, TimeoutError, socket.timeout, socket.error, OSError)
    for attempt in range(1, max_retries + 1):
        try:
            return request.execute()
        except NETWORK_ERRORS as e:
            wait_time = base_wait * (2 ** (attempt - 1))
            print(f"   ⚠️ Network error attempt {attempt}/{max_retries}: {e}")
            if attempt == max_retries:
                raise
            print(f"   ⏳ Waiting {wait_time}s...")
            time.sleep(wait_time)
        except HttpError as e:
            if e.resp.status in [400, 403, 404]:
                raise
            wait_time = base_wait * (2 ** attempt)
            print(f"   ⚠️ HTTP {e.resp.status} attempt {attempt}/{max_retries}, waiting {wait_time}s...")
            if attempt == max_retries:
                raise
            time.sleep(wait_time)


def consolidate_columns(df):
    """
   Merge similar columns using | as a separator
    No value is removed unless duplicate
    """
    print(f"\n🔀 Consolidating columns...")
    
    # Define groups
    COLUMN_GROUPS = {
        'Phones':             ['phones', 'phones2', 'phones3', 'phones4', 'Phone1', 'Phone2', 'phones5', 'phones6', 'phones7', 'phones8', 'phones9', 'Phone3', 'Phone4', 'Phone5'],
        'Faxes':              ['faxes', 'faxes2', 'Fax'],
        'Emails':             ['emails', 'Email', 'OtherEmails'],
        'Websites':           ['urls', 'url', 'Website', 'url2', 'url3', 'url4', 'url5','urls2', 'urls3', 'urls4'],
        'CompanyName':        ['company_names', 'company_names2', 'company_names3', 'CompanyNameEN', 'CompanyNameFA', 'CompanyNameFA_translated', 'company_names4', 'company_names5', 'company_names6', 'CompanyNameEN2', 'CompanyNameEN3', 'CompanyNameFA_translated2','CompanyNameFA_translated3', 'CompanyNameFA_translated4', 'CompanyNameFA_translated5'],
        'Services':           ['services', 'services2', 'services3', 'services4', 'services5', 'services6', 'services7', 'services8','services9', 'services10', 'services11'],
        'Address':            ['AddressEN', 'AddressFA'],
        'ProductName':        ['ProductName', 'ProductNameFA', 'ProductName2', 'ProductName3', 'ProductName4', 'ProductName5'],
        'ProductCategory':    ['ProductCategory', 'ProductCategoryFA', 'ProductCategory2', 'ProductCategory3', 'ProductCategory4', 'ProductCategoryFA2', 'ProductCategoryFA3', 'ProductCategoryFA4', 'ProductCategoryFA5'],
        'ProductDescription': ['ProductDescription', 'ProductDescriptionFA'],
        'Applications':       ['Applications', 'ApplicationsFA', 'Applications2', 'Applications3', 'Applications4', 'Applications5', 'ApplicationsFA2', 'ApplicationsFA3', 'ApplicationsFA4', 'ApplicationsFA5'],
        'Description':        ['Description', 'DescriptionFA', 'Description2', 'Description3', 'Description4', 'Description5', 'DescriptionFA2','DescriptionFA3', 'DescriptionFA4', 'DescriptionFA5'],
        'History':            ['History', 'HistoryFA'],
        'Employees':          ['Employees', 'EmployeesFA'],
        'ClientsPartners':    ['ClientsPartners', 'ClientsPartnersFA', 'ClientsPartners2', 'ClientsPartners3', 'ClientsPartners4', 'ClientsPartners5', 'ClientsPartnersFA2', 'ClientsPartnersFA3', 'ClientsPartnersFA4', 'ClientsPartnersFA5'],
        'Markets':            ['Markets', 'MarketsFA'],
        'Brands':             ['Brands', 'BrandsFA', 'Brands2', 'Brands3', 'Brands4', 'Brands5', 'BrandsFA2', 'BrandsFA3', 'BrandsFA4', 'BrandsFA5'],
        'Industry':           ['Industry', 'IndustryFA', 'Industry2', 'Industry3', 'Industry4', 'Industry5', 'IndustryFA2', 'IndustryFA3', 'IndustryFA4', 'IndustryFA5'],
        'Certifications':     ['Certifications', 'CertificationsFA'],
        'Country':            ['Country', 'CountryFA'],
        'City':               ['City', 'CityFA'],
        'Notes':              ['notes', 'notes2', 'notes3'],
    }
    
    for new_col, source_cols in COLUMN_GROUPS.items():
        # Find columns that actually exist in df
        existing = [c for c in source_cols if c in df.columns]
        
        if not existing:
            continue
        
        print(f"   🔗 {new_col} ← {existing}")
        
        # Merge values for each row
        def merge_row(row):
            values = []
            seen = set()
            for col in existing:
                val = str(row[col]).strip() if col in row else ''
                if val and val.lower() not in ['nan', 'none', 'nat', '', 'null']:
                    if val not in seen:  # Remove duplicates
                        values.append(val)
                        seen.add(val)
            return ' | '.join(values)
        
        df[new_col] = df.apply(merge_row, axis=1)
        
        # Remove old columns (unless their name is the same as new_col)
        cols_to_drop = [c for c in existing if c != new_col]
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    
    print(f"   ✅ Done! Final columns: {len(df.columns)}")
    return df






def append_excel_data_to_sheets(excel_path, folder_id=None, exhibition_name=None, qc_metadata=None):
    """Read Excel data and append to Google Sheets (variable row count)"""
    try:
        drive_service, sheets_service = get_google_services()
        if not drive_service or not sheets_service:
            return False, "Google connection failed", None, 0

        print(f"\n☁️ Starting data save to Google Drive...")

        # ✅ Use existing Google Sheet instead of creating a new one
        file_id = "1OeQbiqvo6v58rcxaoSUidOk0IxSGmL8YCpLnyh27yuE"
        file_url = f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
        exists = True
        print(f"   ✅ Using existing Google Sheet: {file_url}")

        if not file_id:
            return False, "Error creating table", None, 0
        
        print(f"📖 Reading Excel data: {excel_path.name}")
        df = pd.read_excel(excel_path)
        if df.empty:
            return False, "Excel file is empty", None, 0
        
        print(f"   ✅ {len(df)} rows × {len(df.columns)} columns read")
        # =======-------------- add Exhibition Name --------------========
        if exhibition_name:
            print(f"\n📝 Adding Exhibition to Google Sheets: {exhibition_name}")
            if 'Exhibition' not in df.columns:
                df.insert(0, 'Exhibition', exhibition_name)
        
        # ==========------------- add QC Metadata ==========
       
        if qc_metadata:
            print(f"\n👤 Adding QC Metadata to Google Sheets...")
            
            qc_columns_order = ['QC_Supervisor', 'QC_Role', 'QC_Date', 'QC_Time', 'QC_Timestamp']
            
            # Calculate starting position (after Exhibition if present)
            start_pos = 1 if 'Exhibition' in df.columns else 0
            
            for idx, col in enumerate(qc_columns_order, start=start_pos):
                if col in qc_metadata and col not in df.columns:
                    # Convert to string to prevent conversion to number
                    value = str(qc_metadata[col])
                    
                    # Add apostrophe for date and time (like phone number)
                    if col in ['QC_Date', 'QC_Time', 'QC_Timestamp']:
                        value = f"'{value}"
                    
                    df.insert(idx, col, value)
                    print(f"   ✅ {col}: {qc_metadata[col]}")
        
        # ==========------ add Source ------==========
        print(f"\n📋 Detecting Source (Image/PDF/Excel/Web)...")
        
        if 'file_name' in df.columns and 'Source' not in df.columns:
            def detect_source(fname):
                if not fname or pd.isna(fname) or str(fname).strip() in ['', 'Unknown', 'web_only']:
                    return 'Web'
                
                fname_str = str(fname).lower()
                
                if fname_str.endswith(('.jpg', '.jpeg', '.png', '.bmp', '.webp', '.gif', '.tiff', '.heic')):
                    return 'Image'
                elif fname_str.endswith('.pdf'):
                    return 'PDF'
                elif fname_str.endswith(('.xlsx', '.xls', '.xlsm', '.csv')):
                    return 'Excel'
                else:
                    return 'Unknown'
            
            # Calculate the Source column position (after QC metadata)
            qc_count = sum(1 for col in ['QC_Supervisor', 'QC_Role', 'QC_Date', 'QC_Time', 'QC_Timestamp'] if col in df.columns)
            source_pos = (1 if 'Exhibition' in df.columns else 0) + qc_count
            
            df.insert(source_pos, 'Source', df['file_name'].apply(detect_source))
            
            source_counts = df['Source'].value_counts()
            print(f"   ✅ Source Distribution:")
            for source, count in source_counts.items():
                print(f"      • {source}: {count} rows")
        





        # ==========---------  Convert date and time to Text Format  -------------==========
        print(f"\n🕐 Converting date/time columns to text format...")
        
        date_time_columns = ['QC_Date', 'QC_Time', 'QC_Timestamp']
        
        for col in date_time_columns:
            if col in df.columns:
                # Convert to string with a leading apostrophe (for Google Sheets)
                df[col] = df[col].apply(
                    lambda x: f"'{str(x)}" if x and str(x).strip() not in ['', 'nan', 'None'] else ""
                )
                print(f"   ✅ {col} converted to text format")
        
      

        print(f"\n   📊 Final DataFrame: {len(df)} rows × {len(df.columns)} columns")

        
        # Add: if there's no CompanyID, add it
        if 'CompanyID' not in df.columns:
            print(f"   ⚠️ CompanyID not found, generating...")
            df = add_company_id_to_dataframe(df, log_details=False)
        else:
            print(f"   ✅ CompanyID column exists")
        
        #  Make sure CompanyID is the first column
        if 'CompanyID' in df.columns:
            cols = ['CompanyID'] + [col for col in df.columns if col != 'CompanyID']
            df = df[cols]
            print(f"   ✅ CompanyID is now the first column")
        
        

        #  Clean DataFrame from NaN and None values
        import numpy as np

        # Replace empty values
        df = df.replace({np.nan: "", None: "", 'nan': "", 'None': "", 'NaT': ""})
       # ========== Merge similar columns ==========
        df = consolidate_columns(df)

        
        # ========== Remove unnecessary columns==========
        print(f"\n🧹 Removing unnecessary columns...")

        columns_to_remove = []

        #  1. Remove data_source and source_type
        for col in ['data_source', 'source_type', 'Data_Source', 'Source_Type']:
            if col in df.columns:
                columns_to_remove.append(col)
                print(f"   ❌ Removing: {col}")

        # 2.Remove Logo
        if 'Logo' in df.columns:
            columns_to_remove.append('Logo')
            print(f"   ❌ Removing: Logo")

        # Remove columns
        if columns_to_remove:
            df.drop(columns=columns_to_remove, inplace=True)
            print(f"   ✅ Removed {len(columns_to_remove)} columns")

        # ========== Extract Person/Position ==========
        print(f"\n👤 Extracting Person & Position from PersonX columns...")

        
        genai.configure(api_key="AIza**************70")
        model = genai.GenerativeModel('gemini-2.5-flash-lite')

        def translate_to_persian(text):
            """ترجمه انگلیسی به فارسی"""
            if not text or pd.isna(text) or str(text).strip() == '':
                return ""
            
            text = str(text).strip()
            
            # Check whether it's Persian or not
            persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
            has_persian = any(c in persian_chars for c in text)
            
            if has_persian:
                return text  # Already Persian
            
            try:
                prompt = f"Translate this English text to Persian. Only return the translation:\n\n{text}"
                response = model.generate_content(prompt)
                translation = response.text.strip().replace('*', '').replace('`', '').strip('"').strip("'")
                return translation
            except:
                return text

        def extract_person_position(person_col_value):
            """
            Extract Name and Position from the PersonX column
            Example: "Ali Ahmadi - Sales Manager" → ("Ali Ahmadi", "Sales Manager")
            """
            if not person_col_value or pd.isna(person_col_value) or str(person_col_value).strip() == '':
                return "", ""
            
            text = str(person_col_value).strip()
            
            ## Try splitting with different separators
            separators = [' - ', ' – ', ' | ', ' / ', '\n', '،', ',']
            
            name = ""
            position = ""
            
            for sep in separators:
                if sep in text:
                    parts = text.split(sep, 1)
                    if len(parts) == 2:
                        name = parts[0].strip()
                        position = parts[1].strip()
                        break
            
            # اگر جدا نشد، کل متن رو به عنوان اسم در نظر بگیر
            if not name:
                name = text
            
            # ترجمه به فارسی
            name_fa = translate_to_persian(name)
            position_fa = translate_to_persian(position)
            
            return name_fa, position_fa

        # پیدا کردن ستون‌های PersonX
        person_columns = [col for col in df.columns if col.lower().startswith('person')]

        if person_columns:
            print(f"   📋 Found {len(person_columns)} Person columns: {person_columns}")
            
            # لیست اسامی و پوزیشن‌ها
            names_list = []
            positions_list = []
            
            # پردازش هر سطر
            for idx in df.index:
                row_names = []
                row_positions = []
                
                for col in person_columns:
                    if col in df.columns:
                        value = df.at[idx, col]
                        name, position = extract_person_position(value)
                        
                        if name:
                            row_names.append(name)
                        if position:
                            row_positions.append(position)
                
                # ترکیب با " | "
                names_list.append(" | ".join(row_names) if row_names else "")
                positions_list.append(" | ".join(row_positions) if row_positions else "")
            
            # اضافه کردن ستون‌های جدید
            if 'Name' not in df.columns:
                df['Name'] = names_list
                print(f"   ✅ Added 'Name' column")
            
            if 'Position' not in df.columns:
                df['Position'] = positions_list
                print(f"   ✅ Added 'Position' column")
            
            # حذف ستون‌های قدیمی PersonX
            df.drop(columns=person_columns, inplace=True)
            print(f"   ✅ Removed {len(person_columns)} PersonX columns")
            
            # نمایش 3 نمونه
            print(f"\n   📊 Sample extractions:")
            for i in range(min(3, len(df))):
                if df.at[i, 'Name'] or df.at[i, 'Position']:
                    print(f"      Row {i+1}:")
                    print(f"         Name: {df.at[i, 'Name'][:50]}")
                    print(f"         Position: {df.at[i, 'Position'][:50]}")

        else:
            print(f"   ⚠️ No Person columns found")

        print(f"\n   ✅ Cleanup completed!")


        # ========== 🌐 ترجمه Position انگلیسی به فارسی ==========
        print(f"\n🌐 Translating English Positions to Persian...")

        if 'Position' in df.columns:
            
            
            genai.configure(api_key="AIzaSyDMUEVEqDCQpahoyIeXLN0UJ4IKNNPzB70")
            model = genai.GenerativeModel('gemini-2.5-flash-lite')
            
            def detect_language_position(text):
                """تشخیص زبان: fa یا en"""
                if not text or pd.isna(text) or str(text).strip() == '':
                    return None
                
                text = str(text).strip()
                persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
                has_persian = any(c in persian_chars for c in text)
                
                return 'fa' if has_persian else 'en'
            
            def translate_position_to_persian(text):
                """ترجمه انگلیسی به فارسی"""
                if not text or pd.isna(text) or str(text).strip() == '':
                    return ""
                
                text = str(text).strip()
                
                try:
                    prompt = f"Translate this English job position to Persian. Only return the translation:\n\n{text}"
                    response = model.generate_content(prompt)
                    translation = response.text.strip().replace('*', '').replace('`', '').strip('"').strip("'")
                    return translation
                except Exception as e:
                    print(f"   ⚠️ Translation error: {e}")
                    return text
            
            translated_count = 0
            
            for idx in df.index:
                position_value = df.at[idx, 'Position']
                
                if not position_value or pd.isna(position_value) or str(position_value).strip() == '':
                    continue
                
                # تشخیص زبان
                lang = detect_language_position(position_value)
                
                if lang == 'en':
                    # ترجمه انگلیسی → فارسی
                    position_fa = translate_position_to_persian(position_value)
                    
                    if position_fa:
                        # ترکیب: انگلیسی | فارسی
                        df.at[idx, 'Position'] = f"{position_value} | {position_fa}"
                        translated_count += 1
                        
                        if translated_count <= 3:  # نمایش 3 نمونه
                            print(f"      Row {idx+1}: {position_value} → {position_fa}")
                    
                    time.sleep(1)  # Rate limiting
            
            if translated_count > 0:
                print(f"   ✅ Translated {translated_count} English positions")
            else:
                print(f"   ℹ️ No English positions found")

        # ========== 🗑️ حذف PositionFA و PositionEN ==========
        print(f"\n🗑️ Removing PositionFA and PositionEN columns...")
        for col in ['PositionFA', 'PositionEN']:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
                print(f"   ❌ Removed: {col}")

        #
        # ========== 📍 یکپارچه‌سازی Address Columns ==========
        print(f"\n📍 Consolidating Address columns...")
        
        # پیدا کردن تمام ستون‌های Address
        address_columns = []
        for col in df.columns:
            col_lower = col.lower()
            if 'address' in col_lower:
                address_columns.append(col)
                print(f"   Found: {col}")
        
        if address_columns:
            print(f"   📋 Found {len(address_columns)} Address columns: {address_columns}")
            
            # تابع تشخیص زبان
            def detect_language_address(text):
                """تشخیص زبان آدرس: fa یا en"""
                if not text or pd.isna(text) or str(text).strip() == '':
                    return None
                
                text = str(text).strip()
                
                # چک کردن حروف فارسی
                persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
                has_persian = any(c in persian_chars for c in text)
                
                if has_persian:
                    return 'fa'
                else:
                    return 'en'
            
            # تابع ترجمه انگلیسی به فارسی
            def translate_address_to_persian(text):
                """ترجمه آدرس انگلیسی به فارسی"""
                if not text or pd.isna(text) or str(text).strip() == '':
                    return ""
                
                text = str(text).strip()
                
                try:
                    prompt = f"Translate this English address to Persian. Only return the translation:\n\n{text}"
                    response = model.generate_content(prompt)
                    translation = response.text.strip().replace('*', '').replace('`', '').strip('"').strip("'")
                    return translation
                except Exception as e:
                    print(f"   ⚠️ Translation error: {e}")
                    return text
            
            # لیست‌های جدید برای آدرس‌های یکپارچه
            unified_address_en = []
            unified_address_fa = []
            
            # پردازش هر سطر
            for idx in df.index:
                # جمع‌آوری تمام آدرس‌ها از ستون‌های مختلف
                all_addresses = []
                
                for col in address_columns:
                    if col in df.columns:
                        addr = df.at[idx, col]
                        if addr and not pd.isna(addr) and str(addr).strip() not in ['', 'nan', 'None']:
                            all_addresses.append(str(addr).strip())
                
                # اگه هیچ آدرسی نبود
                if not all_addresses:
                    unified_address_en.append("")
                    unified_address_fa.append("")
                    continue
                
                # حذف تکراری‌ها
                unique_addresses = list(dict.fromkeys(all_addresses))
                
                # جداسازی آدرس‌های فارسی و انگلیسی
                fa_addresses = []
                en_addresses = []
                
                for addr in unique_addresses:
                    lang = detect_language_address(addr)
                    
                    if lang == 'fa':
                        fa_addresses.append(addr)
                    elif lang == 'en':
                        en_addresses.append(addr)
                
                # ترکیب آدرس‌های انگلیسی
                final_en = " | ".join(en_addresses) if en_addresses else ""
                
                # ترکیب آدرس‌های فارسی
                final_fa = " | ".join(fa_addresses) if fa_addresses else ""
                
                # اگه آدرس انگلیسی داریم ولی فارسی نداریم → ترجمه کن
                if final_en and not final_fa:
                    print(f"   Row {idx+1}: Translating EN→FA...")
                    final_fa = translate_address_to_persian(final_en)
                    time.sleep(1)  # Rate limiting
                
                unified_address_en.append(final_en)
                unified_address_fa.append(final_fa)
            
            # حذف ستون‌های قدیمی
            for col in address_columns:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)
            
            print(f"   ✅ Removed {len(address_columns)} old Address columns")
            
            # اضافه کردن ستون‌های جدید
            df['AddressEN'] = unified_address_en
            df['AddressFA'] = unified_address_fa
            
            print(f"   ✅ Added unified 'AddressEN' and 'AddressFA' columns")
            
            # نمایش 3 نمونه
            print(f"\n   📊 Sample unified addresses:")
            for i in range(min(3, len(df))):
                if df.at[i, 'AddressEN'] or df.at[i, 'AddressFA']:
                    print(f"      Row {i+1}:")
                    if df.at[i, 'AddressEN']:
                        print(f"         EN: {df.at[i, 'AddressEN'][:60]}")
                    if df.at[i, 'AddressFA']:
                        print(f"         FA: {df.at[i, 'AddressFA'][:60]}")
        
        else:
            print(f"   ⚠️ No Address columns found")
        
        print(f"\n   ✅ Address consolidation completed!")
        
        # ========== پایان یکپارچه‌سازی Address ==========





        # ========== 🧹 تمیز کردن فرمول‌ها و ارورها ==========
        def remove_formulas_from_df(df):
            """حذف فرمول‌ها، ارورها و تبدیل به مقادیر ساده"""
            for col in df.columns:
                if df[col].dtype == 'object':
                    # حذف فرمول‌های Excel (که با = شروع میشن)
                    df[col] = df[col].apply(
                        lambda x: str(x)[1:] if isinstance(x, str) and x.startswith('=') else x
                    )
                    
                    # حذف #ERROR!, #REF!, #VALUE!, #N/A, etc.
                    df[col] = df[col].apply(
                        lambda x: "" if isinstance(x, str) and x.startswith('#') else x
                    )
                    
                    # تبدیل اعداد فارسی به انگلیسی
                    persian_digits = '۰۱۲۳۴۵۶۷۸۹'
                    english_digits = '0123456789'
                    trans_table = str.maketrans(persian_digits, english_digits)
                    df[col] = df[col].apply(
                        lambda x: str(x).translate(trans_table) if isinstance(x, str) else x
                    )
            
            return df
        
        df = remove_formulas_from_df(df)
        print(f"   🧹 Cleaned formulas and errors from {len(df.columns)} columns")
        # =====================================================
        # ========== 📞 تبدیل شماره تلفن‌ها به String ==========
        phone_columns = ['phones', 'phones2', 'phones3', 'phones4', 'phones5',
                        'Phone1', 'Phone2', 'Phone3', 'Phone4', 'Phone5',
                        'Fax', 'Fax2', 'WhatsApp', 'Telegram']
        
        for col in phone_columns:
            if col in df.columns:
                # تبدیل عدد به string با apostrophe در اول (برای Google Sheets)
                df[col] = df[col].apply(
                    lambda x: f"'{str(x)}" if x and str(x).strip() not in ['', 'nan', 'None'] else ""
                )
        
        print(f"   📞 Converted phone columns to text format")

        #
        # ====================================================
        # ========== 📠 تبدیل FAX ها به String (رفع #ERROR!) ==========
        print(f"\n📠 Converting FAX columns to text format...")

        fax_columns = []
        for col in df.columns:
            col_lower = col.lower()
            if 'fax' in col_lower:
                fax_columns.append(col)

        for col in fax_columns:
            # تبدیل به string با apostrophe برای جلوگیری از #ERROR! در Google Sheets
            df[col] = df[col].apply(
                lambda x: f"'{str(x)}" if x and str(x).strip() not in ['', 'nan', 'None'] else ""
            )
            print(f"   ✅ {col} converted to text format")

        print(f"   📠 Converted {len(fax_columns)} FAX columns")

        # ====================================================

        # ========== 🧹 حذف داده‌های تکراری در هر سطر (3+ بار تکرار در یک ROW) ==========
        print(f"\n🧹 Removing duplicate values within each row (3+ occurrences)...")

        total_removed = 0
        rows_affected = 0

        for idx in df.index:
            row = df.loc[idx]
            
            # شمارش مقادیر در این سطر (فقط مقادیر غیرخالی)
            values = []
            for col in df.columns:
                val = row[col]
                # فقط مقادیر معتبر
                if val and str(val).strip() not in ['', 'nan', 'None', 'null', 'NULL']:
                    values.append((col, str(val).strip()))
            
            if not values:
                continue
            
            # شمارش تکرار هر مقدار در این سطر
            value_counts = {}
            for col, val in values:
                if val not in value_counts:
                    value_counts[val] = []
                value_counts[val].append(col)
            
            # پیدا کردن مقادیری که 3+ بار تکرار شدن
            row_modified = False
            for val, columns in value_counts.items():
                if len(columns) >= 3:
                    # نگه‌داشتن اولین occurrence، حذف بقیه
                    for col in columns[1:]:
                        df.at[idx, col] = ''
                        total_removed += 1
                        row_modified = True
                    
                    if not row_modified:
                        rows_affected += 1
                    
                    # نمایش 5 نمونه اول
                    if rows_affected <= 5:
                        print(f"   Row {idx+1}: '{val[:30]}' appeared {len(columns)} times in columns {columns[:3]} → kept first, removed {len(columns)-1}")

        if total_removed > 0:
            print(f"\n   ✅ Removed {total_removed} duplicate values across {rows_affected} rows")
        else:
            print(f"   ℹ️ No duplicate values found (3+ times in same row)")

# ====================================================







        # ====================================================

        # تمیز کردن ستون‌های متنی
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({
                    'nan': '', 
                    'None': '', 
                    'NaT': '',
                    '<NA>': '',
                    'null': '',
                    'NULL': ''
                })
                # حذف مقادیر case-insensitive
                df[col] = df[col].apply(lambda x: "" if str(x).lower() in ['nan', 'none', 'nat', 'null'] else x)
        
        sheet_name = 'Sheet1'
        
        result = retry_sheets_execute(sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=f'{sheet_name}!1:1'
        ))
        
        existing_headers = result.get('values', [[]])[0] if result.get('values') else []







        new_headers = df.columns.tolist()
        
        print(f"   📋 Existing columns: {len(existing_headers)} | New columns: {len(new_headers)}")
        
        if not existing_headers:
            values = [new_headers] + df.values.tolist()
            print(f"   ℹ️ Empty table, adding {len(new_headers)} columns")
        else:
            new_columns = [col for col in new_headers if col not in existing_headers]
            
            all_columns = existing_headers.copy()
            for col in new_columns:
                if col not in all_columns:
                    all_columns.append(col)
            
            print(f"   📊 Final order: {len(all_columns)} columns")
            
            if new_columns:
                print(f"   🆕 New columns: {new_columns}")
                print(f"   🔄 Updating headers...")





                retry_sheets_execute(sheets_service.spreadsheets().values().update(
                    spreadsheetId=file_id,
                    range=f'{sheet_name}!1:1',
                    valueInputOption='USER_ENTERED',
                    body={'values': [all_columns]}
                ))
                
                result = retry_sheets_execute(sheets_service.spreadsheets().values().get(
                    spreadsheetId=file_id, range=f'{sheet_name}!A:A'
                ))
                existing_rows_count = len(result.get('values', [])) - 1






                if existing_rows_count > 0:
                    print(f"   📝 Filling {existing_rows_count} old rows...")
                    empty_values = [[''] * len(new_columns) for _ in range(existing_rows_count)]
                    start_col_index = len(existing_headers)
                    start_col_letter = _col_index_to_letter(start_col_index)
                    end_col_letter = _col_index_to_letter(start_col_index + len(new_columns) - 1)
                    





                    retry_sheets_execute(sheets_service.spreadsheets().values().update(
                        spreadsheetId=file_id,
                        range=f'{sheet_name}!{start_col_letter}2:{end_col_letter}{existing_rows_count+1}',
                        valueInputOption='USER_ENTERED',
                        body={'values': empty_values}
                    ))
                    print(f"   ✅ Old rows updated")
            







            for col in all_columns:
                if col not in df.columns:
                    df[col] = ''
            
            df = df[all_columns]
            print(f"   ✅ DataFrame sorted: {len(df)} rows × {len(all_columns)} columns")
            values = df.values.tolist()

        # ✅ Convert all NaN or None to string before sending to Sheets
        def clean_cell(cell):
            """تمیز کردن کامل سلول"""
            if pd.isna(cell) or cell is None:
                return ""
            cell_str = str(cell).strip()
    
            # چک کردن مقادیر ناخواسته
            if cell_str.lower() in ['nan', 'none', 'nat', '<na>', 'null']:
                return ""
            
            # حذف ارورهای Excel
            if cell_str.startswith('#'):
                return ""
    
            return cell_str

        values = [[clean_cell(cell) for cell in row] for row in values]
        




        result = retry_sheets_execute(sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=f'{sheet_name}!A:A'
        ))
        existing_rows = len(result.get('values', []))




        
        print(f"   📊 Current rows: {existing_rows}")
        print(f"   📤 Adding {len(values)} rows...")
        
       

        # تقسیم به chunk‌های 500 سطری
        chunks = [values[i:i+500] for i in range(0, len(values), 500)]
        print(f"\n📦 {len(chunks)} chunks × 500 rows")
        
        uploaded_rows = 0
        failed_chunks = []
        
        for chunk_idx, chunk in enumerate(chunks, 1):
            print(f"   📤 Chunk {chunk_idx}/{len(chunks)} ({len(chunk)} rows)...")
            try:
                retry_sheets_append(sheets_service, file_id, sheet_name, chunk)
                uploaded_rows += len(chunk)
                print(f"   ✅ Chunk {chunk_idx} done")
                if chunk_idx < len(chunks):
                    time.sleep(2)
            except Exception as e:
                print(f"   ❌ Chunk {chunk_idx} failed: {e}")
                failed_chunks.append(chunk_idx)
                continue
        
        updated_rows = uploaded_rows
        total_rows = existing_rows + uploaded_rows




        result = retry_sheets_execute(sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=f'{sheet_name}!1:1'
        ))
        total_columns = len(result.get('values', [[]])[0])
        



        total_cells = total_rows * total_columns
        capacity = (total_cells / 10_000_000) * 100
        
        print(f"   ✅ {updated_rows} new rows added")
        print(f"   📊 Total: {total_rows} rows × {total_columns} columns")
        print(f"   📊 Total cells: {total_cells:,} ({capacity:.1f}%)")
        print(f"   🔗 {file_url}")
        
        message = f"✅ {updated_rows} new rows | Total: {total_rows} rows | {total_columns} columns"
        return True, message, file_url, total_rows
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False, str(e), None, 0


def get_or_create_folder(folder_name="Exhibition_Data"):
    """پیدا/ساخت پوشه در Drive"""
    try:
        drive_service, _ = get_google_services()
        if not drive_service:
            return None
        
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = drive_service.files().list(
            q=query, spaces='drive', fields='files(id, name)', pageSize=1
        ).execute()
        files = results.get('files', [])
        
        if files:
            print(f"   ✅ پوشه موجود: {files[0]['name']}")
            return files[0]['id']
        
        folder = drive_service.files().create(
            body={'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'},
            fields='id'
        ).execute()
        print(f"   ✅ پوشه جدید: {folder_name}")
        return folder.get('id')
        
    except Exception as e:
        print(f"   ❌ خطا: {e}")
        return None




# =========================================================
# 📅 Quota Management
# =========================================================
DAILY_LIMIT = 240
QUOTA_FILE = Path("quota.json")

def save_quota(q):
    QUOTA_FILE.write_text(json.dumps(q, indent=2, ensure_ascii=False), encoding="utf-8")

def load_quota():
    today = datetime.date.today().isoformat()
    if QUOTA_FILE.exists():
        try:
            data = json.loads(QUOTA_FILE.read_text(encoding="utf-8"))
            file_date = data.get("date")
            if file_date != today:
                q = {"date": today, "used": 0, "remaining": DAILY_LIMIT}
                save_quota(q)
                return q
            used = data.get("used", 0)
            remaining = max(0, DAILY_LIMIT - used)
            q = {"date": today, "used": used, "remaining": remaining}
            save_quota(q)
            return q
        except Exception:
            pass
    q = {"date": today, "used": 0, "remaining": DAILY_LIMIT}
    save_quota(q)
    return q

def decrease_quota(amount=1):
    quota = load_quota()
    quota["used"] += amount
    quota["remaining"] = max(0, DAILY_LIMIT - quota["used"])
    save_quota(quota)
    return quota

# =========================================================
# ✨ Quality Control Tracking Functions
# =========================================================
def get_qc_metadata(user_name, user_role):
    """ساخت متادیتای کنترل کیفیت"""
    now = datetime.datetime.now()
    return {
        "QC_Supervisor": user_name,
        "QC_Role": user_role,
        "QC_Date": now.strftime("%Y-%m-%d"),
        "QC_Time": now.strftime("%H:%M:%S"),
        "QC_Timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
    }

def add_qc_metadata_to_excel(excel_path, qc_metadata):
    """اضافه کردن متادیتای کنترل کیفیت به Excel"""
    try:
        df = pd.read_excel(excel_path)
        for key in ["QC_Supervisor", "QC_Role", "QC_Date", "QC_Time", "QC_Timestamp"]:
            if key in qc_metadata:
                df.insert(0, key, qc_metadata[key])
        df.to_excel(excel_path, index=False, engine='openpyxl')
        print(f"   ✅ QC Metadata added: {qc_metadata['QC_Supervisor']} ({qc_metadata['QC_Role']})")
        return True
    except Exception as e:
        print(f"   ❌ Error adding QC metadata: {e}")
        return False

def save_qc_log(session_dir, qc_metadata, exhibition_name, pipeline_type, total_files):
    """ذخیره لاگ کنترل کیفیت در فایل JSON"""
    try:
        qc_log_file = session_dir / "qc_log.json"
        qc_log = {
            **qc_metadata,
            "Exhibition": exhibition_name,
            "Pipeline_Type": pipeline_type,
            "Total_Files": total_files,
            "Session_Dir": str(session_dir)
        }
        qc_log_file.write_text(json.dumps(qc_log, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"   ✅ QC Log saved: {qc_log_file}")
        return True
    except Exception as e:
        print(f"   ❌ Error saving QC log: {e}")
        return False

# =========================================================
# 🧠 توابع هوشمند مشترک
# =========================================================
def detect_source_type(file_name):
    """تشخیص نوع فایل: Image, PDF, Excel"""
    if not file_name or pd.isna(file_name):
        return "Unknown"
    
    file_name = str(file_name).lower()
    
    # تصاویر
    if file_name.endswith(('.jpg', '.jpeg', '.png', '.bmp', '.webp', '.gif', '.tiff', '.tif', '.svg', '.heic')):
        return "Image"
    
    # PDF
    elif file_name.endswith('.pdf'):
        return "PDF"
    
    # Excel
    elif file_name.endswith(('.xlsx', '.xls', '.xlsm', '.xlsb', '.csv')):
        return "Excel"
    
    else:
        return "Unknown"

def smart_position_from_department(department):
    if not department or pd.isna(department) or str(department).strip() == '':
        return None
    department = str(department).strip().lower()
    department_position_map = {
        'فروش': 'مدیر فروش', 'sales': 'مدیر فروش',
        'بازاریابی': 'مدیر بازاریابی', 'marketing': 'مدیر بازاریابی',
        'صادرات': 'مدیر صادرات', 'export': 'مدیر صادرات',
        'واردات': 'مدیر واردات', 'import': 'مدیر واردات',
        'بازرگانی': 'مدیر بازرگانی', 'commerce': 'مدیر بازرگانی',
        'مدیریت': 'مدیرعامل', 'management': 'مدیرعامل',
        'اجرایی': 'مدیر اجرایی', 'executive': 'مدیر اجرایی',
        'عامل': 'مدیرعامل', 'ceo': 'مدیرعامل',
        'تولید': 'مدیر تولید', 'production': 'مدیر تولید',
        'کارخانه': 'مدیر کارخانه', 'factory': 'مدیر کارخانه',
        'عملیات': 'مدیر عملیات', 'operations': 'مدیر عملیات',
        'فنی': 'مدیر فنی', 'technical': 'مدیر فنی',
        'مالی': 'مدیر مالی', 'finance': 'مدیر مالی',
        'حسابداری': 'مدیر حسابداری', 'accounting': 'مدیر حسابداری',
        'منابع انسانی': 'مدیر منابع انسانی', 'hr': 'مدیر منابع انسانی',
        'فناوری': 'مدیر فناوری اطلاعات', 'it': 'مدیر IT',
        'تحقیق': 'مدیر تحقیق و توسعه', 'r&d': 'مدیر R&D',
        'کیفیت': 'مدیر کنترل کیفیت', 'qc': 'مدیر کنترل کیفیت',
        'خدمات': 'مدیر خدمات', 'support': 'مدیر پشتیبانی',
        'لجستیک': 'مدیر لجستیک', 'logistics': 'مدیر لجستیک',
        'انبار': 'مدیر انبار', 'warehouse': 'مدیر انبار',
        'خرید': 'مدیر خرید', 'purchasing': 'مدیر خرید',
        'روابط عمومی': 'مدیر روابط عمومی', 'pr': 'مدیر روابط عمومی',
    }
    for key, position in department_position_map.items():
        if key in department:
            return position
    if any(word in department for word in ['مدیر', 'manager', 'رئیس', 'chief']):
        return f"مدیر {department.title()}"
    elif any(word in department for word in ['معاون', 'deputy']):
        return f"معاون {department.title()}"
    elif any(word in department for word in ['کارشناس', 'expert']):
        return f"کارشناس {department.title()}"
    return f"مسئول {department.title()}"


# =========================================================
# 🌍 استخراج Country & City از Address
# =========================================================

def extract_country_city_from_address(address_fa, address_en):
    """
    استخراج کشور و شهر از آدرس فارسی و انگلیسی
    
    Returns:
        tuple: (country, city)
    """
    
    # لیست شهرهای اصلی ایران (فارسی + انگلیسی)
    IRANIAN_CITIES = {
        # شهرهای بزرگ
        'تهران': 'Tehran', 'مشهد': 'Mashhad', 'اصفهان': 'Isfahan', 
        'شیراز': 'Shiraz', 'تبریز': 'Tabriz', 'کرج': 'Karaj',
        'قم': 'Qom', 'اهواز': 'Ahvaz', 'کرمانشاه': 'Kermanshah',
        'ارومیه': 'Urmia', 'رشت': 'Rasht', 'زاهدان': 'Zahedan',
        'کرمان': 'Kerman', 'همدان': 'Hamadan', 'یزد': 'Yazd',
        'اردبیل': 'Ardabil', 'بندرعباس': 'Bandar Abbas', 'قزوین': 'Qazvin',
        'زنجان': 'Zanjan', 'سنندج': 'Sanandaj', 'خرم آباد': 'Khorramabad',
        'گرگان': 'Gorgan', 'ساری': 'Sari', 'بجنورد': 'Bojnord',
        'سمنان': 'Semnan', 'یاسوج': 'Yasuj', 'بوشهر': 'Bushehr',
        'ایلام': 'Ilam', 'بیرجند': 'Birjand', 'شهرکرد': 'Shahrekord',
        # نام‌های انگلیسی
        'tehran': 'Tehran', 'mashhad': 'Mashhad', 'isfahan': 'Isfahan',
        'shiraz': 'Shiraz', 'tabriz': 'Tabriz', 'karaj': 'Karaj',
        'qom': 'Qom', 'ahvaz': 'Ahvaz', 'kermanshah': 'Kermanshah',
    }
    
    # لیست کشورها (فارسی + انگلیسی)
    COUNTRIES = {
        # فارسی
        'ایران': 'Iran', 'آلمان': 'Germany', 'چین': 'China', 
        'ترکیه': 'Turkey', 'امارات': 'UAE', 'آمریکا': 'USA',
        'انگلستان': 'UK', 'فرانسه': 'France', 'ایتالیا': 'Italy',
        'کره': 'South Korea', 'ژاپن': 'Japan', 'هند': 'India',
        'عراق': 'Iraq', 'افغانستان': 'Afghanistan', 'پاکستان': 'Pakistan',
        # انگلیسی
        'iran': 'Iran', 'germany': 'Germany', 'china': 'China',
        'turkey': 'Turkey', 'uae': 'UAE', 'usa': 'USA',
        'uk': 'UK', 'france': 'France', 'italy': 'Italy',
        'korea': 'South Korea', 'japan': 'Japan', 'india': 'India',
    }
    
    country = None
    city = None
    
    # ترکیب آدرس‌ها
    combined_address = ""
    if address_fa and not pd.isna(address_fa):
        combined_address += str(address_fa).lower() + " "
    if address_en and not pd.isna(address_en):
        combined_address += str(address_en).lower() + " "
    
    if not combined_address.strip():
        return None, None
    
    # جستجوی شهر
    for city_name, city_en in IRANIAN_CITIES.items():
        if city_name.lower() in combined_address:
            city = city_en
            country = "Iran"  # اگر شهر ایرانی پیدا شد، کشور ایران است
            break
    
    # جستجوی کشور (اگر هنوز پیدا نشده)
    if not country:
        for country_name, country_en in COUNTRIES.items():
            if country_name.lower() in combined_address:
                country = country_en
                break
    
    # اگر کشور پیدا نشد ولی شهر ایرانی بود
    if city and not country:
        country = "Iran"
    
    # اگر فقط کشور پیدا شد (بدون شهر) و ایران بود
    if country == "Iran" and not city:
        # سعی در یافتن شهر با regex
        import re
        
        # الگوهای رایج آدرس ایران
        patterns = [
            r'استان\s+(\w+)',  # استان تهران
            r'شهر\s+(\w+)',     # شهر تهران
            r'م\.(\w+)',        # م.تهران
        ]
        
        for pattern in patterns:
            match = re.search(pattern, combined_address)
            if match:
                potential_city = match.group(1)
                if potential_city in IRANIAN_CITIES:
                    city = IRANIAN_CITIES[potential_city]
                    break
    
    return country, city


def add_country_city_columns(excel_path):
    """
    اضافه کردن ستون‌های Country و City به Excel
    """
    try:
        print(f"\n🌍 Adding Country & City columns...")
        df = pd.read_excel(excel_path)
        
        # چک کردن وجود ستون‌های Address
        has_address_fa = 'AddressFA' in df.columns
        has_address_en = 'AddressEN' in df.columns
        
        if not has_address_fa and not has_address_en:
            print("   ⚠️ No AddressFA or AddressEN columns found")
            return False
        
        # اضافه کردن ستون‌ها در صورت عدم وجود
        if 'Country' not in df.columns:
            df['Country'] = None
        if 'City' not in df.columns:
            df['City'] = None
        
        # پردازش هر سطر
        filled_count = 0
        for idx in df.index:
            address_fa = df.at[idx, 'AddressFA'] if has_address_fa else None
            address_en = df.at[idx, 'AddressEN'] if has_address_en else None
            
            # فقط اگر Country/City خالی بودند
            if pd.isna(df.at[idx, 'Country']) or str(df.at[idx, 'Country']).strip() == '':
                country, city = extract_country_city_from_address(address_fa, address_en)
                
                if country:
                    df.at[idx, 'Country'] = country
                    filled_count += 1
                    
                    if city:
                        df.at[idx, 'City'] = city
                        print(f"   Row {idx + 1}: {city}, {country}")
                    else:
                        print(f"   Row {idx + 1}: {country} (no city)")
        
        # ذخیره
        df.to_excel(excel_path, index=False, engine='openpyxl')
        print(f"   ✅ Updated {filled_count} rows with Country/City")
        
        # نمایش آمار
        if 'Country' in df.columns:
            country_counts = df['Country'].value_counts()
            print(f"\n   📊 Country Distribution:")
            for country, count in list(country_counts.items())[:5]:
                if country and str(country) != 'nan':
                    print(f"      • {country}: {count} rows")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def add_exhibition_and_source(excel_path, exhibition_name, session_dir, qc_metadata=None):
    """
    اضافه کردن متادیتای کامل به Excel:
    - Exhibition Name
    - Source (Image/PDF/Excel)
    - QC Supervisor
    - QC Role
    - QC Date
    - QC Time
    - QC Timestamp
    - Smart Position Detection
    """
    try:
        print(f"\n📝 Adding Exhibition, Source & QC Metadata...")
        df = pd.read_excel(excel_path)
        print(f"   ✅ Loaded: {len(df)} rows × {len(df.columns)} columns")

        # ========== اضافه کردن Exhibition ==========
        df.insert(0, 'Exhibition', exhibition_name)
        print(f"   📋 Exhibition: '{exhibition_name}'")
        
        # ========== اضافه کردن QC Metadata ==========
        if qc_metadata:
            # اضافه کردن ستون‌های QC در ابتدای DataFrame
            qc_columns = ['QC_Supervisor', 'QC_Role', 'QC_Date', 'QC_Time', 'QC_Timestamp']
            
            for idx, col in enumerate(qc_columns, start=1):
                if col in qc_metadata:
                    df.insert(idx, col, qc_metadata[col])
            
            print(f"   👤 QC Supervisor: {qc_metadata.get('QC_Supervisor', 'N/A')}")
            print(f"   💼 QC Role: {qc_metadata.get('QC_Role', 'N/A')}")
            print(f"   📅 QC Date: {qc_metadata.get('QC_Date', 'N/A')}")
            print(f"   🕐 QC Time: {qc_metadata.get('QC_Time', 'N/A')}")
        
        # ========== تشخیص Source ==========
        # ✅ خواندن نوع فایل‌های آپلود شده
        file_types_path = Path(session_dir) / "uploaded_file_types.json"
        
        if file_types_path.exists():
            file_types = json.loads(file_types_path.read_text(encoding='utf-8'))
            print(f"   📖 Loaded file types: {file_types}")
            
            # تشخیص Source بر اساس file_name
            if 'file_name' in df.columns:
                def get_source(fname):
                    if pd.isna(fname):
                        return "Unknown"
                    fname_str = str(fname)
                    
                    # جستجو در file_types
                    if fname_str in file_types:
                        return file_types[fname_str]
                    
                    # اگر پیدا نشد، از detect_source_type استفاده کن
                    return detect_source_type(fname_str)
                
                # اضافه کردن Source بعد از QC ستون‌ها
                insert_position = 6 if qc_metadata else 1
                df.insert(insert_position, 'Source', df['file_name'].apply(get_source))
                print(f"   ✅ Source detected from uploaded file types")
            
            else:
                # ✅ اگر file_name نبود، از تعداد فایل‌ها استفاده کن
                if len(file_types) == 1:
                    # فقط یک فایل بود → همون source رو به همه بده
                    source = list(file_types.values())[0]
                    insert_position = 6 if qc_metadata else 1
                    df.insert(insert_position, 'Source', source)
                    print(f"   ✅ Source set to: {source} (single file)")
                
                elif len(file_types) > 1:
                    # چند فایل بودن → بر اساس ترتیب
                    sources = list(file_types.values())
                    
                    # اگر تعداد سطرها با تعداد فایل‌ها برابره
                    if len(df) == len(sources):
                        insert_position = 6 if qc_metadata else 1
                        df.insert(insert_position, 'Source', sources)
                        print(f"   ✅ Source matched by row count")
                    else:
                        # پر کردن با اولین source
                        insert_position = 6 if qc_metadata else 1
                        df.insert(insert_position, 'Source', sources[0])
                        print(f"   ⚠️ Multiple files but row count mismatch → using first source")
                
                else:
                    insert_position = 6 if qc_metadata else 1
                    df.insert(insert_position, 'Source', 'Unknown')
                    print(f"   ⚠️ No file types found")
        
        else:
            # ✅ fallback: استفاده از file_name
            print(f"   ⚠️ file_types.json not found, using fallback")
            
            if 'file_name' in df.columns:
                insert_position = 6 if qc_metadata else 1
                df.insert(insert_position, 'Source', df['file_name'].apply(detect_source_type))
                print(f"   ✅ Source detected from file_name column")
            else:
                insert_position = 6 if qc_metadata else 1
                df.insert(insert_position, 'Source', 'Unknown')
                print(f"   ⚠️ No file_name column → Source set to Unknown")

        # ========== Smart Position Detection ==========
        if 'Department' in df.columns and 'PositionFA' in df.columns:
            print(f"\n🤖 Smart Position Detection...")
            filled_count = 0
            for idx in df.index:
                if pd.isna(df.loc[idx, 'PositionFA']) or str(df.loc[idx, 'PositionFA']).strip() == '':
                    department = df.loc[idx, 'Department']
                    smart_position = smart_position_from_department(department)
                    if smart_position:
                        df.loc[idx, 'PositionFA'] = smart_position
                        filled_count += 1
                        print(f"   Row {idx + 1}: {department} → {smart_position}")
            
            if filled_count > 0:
                print(f"   ✅ Filled {filled_count} positions from Department")

        # ========== حذف ستون‌های اضافی ==========
        columns_to_remove = ['CompanyNameFA_translated']
        removed = 0
        for col in columns_to_remove:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)
                removed += 1
                print(f"   🗑️ Removed column: {col}")
        
        if removed:
            print(f"   ✅ Removed {removed} unnecessary columns")

        # ========== تمیز کردن داده‌ها ==========
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace('nan', '').replace('None', '')
                except Exception as e:
                    print(f"   ⚠️ Warning: Could not convert column {col}: {e}")

        # ========== ذخیره ==========
        df.to_excel(excel_path, index=False, engine='openpyxl')
        print(f"   💾 Updated: {excel_path.name}")
        print(f"   📊 Final: {len(df)} rows × {len(df.columns)} columns")
        
        # ========== نمایش Source Distribution ==========
        if 'Source' in df.columns:
            source_counts = df['Source'].value_counts()
            print(f"\n   📊 Source Distribution:")
            for source, count in source_counts.items():
                print(f"      • {source}: {count} rows")
        
        # ========== نمایش خلاصه متادیتا ==========
        print(f"\n   📋 Metadata Summary:")
        print(f"      📌 Exhibition: {exhibition_name}")
        if qc_metadata:
            print(f"      👤 QC Supervisor: {qc_metadata.get('QC_Supervisor')}")
            print(f"      💼 QC Role: {qc_metadata.get('QC_Role')}")
            print(f"      📅 QC Timestamp: {qc_metadata.get('QC_Timestamp')}")
        
        return True
    
    except Exception as e:
        print(f"   ❌ Error adding metadata: {e}")
        import traceback
        traceback.print_exc()
        return False
# =========================================================
# 🔍 تشخیص نوع Pipeline و نام نمایشگاه
# =========================================================
def detect_pipeline_type(files):
    extensions = [f.name.split('.')[-1].lower() for f in files]
    if any(ext in ['xlsx', 'xls'] for ext in extensions):
        return 'excel'
    elif any(ext in ['pdf', 'jpg', 'jpeg', 'png'] for ext in extensions):
        return 'ocr_qr'
    return None

def extract_exhibition_name(files):
    if not files:
        return "Unknown_Exhibition"
    first_file = files[0].name
    name_without_ext = first_file.rsplit('.', 1)[0]
    name_parts = re.split(r'[_\-\s]+', name_without_ext)
    cleaned_parts = [p for p in name_parts if not p.isdigit() and len(p) > 2]
    if cleaned_parts:
        return " ".join(cleaned_parts[:3])
    return "Unknown_Exhibition"

# =========================================================
# ✨ Batch Processing Logic
# =========================================================
def get_batch_size(file_type):
    """تعیین اندازه Batch بر اساس نوع فایل"""
    file_type = file_type.lower()
    if file_type in ['jpg', 'jpeg', 'png', 'bmp', 'webp', 'gif']:
        return 5
    elif file_type == 'pdf':
        return 4
    elif file_type in ['xlsx', 'xls']:
        return 1
    else:
        return 1

def create_batches(files_list, batch_size):
    """تقسیم لیست فایل‌ها به Batch‌های کوچک‌تر"""
    batches = []
    for i in range(0, len(files_list), batch_size):
        batches.append(files_list[i:i + batch_size])
    return batches

def process_files_in_batches(uploads_dir, pipeline_type):
    """پردازش فایل‌ها به صورت Batch"""
    if pipeline_type == 'excel':
        excel_files = list(uploads_dir.glob("*.xlsx")) + list(uploads_dir.glob("*.xls"))
        return [(f,) for f in excel_files], 1
    
    elif pipeline_type == 'ocr_qr':
        image_files = []
        pdf_files = []
        
        for f in uploads_dir.iterdir():
            if f.is_file():
                ext = f.suffix.lower()
                if ext in ['.jpg', '.jpeg', '.png', '.bmp', '.webp', '.gif']:
                    image_files.append(f)
                elif ext == '.pdf':
                    pdf_files.append(f)
        
        image_batches = create_batches(image_files, 5) if image_files else []
        pdf_batches = create_batches(pdf_files, 4) if pdf_files else []
        all_batches = image_batches + pdf_batches
        
        if image_files and pdf_files:
            avg_batch_size = (5 + 4) / 2
        elif image_files:
            avg_batch_size = 5
        elif pdf_files:
            avg_batch_size = 4
        else:
            avg_batch_size = 1
        
        return all_batches, int(avg_batch_size)
    
    return [], 1

# =========================================================
# 🔄 اجرای اسکریپت با Fast Mode + Log File
# =========================================================
def run_script(script_name, session_dir, log_area, status_text, script_display_name="", fast_mode=True):
    script_path = Path(script_name)
    if not script_display_name:
        script_display_name = script_name
    if not script_path.exists():
        script_path = Path.cwd() / script_name
        if not script_path.exists():
            status_text.markdown(f"""
            <div class="status-box status-error">❌ فایل {script_name} یافت نشد!</div>
            """, unsafe_allow_html=True)
            return False

    status_text.markdown(f"""
    <div class="status-box status-info">
        <div class="loading-spinner"></div> در حال اجرای {script_display_name}...
    </div>
    """, unsafe_allow_html=True)

    logs_dir = session_dir / "logs"
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f"log_{script_path.stem}_{timestamp}.txt"

    env = os.environ.copy()
    env["SESSION_DIR"] = str(session_dir)
    env["SOURCE_FOLDER"] = str(session_dir / "uploads")

    try:
        with subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=Path.cwd(),
            env=env,
            text=True,
            bufsize=1
        ) as process:
            all_output = ""
            line_count = 0
            with open(log_file, "w", encoding="utf-8") as log_f:
                for line in process.stdout:
                    all_output += line
                    log_f.write(line)
                    log_f.flush()
                    line_count += 1
                    if fast_mode:
                        if line_count % 10 == 0:
                            log_area.code(all_output[-2000:], language="bash")
                    else:
                        log_area.code(all_output[-3000:], language="bash")
                        time.sleep(0.05)
            process.wait()

        if process.returncode == 0:
            status_text.markdown(f"""
            <div class="status-box status-success">✅ {script_display_name} موفقیت‌آمیز بود!</div>
            """, unsafe_allow_html=True)
            return True
        else:
            status_text.markdown(f"""
            <div class="status-box status-warning">⚠️ {script_display_name} با مشکل مواجه شد (exit code: {process.returncode})</div>
            """, unsafe_allow_html=True)
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if lines:
                        st.code(''.join(lines[-50:]), language='bash')
            except:
                pass
            return False

    except Exception as e:
        status_text.markdown(f"""
        <div class="status-box status-error">❌ خطای اجرا: {str(e)}</div>
        """, unsafe_allow_html=True)
        return False

# =========================================================
# 🎯 Header
# =========================================================
st.markdown("""
<div class="main-header">
    <h1>🎯 Smart Exhibition Pipeline</h1>
    <p>تشخیص هوشمند • پردازش خودکار • خروجی یکپارچه • Batch Processing • Quality Control • Google Sheets</p>
</div>
""", unsafe_allow_html=True)

# =========================================================
# 📊 Sidebar
# =========================================================

# ========== لینک سریع به Google Sheets ==========
if 'sheet_url' in st.session_state:
    st.sidebar.markdown(f"""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                padding: 1rem; border-radius: 10px; margin-bottom: 1rem;">
        <h4 style="color: white; margin: 0 0 0.5rem 0;">📊 جدول داده‌ها</h4>
        <a href="{st.session_state['sheet_url']}" target="_blank" 
           style="color: white; background: rgba(255,255,255,0.2); 
                  padding: 0.5rem 1rem; border-radius: 8px; 
                  text-decoration: none; display: block; text-align: center;">
            🔗 باز کردن جدول
        </a>
    </div>
    """, unsafe_allow_html=True)
elif Path("google_sheet_link.txt").exists():
    try:
        saved_link = Path("google_sheet_link.txt").read_text(encoding='utf-8')
        url_line = [line for line in saved_link.split('\n') if line.startswith('https://')]
        if url_line:
            saved_url = url_line[0]
            st.sidebar.markdown(f"""
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        padding: 1rem; border-radius: 10px; margin-bottom: 1rem;">
                <h4 style="color: white; margin: 0 0 0.5rem 0;">📊 جدول داده‌ها</h4>
                <a href="{saved_url}" target="_blank" 
                   style="color: white; background: rgba(255,255,255,0.2); 
                          padding: 0.5rem 1rem; border-radius: 8px; 
                          text-decoration: none; display: block; text-align: center;">
                    🔗 باز کردن جدول
                </a>
                <p style="color: rgba(255,255,255,0.8); font-size: 0.85rem; margin: 0.5rem 0 0 0;">
                    لینک ذخیره شده
                </p>
            </div>
            """, unsafe_allow_html=True)
    except:
        pass
# ========== پایان لینک سریع ==========

quota = load_quota()
st.sidebar.markdown(f"""
<div class="quota-card">
    <h3>📊 API Quota امروز</h3>
    <div class="quota-number">{quota['remaining']}</div>
    <p>از {DAILY_LIMIT} درخواست</p>
</div>
""", unsafe_allow_html=True)
progress_value = quota['used'] / DAILY_LIMIT if DAILY_LIMIT > 0 else 0
st.sidebar.progress(progress_value)

if quota['remaining'] <= 0:
    st.sidebar.markdown('<span class="badge badge-error">❌ سهمیه تمام شد</span>', unsafe_allow_html=True)
elif quota['remaining'] < 20:
    st.sidebar.markdown('<span class="badge badge-warning">⚠️ کم شده</span>', unsafe_allow_html=True)
else:
    st.sidebar.markdown('<span class="badge badge-success">✅ سهمیه خوب</span>', unsafe_allow_html=True)

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ تنظیمات")
rate_limit = st.sidebar.slider("⏱️ فاصله بین درخواست‌ها (ثانیه)", 0, 10, 4)
if rate_limit < 4:
    st.sidebar.markdown('<span class="badge badge-error">⚠️ خطر Block</span>', unsafe_allow_html=True)
elif rate_limit == 4:
    st.sidebar.markdown('<span class="badge badge-success">✅ ایمن (15 RPM)</span>', unsafe_allow_html=True)
else:
    st.sidebar.markdown('<span class="badge badge-success">🔒 خیلی ایمن</span>', unsafe_allow_html=True)

debug_mode = st.sidebar.checkbox("🐛 Debug Mode")
fast_mode = st.sidebar.checkbox("⚡️ Fast Mode", value=True)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔑 وضعیت کلیدها")
for key_name, key_value in API_KEYS.items():
    st.sidebar.text(f"{key_name.upper()}: {key_value[:20]}...")

st.sidebar.markdown("---")
st.sidebar.markdown("### 📦 Batch Processing")
st.sidebar.info("📸 تصاویر: 30 تا\n📄 PDF: 10 تا\n📊 Excel: 5 تا")

# =========================================================
# 📂 آپلود فایل‌ها با محدودیت
# =========================================================
MAX_IMAGES = 30
MAX_PDF = 10
MAX_EXCEL = 5
MAX_FILE_SIZE_MB = 50
MAX_TOTAL_SIZE_MB = 200

uploaded_files = st.file_uploader(
    "📤 آپلود فایل‌ها",
    type=['jpg', 'jpeg', 'png', 'pdf', 'xlsx', 'xls'],
    accept_multiple_files=True,
    help=f"حداکثر: {MAX_IMAGES} عکس، {MAX_PDF} PDF، {MAX_EXCEL} Excel | هر فایل: {MAX_FILE_SIZE_MB}MB | کل: {MAX_TOTAL_SIZE_MB}MB"
)

# ✅ چک کردن محدودیت‌ها
if uploaded_files:
    # ============ چک سایز فایل‌ها ============
    valid_files = []
    total_size_mb = 0
    size_warnings = []
    
    for f in uploaded_files:
        file_size_mb = len(f.getbuffer()) / (1024 * 1024)
        total_size_mb += file_size_mb
        
        # چک سایز فایل منفرد
        if file_size_mb > MAX_FILE_SIZE_MB:
            size_warnings.append(f"❌ {f.name}: {file_size_mb:.1f}MB (خیلی بزرگ، حداکثر {MAX_FILE_SIZE_MB}MB)")
            continue
        
        valid_files.append(f)
    
    # چک سایز کل
    if total_size_mb > MAX_TOTAL_SIZE_MB:
        st.error(f"❌ مجموع حجم فایل‌ها: {total_size_mb:.1f}MB (حداکثر مجاز: {MAX_TOTAL_SIZE_MB}MB)")
        st.stop()
    
    # نمایش هشدارهای سایز
    if size_warnings:
        st.warning("⚠️ فایل‌های زیر به دلیل حجم بالا رد شدند:")
        for warn in size_warnings:
            st.text(f"  {warn}")
    
    if not valid_files:
        st.error("❌ هیچ فایل معتبری آپلود نشد!")
        st.stop()
    
    # استفاده از فایل‌های معتبر
    uploaded_files = valid_files
    
    # جداسازی فایل‌ها بر اساس نوع
    images = [f for f in uploaded_files if f.type.startswith('image')]
    pdfs = [f for f in uploaded_files if f.type == 'application/pdf']
    excels = [f for f in uploaded_files if 'spreadsheet' in f.type or f.name.endswith(('.xlsx', '.xls'))]
    
    # ============ چک تعداد فایل‌ها ============
    warnings = []
    
    if len(images) > MAX_IMAGES:
        warnings.append(f"⚠️ تعداد عکس‌ها: {len(images)} → محدود شد به {MAX_IMAGES}")
        images = images[:MAX_IMAGES]
    
    if len(pdfs) > MAX_PDF:
        warnings.append(f"⚠️ تعداد PDF: {len(pdfs)} → محدود شد به {MAX_PDF}")
        pdfs = pdfs[:MAX_PDF]
    
    if len(excels) > MAX_EXCEL:
        warnings.append(f"⚠️ تعداد Excel: {len(excels)} → محدود شد به {MAX_EXCEL}")
        excels = excels[:MAX_EXCEL]
    
    # نمایش هشدارها
    if warnings:
        st.warning("محدودیت تعداد اعمال شد:")
        for warn in warnings:
            st.text(f"  {warn}")
    
    # نمایش آمار (با اضافه کردن حجم)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("🖼️ عکس", f"{len(images)}/{MAX_IMAGES}")
    with col2:
        st.metric("📄 PDF", f"{len(pdfs)}/{MAX_PDF}")
    with col3:
        st.metric("📊 Excel", f"{len(excels)}/{MAX_EXCEL}")
    with col4:
        st.metric("📁 مجموع", len(images) + len(pdfs) + len(excels))
    with col5:
        st.metric("💾 حجم کل", f"{total_size_mb:.1f}MB")
    
    # نمایش بار پیشرفت حجم
    size_progress = min(total_size_mb / MAX_TOTAL_SIZE_MB, 1.0)
    st.progress(size_progress)
    if size_progress > 0.8:
        st.warning(f"⚠️ حجم فایل‌ها زیاد است ({total_size_mb:.1f}/{MAX_TOTAL_SIZE_MB}MB)")
    
    # ترکیب مجدد فایل‌های مجاز
    uploaded_files = images + pdfs + excels

# =========================================================
# ✨ Quality Control Section
# =========================================================
st.markdown("## 👤 اطلاعات ناظر کیفیت")
st.markdown("*این اطلاعات به عنوان متادیتای کنترل کیفیت در خروجی ثبت می‌شود*")

col_qc1, col_qc2 = st.columns(2)
with col_qc1:
    qc_user_name = st.text_input(
        "🧑‍💼 نام و نام خانوادگی",
        placeholder="مثال: علی احمدی",
        help="نام کامل ناظر کیفیت داده‌ها"
    )
with col_qc2:
    qc_user_role = st.text_input(
        "💼 سمت/نقش",
        placeholder="مثال: کارشناس کنترل کیفیت",
        help="سمت یا نقش شما در سازمان"
    )

if qc_user_name and qc_user_role:
    qc_preview = get_qc_metadata(qc_user_name, qc_user_role)
    st.markdown(f"""
    <div class="qc-card">
        <h4>✅ پیش‌نمایش اطلاعات کنترل کیفیت</h4>
        <p><strong>👤 ناظر:</strong> {qc_preview['QC_Supervisor']}</p>
        <p><strong>💼 نقش:</strong> {qc_preview['QC_Role']}</p>
        <p><strong>📅 تاریخ:</strong> {qc_preview['QC_Date']}</p>
        <p><strong>🕐 ساعت:</strong> {qc_preview['QC_Time']}</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

if uploaded_files:
    pipeline_type = detect_pipeline_type(uploaded_files)
    exhibition_name = extract_exhibition_name(uploaded_files)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🔍 نوع Pipeline</h3>
            <h2>{'📊 Excel' if pipeline_type == 'excel' else '🖼 OCR/QR'}</h2>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>📁 تعداد فایل</h3>
            <h2>{len(uploaded_files)}</h2>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🏢 نمایشگاه</h3>
            <h2>{exhibition_name[:15]}</h2>
        </div>
        """, unsafe_allow_html=True)

    exhibition_name = st.text_input(
        "📝 ویرایش نام نمایشگاه",
        value=exhibition_name,
        help="در ستون Exhibition ثبت می‌شود"
    )

    session_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    session_dir = Path(f"session_{session_timestamp}")
    uploads_dir = session_dir / "uploads"
    logs_dir = session_dir / "logs"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)


    file_types = {}
    for f in uploaded_files:
        (uploads_dir / f.name).write_bytes(f.getbuffer())
        file_types[f.name] = detect_source_type(f.name)
    
    file_types_path = session_dir / "uploaded_file_types.json"
    file_types_path.write_text(json.dumps(file_types, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"   Saved file types: {file_types}")

    os.environ["SESSION_DIR"] = str(session_dir)
    os.environ["SOURCE_FOLDER"] = str(uploads_dir)
    os.environ["EXHIBITION_NAME"] = exhibition_name

    if pipeline_type == 'excel':
        excel_files = list(uploads_dir.glob("*.xlsx")) + list(uploads_dir.glob("*.xls"))
        if excel_files:
            os.environ["INPUT_EXCEL"] = str(excel_files[0])

    batches, batch_size = process_files_in_batches(uploads_dir, pipeline_type)
    total_batches = len(batches)
    
    if total_batches > 0:
        st.info(f"📦 تعداد Batch‌ها: {total_batches} | اندازه هر Batch: {batch_size}")

    st.markdown("---")

    if st.button("🚀 شروع پردازش", type="primary"):
        if not qc_user_name or not qc_user_role:
            st.markdown("""
            <div class="status-box status-warning">
                ⚠️ لطفاً اطلاعات ناظر کیفیت (نام و نقش) را وارد کنید!
            </div>
            """, unsafe_allow_html=True)
            st.stop()
        
        if quota['remaining'] <= 0:
            st.markdown("""
            <div class="status-box status-error">❌ سهمیه API تمام شد! فردا دوباره امتحان کنید.</div>
            """, unsafe_allow_html=True)
            st.stop()

        qc_metadata = get_qc_metadata(qc_user_name, qc_user_role)
        save_qc_log(session_dir, qc_metadata, exhibition_name, pipeline_type, len(uploaded_files))
        
        st.markdown("## 🔄 پردازش در حال انجام...")
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_area = st.empty()
        quota_display = st.empty()

        start_time = time.time()
        success = False
        output_files = []

        try:
            if pipeline_type == 'excel':
                st.markdown("""
                <div class="status-box status-info">📊 Excel Mode فعال شد</div>
                """, unsafe_allow_html=True)

                excel_input = os.environ.get("INPUT_EXCEL")
                if not excel_input or not Path(excel_input).exists():
                    st.markdown("""
                    <div class="status-box status-error">❌ فایل Excel پیدا نشد!</div>
                    """, unsafe_allow_html=True)
                    st.stop()

                try:
                    df_input = pd.read_excel(excel_input)
                    total_rows = len(df_input)
                    st.info(f"📊 تعداد شرکت‌ها: {total_rows}")
                    current_quota = load_quota()
                    if current_quota['remaining'] < total_rows:
                        st.warning(f"⚠️ Quota کافی نیست! نیاز: {total_rows}, موجود: {current_quota['remaining']}")
                        if not st.checkbox("ادامه با Quota ناکافی؟"):
                            st.stop()
                except Exception as e:
                    st.warning(f"نتوانستم تعداد ردیف‌ها را بخوانم: {e}")
                    total_rows = 0

                progress_bar.progress(10)
                current_quota = load_quota()
                quota_display.info(f"🔋 سهمیه باقیمانده: {current_quota['remaining']}/{DAILY_LIMIT}")

                st.info(f"📦 پردازش {total_rows} ردیف به صورت Batch (اندازه: 1)")
                
                success = run_script(
                    "excel_mode.py",
                    session_dir,
                    log_area,
                    status_text,
                    "📊 Excel Web Scraper",
                    fast_mode
                )
                progress_bar.progress(100)

                if total_rows > 0:
                    quota = decrease_quota(total_rows)
                    quota_display.success(f"✅ سهمیه باقیمانده: {quota['remaining']}/{DAILY_LIMIT} (استفاده شده: {total_rows})")
                else:
                    quota = decrease_quota(1)
                    quota_display.success(f"✅ سهمیه باقیمانده: {quota['remaining']}/{DAILY_LIMIT}")

                output_files = list(session_dir.glob("output_enriched_*.xlsx"))
                if not output_files:
                    output_files = [f for f in session_dir.glob("**/*.xlsx")
                                    if "output" in f.name.lower() or "enriched" in f.name.lower()]

            else:
                st.markdown("""
                <div class="status-box status-info">🖼 OCR/QR Pipeline فعال شد</div>
                """, unsafe_allow_html=True)

                if total_batches > 0:
                    st.info(f"📦 پردازش {total_batches} Batch | هر Batch حدود {batch_size} فایل")

                stages = [
                    ("📘 OCR Extraction", "ocr_dyn.py", 20),
                    ("🔍 QR Detection", "qr_dyn.py", 40),
                    ("🧩 Merge OCR+QR", "mix_ocr_qr_dyn.py", 60),
                    ("🌐 Web Scraping", "scrap.py", 80),
                    ("💠 Final Merge", "final_mix.py", 100)
                ]

                all_success = True
                for stage_name, script, progress_val in stages:
                    current_quota = load_quota()
                    quota_display.info(f"🔋 سهمیه باقیمانده: {current_quota['remaining']}/{DAILY_LIMIT}")

                    if total_batches > 0:
                        st.markdown(f"**{stage_name}** - پردازش {total_batches} Batch...")

                    stage_success = run_script(
                        script, session_dir, log_area, status_text,
                        stage_name, fast_mode
                    )
                    if not stage_success:
                        all_success = False
                        st.markdown(f"""
                        <div class="status-box status-warning">⚠️ {stage_name} با مشکل مواجه شد، ادامه می‌دهیم...</div>
                        """, unsafe_allow_html=True)

                    progress_bar.progress(progress_val)
                    time.sleep(rate_limit)
                    
                    quota_decrease_amount = max(1, total_batches)
                    quota = decrease_quota(quota_decrease_amount)
                    quota_display.success(f"✅ سهمیه باقیمانده: {quota['remaining']}/{DAILY_LIMIT}")
                    
                    if quota['remaining'] <= 0:
                        st.markdown('<div class="status-box status-error">❌ سهمیه API تمام شد!</div>', unsafe_allow_html=True)
                        break

                success = all_success
                output_files = list(session_dir.glob("merged_final_*.xlsx"))
                if not output_files:
                    output_files = [f for f in session_dir.glob("**/*.xlsx")
                                    if any(kw in f.name.lower() for kw in ["merged", "final", "output"])]

            elapsed = time.time() - start_time

            if success and output_files:
                st.info("📝 در حال اضافه کردن Exhibition، Source و QC Metadata...")
                for output_file in output_files:
                    add_exhibition_and_source(
                    
                        excel_path=output_file, 
                        exhibition_name=exhibition_name, 
                        session_dir=session_dir,
                        qc_metadata=qc_metadata
                    )
                    
                    add_qc_metadata_to_excel(output_file, qc_metadata)
                
            
                
                # ========== GOOGLE SHEETS UPLOAD ==========
                st.markdown("---")
                st.markdown("## ذخیره داده‌ها در Google Drive")
                st.info("تمام داده‌ها (OCR/QR + Web Scraping) با هم ذخیره می‌شوند")
                sheets_status = st.empty()
                sheets_status.info("در حال ادغام و آماده‌سازی داده‌ها...")
                try:
                    # ادغام تمام منابع داده
                    merged_excel = merge_all_data_sources(session_dir, pipeline_type)

                    if not merged_excel or not merged_excel.exists():
                        sheets_status.warning("هیچ داده‌ای برای آپلود پیدا نشد")
                    else:
                        # 🔹 تمیزکاری فقط برای OCR/QR Mode
                        if pipeline_type == 'ocr_qr':
                            sheets_status.info("🧹 در حال تمیزکاری داده‌ها...")
                            
                            try:
                                from script2 import script2_process_file
                                
                                # نام فایل خروجی
                                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                                processed_excel = session_dir / f"merged_complete_processed_{timestamp}.xlsx"
                                
                                # اجرای Script 2
                                script2_process_file(
                                    input_path=str(merged_excel),
                                    output_path=str(processed_excel)
                                )
                                
                                # چک کن خروجی ساخته شد؟
                                if processed_excel.exists():
                                    sheets_status.success("✅ داده‌ها تمیز شدند")
                                    final_file = processed_excel  # ← از این برای آپلود استفاده کن
                                else:
                                    sheets_status.warning("⚠️ تمیزکاری کار نکرد، از فایل اولیه استفاده می‌شود")
                                    final_file = merged_excel
                            
                            except Exception as e:
                                sheets_status.warning(f"⚠️ خطا در تمیزکاری: {e}")
                                final_file = merged_excel
                        
                        else:
                            # Excel Mode: بدون تمیزکاری
                            sheets_status.info("📊 Excel Mode - بدون تمیزکاری")
                            final_file = merged_excel
                        
                        # آپلود به گوگل
                        sheets_status.info(f"در حال آپلود {final_file.name}...")
                        folder_id = get_or_create_folder("Exhibition_Data")

                        success_gs, msg_gs, url_gs, total_rows = append_excel_data_to_sheets(
                            excel_path=final_file,
                            folder_id=folder_id,
                            exhibition_name=exhibition_name,
                            qc_metadata=qc_metadata
                        )
                                            
                        if success_gs:
                            sheets_status.markdown(f"""
                            <div class="status-box status-success">
                                {msg_gs}
                            </div>
                            """, unsafe_allow_html=True)
                            
                            st.markdown(f"""
                            <div class="file-display" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                                <h4>لینک دائمی جدول</h4>
                                <p style="background: rgba(255,255,255,0.2); padding: 1rem; border-radius: 8px; margin: 0.5rem 0;">
                                    <a href="{url_gs}" target="_blank" style="color: white; font-weight: bold; font-size: 1.1rem;">
                                        باز کردن در Google Drive
                                    </a>
                                </p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        else:
                            sheets_status.error(f"خطا: {msg_gs}")
                except Exception as e:
                    sheets_status.error(f"خطا در ادغام داده‌ها: {e}")
                    import traceback
                    traceback.print_exc()

                # ========== END GOOGLE SHEETS ==========
                # ========== END GOOGLE SHEETS ==========

            st.markdown("---")

            if success and output_files:
                st.markdown("""
                <div class="status-box status-success">
                    <h2>🎉 پردازش با موفقیت کامل شد!</h2>
                </div>
                """, unsafe_allow_html=True)

                st.markdown(f"""
                <div class="qc-card">
                    <h4>👤 اطلاعات ناظر کیفیت</h4>
                    <p><strong>ناظر:</strong> {qc_metadata['QC_Supervisor']} | <strong>نقش:</strong> {qc_metadata['QC_Role']}</p>
                    <p><strong>تاریخ و ساعت:</strong> {qc_metadata['QC_Timestamp']}</p>
                </div>
                """, unsafe_allow_html=True)

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>⏱️ زمان اجرا</h3>
                        <h2>{elapsed:.1f}s</h2>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    quota_now = load_quota()
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>🔋 سهمیه باقیمانده</h3>
                        <h2>{quota_now['remaining']}</h2>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>📊 فایل خروجی</h3>
                        <h2>{len(output_files)}</h2>
                    </div>
                    """, unsafe_allow_html=True)

            
                st.markdown("## دانلود فایل‌های نهایی")
                # اضافه کردن فایل merged_complete
                merged_files = list(session_dir.glob("merged_complete_*.xlsx"))
                if merged_files:
                    all_output_files = merged_files + output_files
                else:
                    all_output_files = output_files
                for output_file in all_output_files:

                    with st.container():
                        colA, colB = st.columns([3, 1])
                        with colA:
                            st.markdown(f"""
                            <div class="file-display">
                                <h4>📄 {output_file.name}</h4>
                                <p>حجم: {output_file.stat().st_size / 1024:.1f} KB</p>
                            </div>
                            """, unsafe_allow_html=True)
                        with colB:
                            with open(output_file, "rb") as f:
                                st.download_button(
                                    label="⬇️ دانلود",
                                    data=f,
                                    file_name=output_file.name,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"download_{output_file.name}"
                                )
                        try:
                            df_prev = pd.read_excel(output_file)
                            for c in df_prev.columns:
                                if df_prev[c].dtype == 'object':
                                    df_prev[c] = df_prev[c].astype(str).replace('nan', '')
                            with st.expander(f"👁 پیش‌نمایش {output_file.name}"):
                                st.markdown(f"""
                                <div class="status-box status-info" style="margin-top:0;">
                                    <p style="margin:0;">📊 <strong>{len(df_prev)}</strong> ردیف × 
                                       <strong>{len(df_prev.columns)}</strong> ستون</p>
                                </div>
                                """, unsafe_allow_html=True)
                                cols_display = ", ".join(df_prev.columns.tolist()[:20])
                                if len(df_prev.columns) > 20: cols_display += "..."
                                st.info(f"🔤 ستون‌ها: {cols_display}")
                                st.dataframe(df_prev.head(10), width='stretch')
                        except Exception as e:
                            st.warning(f"⚠️ خطا در نمایش پیش‌نمایش: {e}")

                json_files = [f for f in session_dir.glob("*.json") if f.name != "quota.json"]
                if json_files:
                    with st.expander("📄 فایل‌های JSON و لاگ‌ها (اختیاری)"):
                        for json_file in json_files:
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                if json_file.name == "qc_log.json":
                                    st.write(f"**👤 {json_file.name}** (لاگ کنترل کیفیت)")
                                else:
                                    st.write(f"**{json_file.name}**")
                            with col2:
                                with open(json_file, "rb") as f:
                                    st.download_button(
                                        label="⬇️ دانلود",
                                        data=f,
                                        file_name=json_file.name,
                                        mime="application/json",
                                        key=f"download_json_{json_file.name}"
                                    )
                st.balloons()

            else:
                st.markdown("""
                <div class="status-box status-warning">
                    <h2>⚠️ پردازش کامل نشد</h2>
                    <p>بعضی داده‌ها پردازش نشدند. لاگ‌ها را بررسی کنید.</p>
                </div>
                """, unsafe_allow_html=True)
                st.info("💡 نکته: اگر شرکتی URL نداشته باشد، نمی‌توان اطلاعات آن را از وب دریافت کرد.")
                if debug_mode:
                    with st.expander("🔍 لیست فایل‌های Session"):
                        for f in session_dir.rglob("*"):
                            if f.is_file():
                                st.write(f"📄 {f.relative_to(session_dir)}")

        except Exception as e:
            st.markdown("""
            <div class="status-box status-error">
                <h2>❌ خطای غیرمنتظره</h2>
            </div>
            """, unsafe_allow_html=True)
            st.error(f"خطا: {str(e)}")
            if debug_mode:
                import traceback
                with st.expander("📋 جزئیات خطا"):
                    st.code(traceback.format_exc())

else:
    st.markdown("""
    <div class="status-box status-info">
        <h3>👋 خوش آمدید!</h3>
        <p>لطفاً ابتدا اطلاعات ناظر کیفیت را وارد کنید، سپس فایل‌های خود را آپلود کنید</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    padding: 2rem; border-radius: 15px; color: white; height: 100%;">
            <h3>📊 Excel Mode</h3>
            <ul style="line-height: 2;">
                <li>فایل Excel با URL/Website</li>
                <li>وب‌اسکرپینگ هوشمند</li>
                <li>استخراج اطلاعات کامل شرکت</li>
                <li>خروجی: Excel غنی‌شده</li>
                <li>📦 Batch: 1 ردیف</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                    padding: 2rem; border-radius: 15px; color: white; height: 100%;">
            <h3>🖼 OCR/QR Mode</h3>
            <ul style="line-height: 2;">
                <li>تصاویر (JPG, PNG) یا PDF</li>
                <li>استخراج OCR + تشخیص QR</li>
                <li>وب‌اسکرپینگ از URLها</li>
                <li>خروجی: Excel یکپارچه</li>
                <li>📦 Batch: تصاویر(5) | PDF(4)</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### ✨ ویژگی‌های کلیدی")
    features = [
        ("🎯", "تشخیص خودکار", "Excel یا OCR/QR به صورت هوشمند"),
        ("🏢", "Exhibition Field", "نام نمایشگاه قابل ویرایش"),
        ("📊", "Source Tracking", "تشخیص منبع (Image/PDF/Excel)"),
        ("🤖", "Smart Position", "50+ دپارتمان فارسی/انگلیسی"),
        ("🔋", "Quota Management", "مدیریت هوشمند API (240/روز)"),
        ("⚡️", "Fast Mode", "پردازش سریع با لاگ بهینه"),
        ("🔒", "Rate Limit", "4 ثانیه (ایمن - 15 RPM)"),
        ("📦", "Batch Processing", "تصاویر(5) | PDF(4) | Excel(1)"),
        ("👤", "Quality Control", "ثبت نام و نقش ناظر کیفیت"),
        ("☁️", "Google Sheets", "ذخیره خودکار در Drive")
    ]
    cols = st.columns(3)
    for idx, (icon, title, desc) in enumerate(features):
        with cols[idx % 3]:
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem; background: white; 
                        border-radius: 10px; margin: 0.5rem 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                <div style="font-size: 2rem;">{icon}</div>
                <h4 style="margin: 0.5rem 0; color: #667eea;">{title}</h4>
                <p style="margin: 0; font-size: 0.85rem; color: #666;">{desc}</p>
            </div>
            """, unsafe_allow_html=True)

st.markdown("---")
st.markdown("""
<div style="text-align: center; padding: 2rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            border-radius: 15px; color: white; margin-top: 2rem;">
    <h4>🚀 Smart Exhibition Pipeline + Google Sheets</h4>
    <p style="margin: 0.5rem 0;">
        ⚡️ Rate Limiting: 4s (ایمن) | 🔒 API Limit: 15 RPM, 240/روز
    </p>
    <p style="margin: 0.5rem 0;">
        📌 Exhibition + Source Tracking | 🤖 Smart Position Detection
    </p>
    <p style="margin: 0.5rem 0;">
        📦 Batch Processing: تصاویر(5) | PDF(4) | Excel(1)
    </p>
    <p style="margin: 0.5rem 0;">
        👤 Quality Control Tracking: نام، نقش، تاریخ، ساعت
    </p>
    <p style="margin: 0.5rem 0;">
        ☁️ Google Sheets: ذخیره خودکار داده‌ها در Drive
    </p>
    <p style="margin: 1rem 0 0 0; opacity: 0.8; font-size: 0.9rem;">
        Made with ❤️ using Streamlit & Gemini AI
    </p>
</div>
""", unsafe_allow_html=True)
