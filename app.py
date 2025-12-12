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




# ---------------------------------------- generate permanent company id 
import hashlib
import re

def generate_company_id(company_name_fa=None, company_name_en=None):
    
    # select company name
    company_name = None
    
    if company_name_fa and str(company_name_fa).strip() not in ['', 'nan', 'None']:
        company_name = str(company_name_fa).strip()
    elif company_name_en and str(company_name_en).strip() not in ['', 'nan', 'None']:
        company_name = str(company_name_en).strip()
    
    if not company_name:
        # if company name doesn't exist, give random id
        import random
        random_hash = hashlib.md5(str(random.random()).encode()).hexdigest()[:12].upper()
        return f"COMP_UNKNOWN_{random_hash}"
    
   # normalize company name (remove extra words)
    normalized = company_name.lower()
    
    # remove common words
    for word in ['شرکت', 'company', 'co.', 'co', 'ltd', 'inc', 'group', 'گروه', 
                 'corporation', 'corp', '.', ',', '-', '_']:
        normalized = normalized.replace(word, ' ')
    
    # remove extra spaces
    normalized = ' '.join(normalized.split())
    normalized = normalized.strip()
    
    # if empty after normalization
    if not normalized or len(normalized) < 2:
        normalized = company_name.lower()
    
    # hash 
    hash_object = hashlib.sha256(normalized.encode('utf-8'))
    hash_hex = hash_object.hexdigest()[:12].upper()
    
    # final format
    company_id = f"COMP_{hash_hex}"
    
    return company_id





def add_company_id_to_dataframe(df, log_details=True):
    """
    add companyid column to dataframe
    
    args:
        df: input dataframe
        log_details: show details in console
    
    returns:
        dataframe with companyid column
    """

    import pandas as pd
    
    if df.empty:
        print("   ⚠️ DataFrame is empty, skipping CompanyID")
        return df
    
    print(f"\n🆔 Generating Hash-based Company IDs...")
    print(f"   📊 Processing {len(df)} rows...")
    
    company_ids = []
    id_mapping = {}  # for tracking duplicates
    
    for idx, row in df.iterrows():

        # extract company name from row
        company_name_fa = None
        company_name_en = None

        for col in ['CompanyNameFA', 'CompanyNameEN', 'company_name_fa', 'company_name_en']:
            if col in row and row[col]:
                if 'FA' in col or 'fa' in col:
                    company_name_fa = row[col]
                else:
                    company_name_en = row[col]

        # generate Company ID
        company_id = generate_company_id(company_name_fa, company_name_en)



        
        company_ids.append(company_id)
        
        # track duplicates
        if company_id not in id_mapping:
            id_mapping[company_id] = []
        id_mapping[company_id].append(idx + 1)
        
        # show first 5 samples
        if log_details and idx < 5:
            company_name = ""
            for col in ['CompanyNameFA', 'CompanyNameEN', 'company_name_fa', 'company_name_en']:
                if col in row and row[col]:
                    company_name = str(row[col])[:20]
                    break
            
            print(f"      Row {idx + 1}: {company_id} → {company_name}")
    
    # add to dataframe (first column)
    df.insert(0, 'CompanyID', company_ids)
    
    # statistics
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
    merge all data sources (for both modes):
    
    ocr/qr mode:
        - mix_ocr_qr.json (always)
        - gemini_scrap_output.json (if available)
    
    excel mode:
        - web_analysis.xlsx (always)
    
    returns:
        path: final excel file path
    """

    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    print(f"\nStarting data merge for {pipeline_type.upper()} mode...")
    
    # paths
    mix_json = Path(session_dir) / "mix_ocr_qr.json"
    scrap_json = Path(session_dir) / "gemini_scrap_output.json"
    web_excel = Path(session_dir) / "web_analysis.xlsx"
    output_enriched = list(Path(session_dir).glob("output_enriched_*.xlsx"))
    
    # excel mode
    if pipeline_type == 'excel':
        print("   Excel Mode detected")
        
        # 1. first check output_enriched
        if output_enriched:
            excel_file = output_enriched[0]
            print(f"   Using output_enriched: {excel_file.name}")
        
        # 2. if not found, check web_analysis
        elif web_excel.exists():
            excel_file = web_excel
            print(f"   Using web_analysis: {excel_file.name}")
        
        else:
            print(f"   No Excel output found!")
            return None
        
        # read and clean
        df = pd.read_excel(excel_file)
        
        # clean
        df = df.fillna("")
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace({
                    'nan': '', 'None': '', 'NaT': '', '<NA>': '', 'null': '', 'NULL': ''
                })

        # ------------------------ translation (new )
        print(f"\n🌐 Starting automatic translation...")
        df = translate_all_columns(df)
        #--------------------------------------------------------

        # save
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(session_dir) / f"merged_complete_{timestamp}.xlsx"
        df.to_excel(output_path, index=False, engine='openpyxl')
        
        print(f"\n   Excel Mode completed!")
        print(f"      Rows: {len(df)}")
        print(f"      Columns: {len(df.columns)}")
        print(f"      Saved to: {output_path.name}")
        
        return output_path
    
    #---------------------------------- ocr/qr mode 
    elif pipeline_type == 'ocr_qr':
        print("   OCR/QR Mode detected")
        
        # 1. read mix_ocr_qr.json (mandatory)
        if not mix_json.exists():
            print(f"   {mix_json.name} not found!")
            return None
        
        print(f"   Reading {mix_json.name}...")
        try:
            with open(mix_json, 'r', encoding='utf-8') as f:
                mix_data = json.load(f)
            
            # convert to dataframe
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
                
                for page_result in page_results:
                    if not isinstance(page_result, dict) or not page_result:
                        continue
                    
                    record = {}
                    
                    for key, value in page_result.items():
                        if key in ['ocr_text']:
                            continue
                        
                        if isinstance(value, list):
                            if not value:
                                continue
                            
                            for idx, item in enumerate(value, 1):
                                if item is not None:
                                    col_name = key if idx == 1 else f"{key}{idx}"
                                    record[col_name] = str(item)
                        
                        elif value is not None and str(value).strip():
                            record[key] = str(value)
                
                    
                    # make sure file_name is taken from file_item
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
        
        # 2. read gemini_scrap_output.json (optional)
        if not scrap_json.exists():
            print(f"   {scrap_json.name} not found - using only OCR/QR data")
            
            # just mix_ocr_qr
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
        
        # 3. read and process scraping data
        
        print(f"   Reading {scrap_json.name}...")
        try:
            with open(scrap_json, 'r', encoding='utf-8') as f:
                scrap_data = json.load(f)
            
            if not isinstance(scrap_data, list):
                print(f"   Invalid scrap data format")
                df_scrap = pd.DataFrame()
            else:
                df_scrap = pd.DataFrame(scrap_data)
                
                # only successful ones
                if 'status' in df_scrap.columns:
                    df_scrap = df_scrap[df_scrap['status'] == 'SUCCESS'].copy()
                
                # remove extra columns
                for col in ['status', 'error']:
                    if col in df_scrap.columns:
                        df_scrap.drop(columns=[col], inplace=True)
                
                # add file_name from ocr/qr to scraping
                if not df_scrap.empty:
                    print(f"   🔗 Matching file_names from OCR/QR to Scraping...")
                    
                    # normalize_url function
                    def normalize_url(url):
                        if not url or pd.isna(url):
                            return ""
                        url = str(url).strip().lower()
                        url = url.replace('http://', '').replace('https://', '').replace('www.', '')
                        return url.split('/')[0].split('?')[0]
                    
                    # create dictionary: website → file_name
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
                    
                    # add file_name to scraping
                    matched_count = 0
                    # if doesn't have file_name, add it
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


                        
                        print(f"   Matched {matched_count}/{len(df_scrap)} scraping records with file_name")

                        # fill empty file_names
                        print(f"\n   🔧 Filling empty file_names for Web rows...")

                        if 'file_name' in df_scrap.columns:
                            # if some rows don't have file_name, use the first available file_name
                            if url_to_filename:
                                # get first file_name from dictionary
                                default_filename = list(url_to_filename.values())[0]
                                
                                empty_count = 0
                                for idx in df_scrap.index:
                                    fname = df_scrap.at[idx, 'file_name']
                                    if not fname or pd.isna(fname) or str(fname).strip() in ['', 'Unknown']:
                                        df_scrap.at[idx, 'file_name'] = default_filename
                                        empty_count += 1
        
                                print(f"      ✅ Filled {empty_count} empty file_names with: {default_filename}")
                    
                    print(f"      ✅ Matched {matched_count}/{len(df_scrap)} scraping records with file_name")
                    
                    # remove duplicate scraping rows
                    print(f"\n   🧹 Removing duplicate scraping records...")
                    
                    initial_count = len(df_scrap)
                    
                    if 'Website' in df_scrap.columns or 'urls' in df_scrap.columns:
                        url_col = 'Website' if 'Website' in df_scrap.columns else 'urls'
                        
                        # normalize url 
                        df_scrap['_normalized_url'] = df_scrap[url_col].apply(normalize_url)
                        
                        # remove duplicates (keep first)
                        df_scrap = df_scrap.drop_duplicates(subset=['_normalized_url'], keep='first')
                        
                        # remove helper column
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
        
        # 4. merge ocr/qr + scraping (without merging rows)
        print(f"\n   Concatenating OCR/QR + Web Scraping (separate rows)...")
        
        # just concat, without merging
        df_final = pd.concat([df_mix, df_scrap], ignore_index=True)
        
        # cleaning
        df_final = df_final.fillna("")
        for col in df_final.columns:
            if df_final[col].dtype == 'object':
                df_final[col] = df_final[col].astype(str).str.strip()
                df_final[col] = df_final[col].replace({
                    'nan': '', 'None': '', 'NaT': '', '<NA>': '', 'null': '', 'NULL': ''
                })
        
        # generate unique companyid for each file_name
        print(f"\n🆔 Generating unique CompanyID for each file_name...")
        
        if 'file_name' in df_final.columns:
            # create dictionary: file_name → companyid
            file_to_company_id = {}
            
            for idx, row in df_final.iterrows():
                fname = row.get('file_name', '')
                
                if not fname or pd.isna(fname) or str(fname).strip() in ['', 'Unknown', 'web_only']:
                    # if doesn't have file_name, give unique id
                    company_id = generate_company_id(
                        row.get('CompanyNameFA'),
                        row.get('CompanyNameEN')
                    )
                else:
                    # if has file_name, check if created before
                    fname_str = str(fname).strip()
                    
                    if fname_str not in file_to_company_id:
                        # first time seeing this file_name
                        company_id = generate_company_id(
                            row.get('CompanyNameFA'),
                            row.get('CompanyNameEN')
                        )
                        file_to_company_id[fname_str] = company_id
                        print(f"      {fname_str} → {company_id}")
                    else:
                        # seen before, use same id
                        company_id = file_to_company_id[fname_str]
                
                # add companyid
                df_final.at[idx, 'CompanyID'] = company_id
            
            # move companyid to first
            cols = ['CompanyID'] + [col for col in df_final.columns if col != 'CompanyID']
            df_final = df_final[cols]
            
            print(f"   ✅ Generated {len(file_to_company_id)} unique CompanyIDs for {len(df_final)} rows")
        
        # sort based on file_name
        print(f"\n📑 Sorting by file_name...")
        
        if 'file_name' in df_final.columns:
            # sort: first file_name, then companyid
            df_final = df_final.sort_values(
                by=['file_name', 'CompanyID'], 
                ascending=[True, True]
            ).reset_index(drop=True)
            
            print(f"   ✅ Sorted {len(df_final)} rows by file_name")
            
            # show statistics
            file_counts = df_final['file_name'].value_counts()
            print(f"\n   📊 File Distribution:")
            for fname, count in list(file_counts.items())[:5]:
                if fname and str(fname) not in ['', 'nan', 'Unknown']:
                    print(f"      • {fname}: {count} rows")
        
        # save
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





def translate_all_columns(df, api_key="AIzaSyD***PzB70"):
    """
    translate all columns in dataframe
    - only english → persian
    """

    import google.generativeai as genai
    import time
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')
    
    print(f"\n🌐 Starting translation for {len(df)} rows...")
    
    # columns that should not be translated
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
        #detect language: fa or en
        if not text or pd.isna(text) or str(text).strip() == '':
            return None
        
        text = str(text).strip()
        
        # check persian characters
        persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
        has_persian = any(c in persian_chars for c in text)
        
        if has_persian:
            return 'fa'
        else:
            return 'en'
    
    def translate_text(text):
        ## translate english with gemini
        if not text or pd.isna(text) or str(text).strip() == '':
            return ""
        
        text = str(text).strip()
        
        try:
            prompt = f"Translate this English text to Persian. Only return the translation, no explanations:\n\n{text}"
            
            response = model.generate_content(prompt)
            translation = response.text.strip()
            
            # remove markdown & quotes
            translation = translation.replace('*', '').replace('`', '').strip('"').strip("'")
            
            return translation
        
        except Exception as e:
            print(f"   ⚠️ Translation error: {e}")
            return ""
    
    # process each column
    for col in df.columns:
        # Skip specific columns
        if col in skip_columns:
            continue
        
    
        if col.endswith('_translated') or col.endswith('FA') or col.endswith('EN'):
            continue
        
        print(f"\n   🔄 Processing column: {col}")
        
        # count non-empty cells
        non_empty = df[col].notna() & (df[col].astype(str).str.strip() != '')
        total_cells = non_empty.sum()
        
        if total_cells == 0:
            print(f"      ⏭️ Empty column, skipping")
            continue
        
        print(f"      📊 {total_cells} non-empty cells")
        
        # process each row
        translated_count = 0
        
        for idx in df.index:
            cell_value = df.at[idx, col]
            
            if not cell_value or pd.isna(cell_value) or str(cell_value).strip() == '':
                continue
            
            # language detection
            lang = detect_language(cell_value)
            
            if lang != 'en':
                
                continue
            
            
            translated = translate_text(cell_value)
            
            if translated:
                # save in new column
                new_col = f"{col}FA" if not col.endswith('EN') else col.replace('EN', 'FA')
                df.at[idx, new_col] = translated
                translated_count += 1
            
            # Rate limiting
            time.sleep(1)
        
        print(f"      ✅ Translated {translated_count} cells")
    
    print(f"\n   ✅ Translation completed!")
    return df



def append_excel_data_to_sheets(excel_path, folder_id=None, exhibition_name=None, qc_metadata=None):
    """read excel data and append to google sheets (variable row count)"""
    try:
        drive_service, sheets_service = get_google_services()
        if not drive_service or not sheets_service:
            return False, "Google connection failed", None, 0

        print(f"\n☁️ Starting data save to Google Drive...")

        # use existing google sheet instead of creating a new one
        file_id = "1OeQbiqvo***27yuE"
        file_url = f"https://docs.google.com/spreadsheets/d/{file_id}/edit"
        exists = True
        print(f"    Using existing Google Sheet: {file_url}")

        if not file_id:
            return False, "Error creating table", None, 0
        
        print(f"📖 Reading Excel data: {excel_path.name}")
        df = pd.read_excel(excel_path)
        if df.empty:
            return False, "Excel file is empty", None, 0
        
        print(f"   ✅ {len(df)} rows × {len(df.columns)} columns read")
        #  adding exhibition name 
        if exhibition_name:
            print(f"\n📝 Adding Exhibition to Google Sheets: {exhibition_name}")
            if 'Exhibition' not in df.columns:
                df.insert(0, 'Exhibition', exhibition_name)
        
        # adding qc metadata 
        if qc_metadata:
            print(f"\n👤 Adding QC Metadata to Google Sheets...")
            
            qc_columns_order = ['QC_Supervisor', 'QC_Role', 'QC_Date', 'QC_Time', 'QC_Timestamp']
            
            # calculate start position (after exhibition if exists)
            start_pos = 1 if 'Exhibition' in df.columns else 0
            
            for idx, col in enumerate(qc_columns_order, start=start_pos):
                if col in qc_metadata and col not in df.columns:
                    # convert to string to prevent conversion to number
                    value = str(qc_metadata[col])
                    
                    # add apostrophe for date and time (like phone number)
                    if col in ['QC_Date', 'QC_Time', 'QC_Timestamp']:
                        value = f"'{value}"
                    
                    df.insert(idx, col, value)
                    print(f"   ✅ {col}: {qc_metadata[col]}")
        
        # adding source 
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
            
            # calculate source column position (after qc metadata)
            qc_count = sum(1 for col in ['QC_Supervisor', 'QC_Role', 'QC_Date', 'QC_Time', 'QC_Timestamp'] if col in df.columns)
            source_pos = (1 if 'Exhibition' in df.columns else 0) + qc_count
            
            df.insert(source_pos, 'Source', df['file_name'].apply(detect_source))
            
            source_counts = df['Source'].value_counts()
            print(f"    Source Distribution:")
            for source, count in source_counts.items():
                print(f"      • {source}: {count} rows")
        





        #  convert date and time to text format 
        print(f"\n🕐 Converting date/time columns to text format...")
        
        date_time_columns = ['QC_Date', 'QC_Time', 'QC_Timestamp']
        
        for col in date_time_columns:
            if col in df.columns:
                # convert to string with apostrophe at start (for google sheets)
                df[col] = df[col].apply(
                    lambda x: f"'{str(x)}" if x and str(x).strip() not in ['', 'nan', 'None'] else ""
                )
                print(f"  {col} converted to text format")
        
      

        print(f"\n   📊 Final DataFrame: {len(df)} rows × {len(df.columns)} columns")

        
        # add: if doesn't have companyid, add it
        if 'CompanyID' not in df.columns:
            print(f"   ⚠️ CompanyID not found, generating...")
            df = add_company_id_to_dataframe(df, log_details=False)
        else:
            print(f"   ✅ CompanyID column exists")
        
        # make sure companyid is first column
        if 'CompanyID' in df.columns:
            cols = ['CompanyID'] + [col for col in df.columns if col != 'CompanyID']
            df = df[cols]
            print(f"   CompanyID is now the first column")
        
        

        # clean dataframe from nan and none values
        import numpy as np

        # replace empty values
        df = df.replace({np.nan: "", None: "", 'nan': "", 'None': "", 'NaT': ""})


        
        # removing unnecessary columns 
        print(f"\n🧹 Removing unnecessary columns...")

        columns_to_remove = []

        # 1. remove data_source and source_type
        for col in ['data_source', 'source_type', 'Data_Source', 'Source_Type']:
            if col in df.columns:
                columns_to_remove.append(col)
                print(f"   ❌ Removing: {col}")

        # 2. remove logo
        if 'Logo' in df.columns:
            columns_to_remove.append('Logo')
            print(f"   ❌ Removing: Logo")

        # remove columns
        if columns_to_remove:
            df.drop(columns=columns_to_remove, inplace=True)
            print(f"   ✅ Removed {len(columns_to_remove)} columns")

        #  extracting person/position 
        print(f"\n👤 Extracting Person & Position from PersonX columns...")

        import google.generativeai as genai
        genai.configure(api_key="AIzaSy***70")
        model = genai.GenerativeModel('gemini-1.5-flash')

        def translate_to_persian(text):
            """translate english to persian"""
            if not text or pd.isna(text) or str(text).strip() == '':
                return ""
            
            text = str(text).strip()
            
            # check if it's persian or not
            persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
            has_persian = any(c in persian_chars for c in text)
            
            if has_persian:
                return text  # already persian
            
            try:
                prompt = f"Translate this English text to Persian. Only return the translation:\n\n{text}"
                response = model.generate_content(prompt)
                translation = response.text.strip().replace('*', '').replace('`', '').strip('"').strip("'")
                return translation
            except:
                return text

        def extract_person_position(person_col_value):
        
            #extract name and position from personx column
            if not person_col_value or pd.isna(person_col_value) or str(person_col_value).strip() == '':
                return "", ""
            
            text = str(person_col_value).strip()
            
            # try to separate with different separators
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
            
            # if not separated, consider entire text as name
            if not name:
                name = text
            
            # translate to persian
            name_fa = translate_to_persian(name)
            position_fa = translate_to_persian(position)
            
            return name_fa, position_fa

        # find personx columns
        person_columns = [col for col in df.columns if col.lower().startswith('person')]

        if person_columns:
            print(f"   📋 Found {len(person_columns)} Person columns: {person_columns}")
            
            # list of names and positions
            names_list = []
            positions_list = []
            
            # process each row
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
                
                # combine with " | "
                names_list.append(" | ".join(row_names) if row_names else "")
                positions_list.append(" | ".join(row_positions) if row_positions else "")
            
            # add new columns
            if 'Name' not in df.columns:
                df['Name'] = names_list
                print(f"   ✅ Added 'Name' column")
            
            if 'Position' not in df.columns:
                df['Position'] = positions_list
                print(f"   ✅ Added 'Position' column")
            
            # remove old personx columns
            df.drop(columns=person_columns, inplace=True)
            print(f"   ✅ Removed {len(person_columns)} PersonX columns")
            
            # show 3 samples
            print(f"\n   📊 Sample extractions:")
            for i in range(min(3, len(df))):
                if df.at[i, 'Name'] or df.at[i, 'Position']:
                    print(f"      Row {i+1}:")
                    print(f"         Name: {df.at[i, 'Name'][:50]}")
                    print(f"         Position: {df.at[i, 'Position'][:50]}")

        else:
            print(f"   ⚠️ No Person columns found")

        print(f"\n   ✅ Cleanup completed!")


        #  translate english positions to persian 
        print(f"\n🌐 Translating English Positions to Persian...")

        if 'Position' in df.columns:
            import google.generativeai as genai
            import time
            
            genai.configure(api_key="AIzaS***zB70")
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            def detect_language_position(text):
                #detect language: fa or en
                if not text or pd.isna(text) or str(text).strip() == '':
                    return None
                
                text = str(text).strip()
                persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
                has_persian = any(c in persian_chars for c in text)
                
                return 'fa' if has_persian else 'en'
            
            def translate_position_to_persian(text):
                #translate english to persian
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
                
                # detect language
                lang = detect_language_position(position_value)
                
                if lang == 'en':
                    # translate english → persian
                    position_fa = translate_position_to_persian(position_value)
                    
                    if position_fa:
                        # combine: english | persian
                        df.at[idx, 'Position'] = f"{position_value} | {position_fa}"
                        translated_count += 1
                        
                        if translated_count <= 3:  # show 3 samples
                            print(f"      Row {idx+1}: {position_value} → {position_fa}")
                    
                    time.sleep(1)  # rate limiting
            
            if translated_count > 0:
                print(f"   ✅ Translated {translated_count} English positions")
            else:
                print(f"   ℹ️ No English positions found")

        # remove positionfa and positionen 
        print(f"\n🗑️ Removing PositionFA and PositionEN columns...")
        for col in ['PositionFA', 'PositionEN']:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
                print(f"   ❌ Removed: {col}")

    
        # consolidating address columns
        print(f"\n📍 Consolidating Address columns...")
        
        # find all address columns
        address_columns = []
        for col in df.columns:
            col_lower = col.lower()
            if 'address' in col_lower:
                address_columns.append(col)
                print(f"   Found: {col}")
        
        if address_columns:
            print(f"   📋 Found {len(address_columns)} Address columns: {address_columns}")
            
            # language detection function
            def detect_language_address(text):
                """detect address language: fa or en"""
                if not text or pd.isna(text) or str(text).strip() == '':
                    return None
                
                text = str(text).strip()
                
                # check persian characters
                persian_chars = set('آابپتثجچحخدذرزژسشصضطظعغفقکگلمنوهی')
                has_persian = any(c in persian_chars for c in text)
                
                if has_persian:
                    return 'fa'
                else:
                    return 'en'
            
            # english to persian translation function
            def translate_address_to_persian(text):
                """translate english address to persian"""
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
            
            # new lists for unified addresses
            unified_address_en = []
            unified_address_fa = []
            
            # process each row
            for idx in df.index:
                # collect all addresses from different columns
                all_addresses = []
                
                for col in address_columns:
                    if col in df.columns:
                        addr = df.at[idx, col]
                        if addr and not pd.isna(addr) and str(addr).strip() not in ['', 'nan', 'None']:
                            all_addresses.append(str(addr).strip())
                
                # if no address found
                if not all_addresses:
                    unified_address_en.append("")
                    unified_address_fa.append("")
                    continue
                
                # remove duplicates
                unique_addresses = list(dict.fromkeys(all_addresses))
                
                # separate persian and english addresses
                fa_addresses = []
                en_addresses = []
                
                for addr in unique_addresses:
                    lang = detect_language_address(addr)
                    
                    if lang == 'fa':
                        fa_addresses.append(addr)
                    elif lang == 'en':
                        en_addresses.append(addr)
                
                # combine english addresses
                final_en = " | ".join(en_addresses) if en_addresses else ""
                
                # combine persian addresses
                final_fa = " | ".join(fa_addresses) if fa_addresses else ""
                
                # if we have english address but no persian → translate it
                if final_en and not final_fa:
                    print(f"   Row {idx+1}: Translating EN→FA...")
                    final_fa = translate_address_to_persian(final_en)
                    time.sleep(1)  # rate limiting
                
                unified_address_en.append(final_en)
                unified_address_fa.append(final_fa)
            
            # remove old columns
            for col in address_columns:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)
            
            print(f"   ✅ Removed {len(address_columns)} old Address columns")
            
            # add new columns
            df['AddressEN'] = unified_address_en
            df['AddressFA'] = unified_address_fa
            
            print(f"   ✅ Added unified 'AddressEN' and 'AddressFA' columns")
            
            # show 3 samples
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
        
        # end of address consolidation

        #  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ cleaning formulas and errors
        def remove_formulas_from_df(df):
            """remove formulas, errors and convert to simple values"""
            for col in df.columns:
                if df[col].dtype == 'object':
                    # remove excel formulas (that start with =)
                    df[col] = df[col].apply(
                        lambda x: str(x)[1:] if isinstance(x, str) and x.startswith('=') else x
                    )
                    
                    # remove #error!, #ref!, #value!, #n/a, etc.
                    df[col] = df[col].apply(
                        lambda x: "" if isinstance(x, str) and x.startswith('#') else x
                    )
                    
                    # convert persian digits to english
                    persian_digits = '۰۱۲۳۴۵۶۷۸۹'
                    english_digits = '0123456789'
                    trans_table = str.maketrans(persian_digits, english_digits)
                    df[col] = df[col].apply(
                        lambda x: str(x).translate(trans_table) if isinstance(x, str) else x
                    )
            
            return df
        
        df = remove_formulas_from_df(df)
        print(f"   🧹 Cleaned formulas and errors from {len(df.columns)} columns")
        
        # convert phone numbers to string
        phone_columns = ['phones', 'phones2', 'phones3', 'phones4', 'phones5',
                        'Phone1', 'Phone2', 'Phone3', 'Phone4', 'Phone5',
                        'Fax', 'Fax2', 'WhatsApp', 'Telegram']
        
        for col in phone_columns:
            if col in df.columns:
                # convert number to string with apostrophe at start (for google sheets)
                df[col] = df[col].apply(
                    lambda x: f"'{str(x)}" if x and str(x).strip() not in ['', 'nan', 'None'] else ""
                )
        
        print(f"   📞 Converted phone columns to text format")

        
        
        #convert fax to string
        print(f"\n📠 Converting FAX columns to text format...")

        fax_columns = []
        for col in df.columns:
            col_lower = col.lower()
            if 'fax' in col_lower:
                fax_columns.append(col)

        for col in fax_columns:
            # convert to string with apostrophe to prevent error in google sheets
            df[col] = df[col].apply(
                lambda x: f"'{str(x)}" if x and str(x).strip() not in ['', 'nan', 'None'] else ""
            )
            print(f"   ✅ {col} converted to text format")

        print(f"   📠 Converted {len(fax_columns)} FAX columns")

       

        #  removing duplicate data in each row
        print(f"\n🧹 Removing duplicate values within each row (3+ occurrences)...")

        total_removed = 0
        rows_affected = 0

        for idx in df.index:
            row = df.loc[idx]
            
            # count values in this row 
            values = []
            for col in df.columns:
                val = row[col]
                # only valid values
                if val and str(val).strip() not in ['', 'nan', 'None', 'null', 'NULL']:
                    values.append((col, str(val).strip()))
            
            if not values:
                continue
            
            # count repetition of each value in this row
            value_counts = {}
            for col, val in values:
                if val not in value_counts:
                    value_counts[val] = []
                value_counts[val].append(col)
            
            # find values that repeat 3+ times
            row_modified = False
            for val, columns in value_counts.items():
                if len(columns) >= 3:
                    # keep first occurrence, remove rest
                    for col in columns[1:]:
                        df.at[idx, col] = ''
                        total_removed += 1
                        row_modified = True
                    
                    if not row_modified:
                        rows_affected += 1
                    
                    # show first 5 samples
                    if rows_affected <= 5:
                        print(f"   Row {idx+1}: '{val[:30]}' appeared {len(columns)} times in columns {columns[:3]} → kept first, removed {len(columns)-1}")

        if total_removed > 0:
            print(f"\n   ✅ Removed {total_removed} duplicate values across {rows_affected} rows")
        else:
            print(f"   ℹ️ No duplicate values found (3+ times in same row)")


        
        # clean text columns
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
                # remove case-insensitive values
                df[col] = df[col].apply(lambda x: "" if str(x).lower() in ['nan', 'none', 'nat', 'null'] else x)
        
        sheet_name = 'Sheet1'
        
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=f'{sheet_name}!1:1'
        ).execute()
        
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
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=file_id,
                    range=f'{sheet_name}!1:1',
                    valueInputOption='USER_ENTERED',
                    body={'values': [all_columns]}
                ).execute()
                
                result = sheets_service.spreadsheets().values().get(
                    spreadsheetId=file_id, range=f'{sheet_name}!A:A'
                ).execute()
                existing_rows_count = len(result.get('values', [])) - 1
                
                if existing_rows_count > 0:
                    print(f"   📝 Filling {existing_rows_count} old rows...")
                    empty_values = [[''] * len(new_columns) for _ in range(existing_rows_count)]
                    start_col_index = len(existing_headers)
                    start_col_letter = _col_index_to_letter(start_col_index)
                    end_col_letter = _col_index_to_letter(start_col_index + len(new_columns) - 1)
                    
                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=file_id,
                        range=f'{sheet_name}!{start_col_letter}2:{end_col_letter}{existing_rows_count+1}',
                        valueInputOption='USER_ENTERED',
                        body={'values': empty_values}
                    ).execute()
                    print(f"   ✅ Old rows updated")
            
            for col in all_columns:
                if col not in df.columns:
                    df[col] = ''
            
            df = df[all_columns]
            print(f"   ✅ DataFrame sorted: {len(df)} rows × {len(all_columns)} columns")
            values = df.values.tolist()

        # ✅ convert all nan or none to string before sending to sheets
        def clean_cell(cell):
            """complete cell cleaning"""
            if pd.isna(cell) or cell is None:
                return ""
            cell_str = str(cell).strip()
    
            # check unwanted values
            if cell_str.lower() in ['nan', 'none', 'nat', '<na>', 'null']:
                return ""
            
            # remove excel errors
            if cell_str.startswith('#'):
                return ""
    
            return cell_str

        values = [[clean_cell(cell) for cell in row] for row in values]
        
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=f'{sheet_name}!A:A'
        ).execute()
        existing_rows = len(result.get('values', []))
        
        print(f"   📊 Current rows: {existing_rows}")
        print(f"   📤 Adding {len(values)} rows...")
        
        from googleapiclient.errors import HttpError
        body = {'values': values}

        try:
            result = sheets_service.spreadsheets().values().append(
                spreadsheetId=file_id,
                range=f'{sheet_name}!A:A',
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
    
            updated_rows = result.get('updates', {}).get('updatedRows', 0)
            total_rows = existing_rows + updated_rows

        except HttpError as e:
            error_code = e.resp.status
            error_msg = str(e)
    
            if error_code == 429:
                # rate limit
                print(f"   ❌ Rate Limit: google api called too many times")
                return False, "Rate limit reached. please wait a few minutes", None, 0
    
            elif error_code == 403:
                # permission denied
                print(f"   ❌ Permission Error: no access to sheet")
                return False, "no access to google sheet. check service account", None, 0
    
            elif "exceeds grid limits" in error_msg or "GRID_LIMITS" in error_msg:
                # sheet full (10m cells)
                print(f"   ❌ Sheet Full: 10 million cell capacity filled")
                return False, "sheet is full (10m cells limit). create a new sheet", None, 0
    
            elif "Quota exceeded" in error_msg:
                # daily quota
                print(f"   ❌ Quota Exceeded: google daily quota finished")
                return False, "google daily quota finished. try again tomorrow", None, 0
    
            else:
                # unknown error
                print(f"   ❌ Google API Error: {error_msg}")
                return False, f"google error: {error_msg}", None, 0

        except Exception as e:
            print(f"   ❌ Unexpected Error: {e}")
            return False, str(e), None, 0
        

        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=f'{sheet_name}!1:1'
        ).execute()
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
    #find/create folder in drive
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
            print(f"   ✅ existing folder: {files[0]['name']}")
            return files[0]['id']
        
        folder = drive_service.files().create(
            body={'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'},
            fields='id'
        ).execute()
        print(f"   ✅ new folder: {folder_name}")
        return folder.get('id')
        
    except Exception as e:
        print(f"   ❌ error: {e}")
        return None






# quota management
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


# quality control tracking functions

def get_qc_metadata(user_name, user_role):
    """create quality control metadata"""
    now = datetime.datetime.now()
    return {
        "QC_Supervisor": user_name,
        "QC_Role": user_role,
        "QC_Date": now.strftime("%Y-%m-%d"),
        "QC_Time": now.strftime("%H:%M:%S"),
        "QC_Timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
    }

def add_qc_metadata_to_excel(excel_path, qc_metadata):
    """add quality control metadata to excel"""
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
    """save quality control log in json file"""
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


# smart shared functions
def detect_source_type(file_name):
    """detect file type: image, pdf, excel"""
    if not file_name or pd.isna(file_name):
        return "Unknown"
    
    file_name = str(file_name).lower()
    
    # images
    if file_name.endswith(('.jpg', '.jpeg', '.png', '.bmp', '.webp', '.gif', '.tiff', '.tif', '.svg', '.heic')):
        return "Image"
    
    # pdf
    elif file_name.endswith('.pdf'):
        return "PDF"
    
    # excel
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



# extract country & city from address
def extract_country_city_from_address(address_fa, address_en):
    """
    extract country and city from persian and english address
    
    returns:
        tuple: (country, city)
    """
    
    # list of main iranian cities (persian + english)
    IRANIAN_CITIES = {
        # major cities
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
        # english names
        'tehran': 'Tehran', 'mashhad': 'Mashhad', 'isfahan': 'Isfahan',
        'shiraz': 'Shiraz', 'tabriz': 'Tabriz', 'karaj': 'Karaj',
        'qom': 'Qom', 'ahvaz': 'Ahvaz', 'kermanshah': 'Kermanshah',
    }
    
    # list of countries (persian + english)
    COUNTRIES = {
        # persian
        'ایران': 'Iran', 'آلمان': 'Germany', 'چین': 'China', 
        'ترکیه': 'Turkey', 'امارات': 'UAE', 'آمریکا': 'USA',
        'انگلستان': 'UK', 'فرانسه': 'France', 'ایتالیا': 'Italy',
        'کره': 'South Korea', 'ژاپن': 'Japan', 'هند': 'India',
        'عراق': 'Iraq', 'افغانستان': 'Afghanistan', 'پاکستان': 'Pakistan',
        # english
        'iran': 'Iran', 'germany': 'Germany', 'china': 'China',
        'turkey': 'Turkey', 'uae': 'UAE', 'usa': 'USA',
        'uk': 'UK', 'france': 'France', 'italy': 'Italy',
        'korea': 'South Korea', 'japan': 'Japan', 'india': 'India',
    }
    
    country = None
    city = None
    
    # combine addresses
    combined_address = ""
    if address_fa and not pd.isna(address_fa):
        combined_address += str(address_fa).lower() + " "
    if address_en and not pd.isna(address_en):
        combined_address += str(address_en).lower() + " "
    
    if not combined_address.strip():
        return None, None
    
    # search for city
    for city_name, city_en in IRANIAN_CITIES.items():
        if city_name.lower() in combined_address:
            city = city_en
            country = "Iran"  # if iranian city found, country is iran
            break
    
    # search for country (if not found yet)
    if not country:
        for country_name, country_en in COUNTRIES.items():
            if country_name.lower() in combined_address:
                country = country_en
                break
    
    # if country not found but city was iranian
    if city and not country:
        country = "Iran"
    
    # if only country found (without city) and it was iran
    if country == "Iran" and not city:
        # try to find city with regex
        import re
        
        # common iranian address patterns
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
    
    # add country and city columns to excel
    
    try:
        print(f"\n🌍 Adding Country & City columns...")
        df = pd.read_excel(excel_path)
        
        # check existence of address columns
        has_address_fa = 'AddressFA' in df.columns
        has_address_en = 'AddressEN' in df.columns
        
        if not has_address_fa and not has_address_en:
            print("   ⚠️ No AddressFA or AddressEN columns found")
            return False
        
        # add columns if not exist
        if 'Country' not in df.columns:
            df['Country'] = None
        if 'City' not in df.columns:
            df['City'] = None
        
        # process each row
        filled_count = 0
        for idx in df.index:
            address_fa = df.at[idx, 'AddressFA'] if has_address_fa else None
            address_en = df.at[idx, 'AddressEN'] if has_address_en else None
            
            # only if country/city were empty
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
        
        # save
        df.to_excel(excel_path, index=False, engine='openpyxl')
        print(f"   ✅ Updated {filled_count} rows with Country/City")
        
        # show statistics
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
    add complete metadata to excel:
    - exhibition name
    - source (image/pdf/excel)
    - qc supervisor
    - qc role
    - qc date
    - qc time
    - qc timestamp
    - smart position detection
    """
    try:
        print(f"\n📝 Adding Exhibition, Source & QC Metadata...")
        df = pd.read_excel(excel_path)
        print(f"   ✅ Loaded: {len(df)} rows × {len(df.columns)} columns")

        # add exhibition
        df.insert(0, 'Exhibition', exhibition_name)
        print(f"   📋 Exhibition: '{exhibition_name}'")
        
        # add qc metadata 
        if qc_metadata:
            # add qc columns at the beginning of dataframe
            qc_columns = ['QC_Supervisor', 'QC_Role', 'QC_Date', 'QC_Time', 'QC_Timestamp']
            
            for idx, col in enumerate(qc_columns, start=1):
                if col in qc_metadata:
                    df.insert(idx, col, qc_metadata[col])
            
            print(f"   👤 QC Supervisor: {qc_metadata.get('QC_Supervisor', 'N/A')}")
            print(f"   💼 QC Role: {qc_metadata.get('QC_Role', 'N/A')}")
            print(f"   📅 QC Date: {qc_metadata.get('QC_Date', 'N/A')}")
            print(f"   🕐 QC Time: {qc_metadata.get('QC_Time', 'N/A')}")
        
        # detect source 
        #  read uploaded file types
        file_types_path = Path(session_dir) / "uploaded_file_types.json"
        
        if file_types_path.exists():
            file_types = json.loads(file_types_path.read_text(encoding='utf-8'))
            print(f"   📖 Loaded file types: {file_types}")
            
            # detect source based on file_name
            if 'file_name' in df.columns:
                def get_source(fname):
                    if pd.isna(fname):
                        return "Unknown"
                    fname_str = str(fname)
                    
                    # search in file_types
                    if fname_str in file_types:
                        return file_types[fname_str]
                    
                    # if not found, use detect_source_type
                    return detect_source_type(fname_str)
                
                # add source after qc columns
                insert_position = 6 if qc_metadata else 1
                df.insert(insert_position, 'Source', df['file_name'].apply(get_source))
                print(f"   ✅ Source detected from uploaded file types")
            
            else:
                # if file_name didn't exist, use file count
                if len(file_types) == 1:
                    # only one file → give same source to all
                    source = list(file_types.values())[0]
                    insert_position = 6 if qc_metadata else 1
                    df.insert(insert_position, 'Source', source)
                    print(f"   ✅ Source set to: {source} (single file)")
                
                elif len(file_types) > 1:
                    # multiple files → based on order
                    sources = list(file_types.values())
                    
                    # if row count equals file count
                    if len(df) == len(sources):
                        insert_position = 6 if qc_metadata else 1
                        df.insert(insert_position, 'Source', sources)
                        print(f"   ✅ Source matched by row count")
                    else:
                        # fill with first source
                        insert_position = 6 if qc_metadata else 1
                        df.insert(insert_position, 'Source', sources[0])
                        print(f"   ⚠️ Multiple files but row count mismatch → using first source")
                
                else:
                    insert_position = 6 if qc_metadata else 1
                    df.insert(insert_position, 'Source', 'Unknown')
                    print(f"   ⚠️ No file types found")
        
        else:
            # fallback: use file_name
            print(f"   ⚠️ file_types.json not found, using fallback")
            
            if 'file_name' in df.columns:
                insert_position = 6 if qc_metadata else 1
                df.insert(insert_position, 'Source', df['file_name'].apply(detect_source_type))
                print(f"   ✅ Source detected from file_name column")
            else:
                insert_position = 6 if qc_metadata else 1
                df.insert(insert_position, 'Source', 'Unknown')
                print(f"   ⚠️ No file_name column → Source set to Unknown")

        #  smart position detection 
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

        # remove extra columns 
        columns_to_remove = ['CompanyNameFA_translated']
        removed = 0
        for col in columns_to_remove:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)
                removed += 1
                print(f"   🗑️ Removed column: {col}")
        
        if removed:
            print(f"   ✅ Removed {removed} unnecessary columns")

        # clean data 
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace('nan', '').replace('None', '')
                except Exception as e:
                    print(f"   ⚠️ Warning: Could not convert column {col}: {e}")

        #  save
        df.to_excel(excel_path, index=False, engine='openpyxl')
        print(f"   💾 Updated: {excel_path.name}")
        print(f"   📊 Final: {len(df)} rows × {len(df.columns)} columns")
        
        #show source distribution 
        if 'Source' in df.columns:
            source_counts = df['Source'].value_counts()
            print(f"\n    Source Distribution:")
            for source, count in source_counts.items():
                print(f"      • {source}: {count} rows")
        
        # show metadata summary 
        print(f"\n    Metadata Summary:")
        print(f"      Exhibition: {exhibition_name}")
        if qc_metadata:
            print(f"       QC Supervisor: {qc_metadata.get('QC_Supervisor')}")
            print(f"       QC Role: {qc_metadata.get('QC_Role')}")
            print(f"       QC Timestamp: {qc_metadata.get('QC_Timestamp')}")
        
        return True
    
    except Exception as e:
        print(f"   ❌ Error adding metadata: {e}")
        import traceback
        traceback.print_exc()
        return False

# detect pipeline type and exhibition name
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


# batch processing logic
def get_batch_size(file_type):
    """determine batch size based on file type"""
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
    """divide files list into smaller batches"""
    batches = []
    for i in range(0, len(files_list), batch_size):
        batches.append(files_list[i:i + batch_size])
    return batches

def process_files_in_batches(uploads_dir, pipeline_type):
    """process files in batches"""
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


# run script with fast mode + log file
def run_script(script_name, session_dir, log_area, status_text, script_display_name="", fast_mode=True):
    script_path = Path(script_name)
    if not script_display_name:
        script_display_name = script_name
    if not script_path.exists():
        script_path = Path.cwd() / script_name
        if not script_path.exists():
            status_text.markdown(f"""
            <div class="status-box status-error">❌ file {script_name} not found!</div>
            """, unsafe_allow_html=True)
            return False

    status_text.markdown(f"""
    <div class="status-box status-info">
        <div class="loading-spinner"></div> running {script_display_name}...
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
            <div class="status-box status-success">✅ {script_display_name} completed successfully!</div>
            """, unsafe_allow_html=True)
            return True
        else:
            status_text.markdown(f"""
            <div class="status-box status-warning">⚠️ {script_display_name} encountered an issue (exit code: {process.returncode})</div>
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
        <div class="status-box status-error">❌ execution error: {str(e)}</div>
        """, unsafe_allow_html=True)
        return False


#  header
st.markdown("""
<div class="main-header">
    <h1>🎯 Smart Exhibition Pipeline</h1>
    <p>smart detection • automatic processing • unified output • batch processing • quality control • google sheets</p>
</div>
""", unsafe_allow_html=True)


