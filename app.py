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
