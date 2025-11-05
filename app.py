# -*- coding: utf-8 -*-
"""
🎯 Smart Exhibition Pipeline — Final Unified Edition + Google Sheets
ادغام کامل دو اپ: «Ultimate Smart Exhibition Pipeline» + «Smart Data Pipeline»
- UI خفن نسخه ۱ + منطق و لاگ‌نویسی و مدیریت سهمیه نسخه ۲
- Excel Mode و OCR/QR Mode با تشخیص خودکار
- Smart Metadata Injection (Exhibition + Source + Smart Position)
- Fast Mode, Debug Mode, Rate Limiting, Daily Quota
- ✨ Batch Processing: Images(5), PDFs(4), Excel(1)
- ✨ Quality Control Tracking: User Name, Role, Date, Time
- ☁️ Google Sheets Integration: ذخیره خودکار داده‌ها در Google Drive

اجرا:
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

# =========================================================
# ⚙️ تنظیمات صفحه
# =========================================================
st.set_page_config(
    page_title="Smart Exhibition Pipeline",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# 📌 Permanent Google Sheets Link (Always Visible)
# =========================================================
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



# =========================================================
# 🎨 UI خفن با گرادیانت‌های حرفه‌ای
# =========================================================
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

# =========================================================
# 🔐 کلیدهای API
# =========================================================
API_KEYS = {
    "excel": "AIzaSyBzVNw34fbQRcxCSZDouR35hoZNxqsW6pc",
    "ocr": "AIzaSyCKoaSP6Wgj5FCJDGGXIBHy1rt61Cl2ZTs",
    "scrap": "AIzaSyAhuC9Grg_FlxwDwYUW-_CpNaFzjwUg24w"
}
for key_name, key_value in API_KEYS.items():
    os.environ[f"GOOGLE_API_KEY_{key_name.upper()}"] = key_value
    os.environ["GOOGLE_API_KEY"] = key_value
    os.environ["GEMINI_API_KEY"] = key_value

# =========================================================
# ☁️ GOOGLE SHEETS INTEGRATION
# =========================================================
from google.oauth2 import service_account
from googleapiclient.discovery import build

GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets'
]

@st.cache_resource
def get_google_services():
    """اتصال به Google Drive و Sheets"""
    try:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=GOOGLE_SCOPES
        )
        drive_service = build('drive', 'v3', credentials=creds)
        sheets_service = build('sheets', 'v4', credentials=creds)
        return drive_service, sheets_service
    except Exception as e:
        st.error(f"❌ خطا در اتصال به Google: {e}")
        return None, None

def _col_index_to_letter(col_index):
    """تبدیل index به حرف Excel (0->A, 25->Z, 26->AA)"""
    result = ""
    while col_index >= 0:
        result = chr(col_index % 26 + 65) + result
        col_index = col_index // 26 - 1
    return result

def find_or_create_data_table(drive_service, sheets_service, folder_id=None):
    """پیدا کردن یا ساخت جدول در Drive"""
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
            print(f"   ✅ جدول موجود: {file_id}")
            return file_id, file_url, True
        
        print(f"   📝 ساخت جدول جدید...")
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
        
        print(f"   ✅ جدول جدید ساخته شد: {file_id}")
        return file_id, file_url, False
        
    except Exception as e:
        print(f"   ❌ خطا: {e}")
        return None, None, False

def append_excel_data_to_sheets(excel_path, folder_id=None):
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

        # file_id, file_url, exists = find_or_create_data_table(drive_service, sheets_service, folder_id)
        if not file_id:
            return False, "Error creating table", None, 0
        
        print(f"📖 Reading Excel data: {excel_path.name}")
        df = pd.read_excel(excel_path)
        if df.empty:
            return False, "Excel file is empty", None, 0
        
        print(f"   ✅ {len(df)} rows × {len(df.columns)} columns read")
        
        # ✅ Clean DataFrame from NaN and None values
        df = df.replace({np.nan: "", None: ""})
        
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace('nan', '').replace('None', '').replace('NaT', '')
        
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

        # ✅ Convert all NaN or None to string before sending to Sheets
        values = [[("" if (pd.isna(cell) or cell is None) else str(cell)) for cell in row] for row in values]
        
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=f'{sheet_name}!A:A'
        ).execute()
        existing_rows = len(result.get('values', []))
        
        print(f"   📊 Current rows: {existing_rows}")
        print(f"   📤 Adding {len(values)} rows...")
        
        body = {'values': values}
        result = sheets_service.spreadsheets().values().append(
            spreadsheetId=file_id,
            range=f'{sheet_name}!A:A',
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        updated_rows = result.get('updates', {}).get('updatedRows', 0)
        total_rows = existing_rows + updated_rows
        
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
    if not file_name or pd.isna(file_name):
        return "Unknown"
    file_name = str(file_name).lower()
    if file_name.endswith(('.jpg', '.jpeg', '.png', '.bmp', '.webp', '.gif')):
        return "Image"
    elif file_name.endswith('.pdf'):
        return "PDF"
    elif file_name.endswith(('.xlsx', '.xls', '.csv')):
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

def add_exhibition_and_source(excel_path, exhibition_name):
    """نسخه‌ی جامع + اعلان UI"""
    try:
        print(f"\n📝 Adding Exhibition & Source metadata...")
        df = pd.read_excel(excel_path)
        print(f"   ✓ Loaded: {len(df)} rows × {len(df.columns)} columns")

        df.insert(0, 'Exhibition', exhibition_name)
        if 'file_name' in df.columns:
            df.insert(1, 'Source', df['file_name'].apply(detect_source_type))
        elif 'url' in df.columns or 'Website' in df.columns:
            df.insert(1, 'Source', 'Excel')
        else:
            df.insert(1, 'Source', 'Unknown')

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
                        print(f"   ✓ Row {idx + 1}: {department} → {smart_position}")
            if filled_count > 0:
                st.info(f"🤖 پر شد {filled_count} سمت از روی دپارتمان")

        columns_to_remove = ['CompanyNameFA_translated']
        removed = 0
        for col in columns_to_remove:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)
                removed += 1
                print(f"   🗑️ Removed column: {col}")
        if removed:
            print(f"   ✅ Removed {removed} unnecessary columns")

        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace('nan', None).replace('', None)
                except Exception as e:
                    print(f"   ⚠️ Warning: Could not convert column {col}: {e}")

        df.to_excel(excel_path, index=False, engine='openpyxl')
        print(f"   ✅ Updated: {excel_path}")
        print(f"   📊 Final: {len(df)} rows × {len(df.columns)} columns")
        return True
    except Exception as e:
        print(f"   ❌ Error adding metadata: {e}")
        st.error(f"خطا در اضافه کردن متادیتا: {e}")
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
st.sidebar.info("📸 تصاویر: 5 تا\n📄 PDF: 4 تا\n📊 Excel: 1 تا")

# =========================================================
# 📂 آپلود فایل‌ها
# =========================================================
st.markdown("## 📂 آپلود فایل‌ها")
uploaded_files = st.file_uploader(
    "فایل‌های خود را بکشید یا کلیک کنید",
    type=['xlsx', 'xls', 'pdf', 'jpg', 'jpeg', 'png'],
    accept_multiple_files=True,
    help="Excel → Excel Mode | Image/PDF → OCR/QR Pipeline"
)

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

    for f in uploaded_files:
        (uploads_dir / f.name).write_bytes(f.getbuffer())

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
                    add_exhibition_and_source(output_file, exhibition_name)
                    add_qc_metadata_to_excel(output_file, qc_metadata)
                
                # ========== GOOGLE SHEETS UPLOAD ==========
                st.markdown("---")
                st.markdown("## ☁️ ذخیره داده‌ها در Google Drive")
                st.info("💡 فقط داده‌های داخل Excel ذخیره می‌شود، نه خود فایل!")
                
                sheets_status = st.empty()
                sheets_status.info("📤 در حال آپلود داده‌ها...")
                
                try:
                    folder_id = get_or_create_folder("Exhibition_Data")
                    
                    for output_file in output_files:
                        success_gs, msg_gs, url_gs, total_rows = append_excel_data_to_sheets(
                            excel_path=output_file,
                            folder_id=folder_id
                        )
                        
                        if success_gs:
                            sheets_status.markdown(f"""
                            <div class="status-box status-success">
                                {msg_gs}
                            </div>
                            """, unsafe_allow_html=True)
                            
                            st.session_state['sheet_url'] = url_gs
                            st.session_state['sheet_id'] = url_gs.split('/d/')[1].split('/')[0] if '/d/' in url_gs else ''
                            
                            link_file = Path("google_sheet_link.txt")
                            link_file.write_text(f"لینک جدول:\n{url_gs}", encoding='utf-8')
                            
                            total_cells = total_rows * 90
                            capacity = (total_cells / 10_000_000) * 100
                            
                            col_a, col_b, col_c = st.columns(3)
                            with col_a:
                                st.metric("📊 کل ردیف‌ها", f"{total_rows:,}")
                            with col_b:
                                st.metric("📦 کل سلول‌ها", f"{total_cells:,}")
                            with col_c:
                                st.metric("⚡️ ظرفیت", f"{capacity:.1f}%")
                            
                            st.markdown(f"""
                            <div class="file-display" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                                <h4>🔗 لینک دائمی جدول</h4>
                                <p style="background: rgba(255,255,255,0.2); padding: 1rem; border-radius: 8px; margin: 0.5rem 0;">
                                    <a href="{url_gs}" target="_blank" style="color: white; font-weight: bold; font-size: 1.1rem;">
                                        📊 باز کردن در Google Drive
                                    </a>
                                </p>
                                <p style="font-size: 0.9rem; margin: 0.5rem 0 0 0; opacity: 0.9;">
                                    💡 این لینک همیشه ثابت است! Bookmark کنید!
                                </p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            st.code(url_gs, language=None)
                            
                            if capacity > 80:
                                st.warning(f"⚠️ ظرفیت بالا ({capacity:.1f}%)!")
                            else:
                                st.success(f"✅ فضای کافی ({100-capacity:.1f}% باقی)")
                        else:
                            sheets_status.error(f"❌ خطا: {msg_gs}")
                
                except Exception as e:
                    sheets_status.error(f"❌ خطا: {e}")
                    st.warning("💡 مطمئن شوید Google Drive API و Sheets API فعال است")
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

                st.markdown("## 📥 دانلود فایل‌های نهایی")
                for output_file in output_files:
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