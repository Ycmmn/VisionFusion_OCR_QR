# -*- coding: utf-8 -*-
"""
Complete JSON + Excel Merger - Always Merge Both Sources
Smart merging of JSON and Excel - both sources are always merged
"""

from pathlib import Path
import os, json, re, pandas as pd
from collections import defaultdict
import time

# =========================================================
# Dynamic paths
# =========================================================
SESSION_DIR = Path(os.getenv("SESSION_DIR", Path.cwd()))
INPUT_JSON = Path(os.getenv("INPUT_JSON", SESSION_DIR / "mix_ocr_qr.json"))
INPUT_EXCEL = Path(os.getenv("INPUT_EXCEL", SESSION_DIR / "web_analysis.xlsx"))
timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_EXCEL = Path(os.getenv("OUTPUT_EXCEL", SESSION_DIR / f"merged_final_{timestamp}.xlsx"))

print("\n" + "="*70)
print("JSON + Excel Merger - Flexible Mode")
print("="*70)
print(f"Session: {SESSION_DIR}")
print(f"JSON: {INPUT_JSON}")
print(f"Excel: {INPUT_EXCEL}")
print(f"Output: {OUTPUT_EXCEL}")
print("="*70 + "\n")

# =========================================================
# Mapping of JSON fields to Excel
# =========================================================
FIELD_MAPPING = {
    'addresses': 'Address',
    'phones': 'Phone1',
    'faxes': 'Fax',
    'emails': 'Email',
    'urls': 'Website',
    'telegram': 'Telegram',
    'instagram': 'Instagram',
    'linkedin': 'LinkedIn',
    'company_names': 'CompanyName',
    'services': 'Services',
    'persons': 'ContactName',
    'notes': 'Notes',
}

# =========================================================
# Helper functions
# =========================================================
def is_persian(text):
    if not text or pd.isna(text):
        return False
    return bool(re.search(r"[\u0600-\u06FF]", str(text)))

def merge_url_columns(df):
    """Merge url, urls, Website into a single Website column"""
    if df.empty:
        return df
    
    url_fields = ['urls', 'url', 'Website']
    
    def get_first_url(row):
        for field in url_fields:
            if field in row and row[field] and not pd.isna(row[field]) and str(row[field]).strip():
                return str(row[field]).strip()
        return ""
    
    if any(col in df.columns for col in url_fields):
        df['Website'] = df.apply(get_first_url, axis=1)
        
        # remove duplicate columns
        for col in ['url', 'urls']:
            if col in df.columns:
                df = df.drop(columns=[col])
    
    return df

def normalize_value(val):
    if val is None or pd.isna(val):
        return ""
    return str(val).strip().lower()

def normalize_website(url):
    if not url or pd.isna(url):
        return ""
    u = str(url).strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    u = u.split("/")[0].split("?")[0]
    return u.rstrip(".")

def normalize_phone(phone):
    if not phone or pd.isna(phone):
        return ""
    return re.sub(r"[^\d+]", "", str(phone))

def normalize_company_name(name):
    if not name or pd.isna(name):
        return ""
    n = str(name).strip().lower()
    stopwords = ["شرکت", "company", "co.", "co", "ltd", "inc", "corp",
                 "سهامی", "خاص", "عام", "private", "public", "holding",
                 "international", "بین المللی", "گروه", "group"]
    for word in stopwords:
        n = n.replace(word, " ")
    n = re.sub(r"[^\w\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

def are_values_same(val1, val2):
    return normalize_value(val1) == normalize_value(val2)

def extract_key_identifier(record):
    """Extract a unique identifier to compare records"""
    website = normalize_website(record.get("Website") or record.get("url", ""))
    if website:
        return ("website", website)
    
    phone_fields = ["Phone1", "Phone2", "Phone3", "Phone4"]
    for pf in phone_fields:
        phone = normalize_phone(record.get(pf, ""))
        if phone and len(phone) >= 8:
            return ("phone", phone)
    
    email = normalize_value(record.get("Email", ""))
    if email and "@" in email:
        return ("email", email)
    
    for name_field in ["CompanyNameEN", "CompanyNameFA"]:
        name = normalize_company_name(record.get(name_field, ""))
        if name and len(name) > 3:
            return ("company", name)
    
    return ("unique", str(id(record)))

# =========================================================
# Convert JSON to DataFrame
# =========================================================
def json_to_dataframe_smart(json_path):
    """Convert JSON to DataFrame - all fields"""
    print("\nConverting JSON to DataFrame...")
    
    if not json_path.exists():
        print(f"    JSON not found: {json_path}")
        return pd.DataFrame()
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        records = []
        
        if isinstance(raw_data, list):
            for file_item in raw_data:
                if not isinstance(file_item, dict):
                    continue
                
                result_data = file_item.get("result")
                
                # structure 1: result is directly a dict
                if isinstance(result_data, dict):
                    page_results = [result_data]
                # structure 2: result is an array of pages
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
                    
                    # process all fields
                    for key, value in page_result.items():
                        if key in ['ocr_text', 'qr_links']:
                            continue
                        
                        if isinstance(value, list):
                            if not value:
                                continue
                            
                            if value[0] is not None:
                                col_name = FIELD_MAPPING.get(key, key)
                                record[col_name] = str(value[0])
                            
                            for idx, item in enumerate(value[1:], 2):
                                if item is not None:
                                    col_name = FIELD_MAPPING.get(key, key)
                                    record[f"{col_name}{idx}"] = str(item)
                        
                        elif isinstance(value, dict):
                            for sub_key, sub_val in value.items():
                                if sub_val:
                                    record[f"{key}_{sub_key}"] = str(sub_val)
                        
                        elif value is not None and str(value).strip():
                            col_name = FIELD_MAPPING.get(key, key)
                            record[col_name] = str(value)
                    
                    # process company_names
                    if 'company_names' in page_result and isinstance(page_result['company_names'], list):
                        names = page_result['company_names']
                        fa_names = [str(n) for n in names if n and is_persian(n)]
                        en_names = [str(n) for n in names if n and not is_persian(n)]
                        
                        if fa_names:
                            record['CompanyNameFA'] = fa_names[0]
                        if en_names:
                            record['CompanyNameEN'] = en_names[0]
                        
                        record.pop('CompanyName', None)
                    
                    # process addresses
                    if 'addresses' in page_result and isinstance(page_result['addresses'], list):
                        addresses = page_result['addresses']
                        fa_addrs = [str(a) for a in addresses if a and is_persian(a)]
                        en_addrs = [str(a) for a in addresses if a and not is_persian(a)]
                        
                        if fa_addrs:
                            record['AddressFA'] = fa_addrs[0]
                        if en_addrs:
                            record['AddressEN'] = en_addrs[0]
                        
                        record.pop('Address', None)
                    
                    # process phones
                    if 'phones' in page_result and isinstance(page_result['phones'], list):
                        for idx, phone in enumerate(page_result['phones'], 1):
                            if phone:
                                record[f'Phone{idx}'] = str(phone)
                    
                    # process persons
                    if 'persons' in page_result and page_result['persons']:
                        if isinstance(page_result['persons'], list):
                            for idx, person in enumerate(page_result['persons'], 1):
                                if isinstance(person, dict):
                                    name = person.get('name', '')
                                    position = person.get('position', '')
                                    
                                    if name:
                                        record[f'ContactName{idx if idx > 1 else ""}'] = name
                                    
                                    if position:
                                        if is_persian(position):
                                            record[f'PositionFA{idx if idx > 1 else ""}'] = position
                                        else:
                                            record[f'PositionEN{idx if idx > 1 else ""}'] = position
                    
                    if record:
                        records.append(record)
        
        if not records:
            print("    No records in JSON")
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        df = merge_url_columns(df)
        
        print(f"    JSON: {len(df)} rows x {len(df.columns)} columns")
        return df
        
    except Exception as e:
        print(f"    Error: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

# =========================================================
# Load Excel
# =========================================================
def load_excel_dataframe(excel_path):
    """Load Excel as a DataFrame"""
    print("\nLoading Excel...")
    
    if not excel_path.exists():
        print(f"    Not found: {excel_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_excel(excel_path)
        print(f"    Size: {df.shape[0]} rows x {df.shape[1]} columns")
        
        if df.empty:
            print(f"    Excel is empty")
            return pd.DataFrame()
        
        # check success
        if 'status' in df.columns:
            success = df[df['status'] == 'SUCCESS']
            print(f"    Success: {len(success)}, Failed: {len(df) - len(success)}")
            
            if len(success) == 0:
                print(f"    No successful scraping")
                return pd.DataFrame()
            
            # keep only the successful ones
            df = success.copy()
        
        # cleanup
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.dropna(how='all', axis=0)
        df = df.dropna(how='all', axis=1)
        df = df.drop_duplicates()
        df.columns = [str(col).strip() for col in df.columns]
        
        # remove extra columns
        drop_cols = ['status', 'error']
        for col in drop_cols:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        print(f"    Excel: {len(df)} clean records")
        return df
        
    except Exception as e:
        print(f"    Error: {e}")
        return pd.DataFrame()

# =========================================================
# Merge two records
# =========================================================
def merge_two_records(r1, r2):
    """Merge two records - r1 takes priority"""
    merged = {}
    for key in set(r1.keys()) | set(r2.keys()):
        v1, v2 = r1.get(key), r2.get(key)
        
        if not v1 and not v2:
            continue
        if not v1:
            merged[key] = v2
            continue
        if not v2:
            merged[key] = v1
            continue
        
        if are_values_same(v1, v2):
            merged[key] = v1
        else:
            # if values differ, keep both
            merged[key] = v1
            counter = 2
            while f"{key}[{counter}]" in merged:
                counter += 1
            merged[f"{key}[{counter}]"] = v2
    
    return merged

# =========================================================
# Smart merge of DataFrames
# =========================================================
def smart_merge_dataframes(json_df, excel_df):
    """Smart merge of two DataFrames"""
    print("\nSmart merging JSON + Excel...")
    
    # if one is empty
    if json_df.empty and excel_df.empty:
        print("    Both empty!")
        return pd.DataFrame()
    
    if json_df.empty:
        print("    Using Excel only")
        return excel_df
    
    if excel_df.empty:
        print("    Using JSON only")
        return json_df
    
    # convert to records
    json_records = json_df.to_dict('records')
    excel_records = excel_df.to_dict('records')
    
    # group by identifier
    groups = defaultdict(list)
    
    for rec in json_records:
        rec['_source'] = 'JSON'
        kt, kv = extract_key_identifier(rec)
        groups[f"{kt}:{kv}"].append(rec)
    
    for rec in excel_records:
        rec['_source'] = 'Excel'
        kt, kv = extract_key_identifier(rec)
        groups[f"{kt}:{kv}"].append(rec)
    
    # statistics
    json_only = sum(1 for g in groups.values() if len(g)==1 and g[0]['_source']=='JSON')
    excel_only = sum(1 for g in groups.values() if len(g)==1 and g[0]['_source']=='Excel')
    merged = sum(1 for g in groups.values() if len(g)>1)
    
    print(f"    Groups: {len(groups)}")
    print(f"       JSON only: {json_only}")
    print(f"       Excel only: {excel_only}")
    print(f"       Merged: {merged}")
    
    # merge
    final_records = []
    for gk, grecs in groups.items():
        if len(grecs) == 1:
            rec = grecs[0].copy()
            rec.pop('_source', None)
            final_records.append(rec)
        else:
            # merge multiple - Excel takes priority
            excel_recs = [r for r in grecs if r.get('_source') == 'Excel']
            json_recs = [r for r in grecs if r.get('_source') == 'JSON']
            
            # if we have Excel, start from it
            if excel_recs:
                base = excel_recs[0].copy()
                base.pop('_source', None)
                
                # merge the JSON ones in
                for jr in json_recs:
                    jc = jr.copy()
                    jc.pop('_source', None)
                    base = merge_two_records(base, jc)
                
                final_records.append(base)
            else:
                # JSON only
                base = json_recs[0].copy()
                base.pop('_source', None)
                final_records.append(base)
    
    result_df = pd.DataFrame(final_records)
    result_df = merge_url_columns(result_df)
    
    print(f"    Final: {len(result_df)} rows x {len(result_df.columns)} columns")
    
    return result_df

# =========================================================
# Clean up extra columns
# =========================================================
def remove_junk_columns(df):
    """Remove extra/junk columns"""
    print("\nRemoving junk columns...")
    
    if df.empty:
        return df
    
    # columns that should be removed
    junk_patterns = [
        r'^Phone\d{2,}$',
        r'^Services\d+$',
        r'^CompanyName\d+$',
        r'^Email\d+$',
        r'^Address\d+$',
        r'^ContactName\d+$',
        r'.*\[2\]$',
        r'.*\[3\]$',
        r'.*\[4\]$',
        r'^Notes$',
        r'^_source$',
        r'^Website\d+$',
    ]
    
    cols_to_drop = []
    for col in df.columns:
        for pattern in junk_patterns:
            if re.match(pattern, str(col)):
                cols_to_drop.append(col)
                break
    
    # remove duplicates
    cols_to_drop = list(set(cols_to_drop))
    
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop, errors='ignore')
        print(f"   Removed {len(cols_to_drop)} junk columns: {', '.join(cols_to_drop[:5])}...")
    else:
        print(f"   No junk columns found")
    
    return df

# =========================================================
# Save
# =========================================================
def save_excel(df, path):
    """Save DataFrame to Excel with cleanup"""
    if df.empty:
        print("\nNo data to save!")
        return False
    
    try:
        print("\nSaving Excel...")
        
        # extra cleanup
        def clean_dataframe_before_excel(df):
            """Remove formulas and errors"""
            import numpy as np
            
            for col in df.columns:
                if df[col].dtype == 'object':
                    # 1. remove Excel formulas
                    df[col] = df[col].apply(
                        lambda x: str(x)[1:] if isinstance(x, str) and str(x).startswith('=') else x
                    )
                    
                    # 2. remove errors
                    df[col] = df[col].apply(
                        lambda x: "" if isinstance(x, str) and str(x).startswith('#') else x
                    )
                    
                    # 3. convert Persian digits
                    persian_digits = '۰۱۲۳۴۵۶۷۸۹'
                    english_digits = '0123456789'
                    trans_table = str.maketrans(persian_digits, english_digits)
                    df[col] = df[col].apply(
                        lambda x: str(x).translate(trans_table) if isinstance(x, str) else x
                    )
            
            return df
        
        df = clean_dataframe_before_excel(df)
        print(f"   Cleaned {len(df.columns)} columns")
        
        df = df.fillna("")
        df.to_excel(path, index=False, engine='openpyxl')
        print(f"    Saved: {path}")
        print(f"    {len(df)} rows x {len(df.columns)} columns")
        return True
        
    except Exception as e:
        print(f"    Error: {e}")
        return False

# =========================================================
# Main - flexible mode
# =========================================================
def main():
    start = time.time()
    
    # check whether the files exist
    json_exists = INPUT_JSON.exists()
    excel_exists = INPUT_EXCEL.exists()
    
    print(f"JSON exists: {json_exists}")
    print(f"Excel exists: {excel_exists}\n")
    
    # mode 1: neither exists
    if not json_exists and not excel_exists:
        print("ERROR: Neither JSON nor Excel found!")
        return 1
    
    # mode 2: only JSON exists
    if json_exists and not excel_exists:
        print("Excel not found - Converting JSON to Excel...")
        
        # convert JSON to DataFrame
        json_df = json_to_dataframe_smart(INPUT_JSON)
        
        if json_df.empty:
            print("ERROR: JSON is empty!")
            return 1
        
        print(f"JSON loaded: {len(json_df)} rows x {len(json_df.columns)} columns")
        
        # clean up extra columns
        final_df = remove_junk_columns(json_df)
        
        # save directly
        if save_excel(final_df, OUTPUT_EXCEL):
            print(f"\n{'='*70}")
            print("SUCCESS - JSON ONLY MODE")
            print(f"{'='*70}")
            print(f"Input: {len(json_df)} rows from JSON")
            print(f"Output: {len(final_df)} rows x {len(final_df.columns)} columns")
            print(f"Saved: {OUTPUT_EXCEL}")
            print(f"Time: {time.time()-start:.2f}s")
            print(f"{'='*70}")
            return 0
        else:
            print("ERROR: Failed to save Excel!")
            return 1
    
    # mode 3: only Excel exists
    if not json_exists and excel_exists:
        print("JSON not found - Using Excel only...")
        
        excel_df = load_excel_dataframe(INPUT_EXCEL)
        
        if excel_df.empty:
            print("ERROR: Excel is empty!")
            return 1
        
        print(f"Excel loaded: {len(excel_df)} rows x {len(excel_df.columns)} columns")
        
        final_df = remove_junk_columns(excel_df)
        
        if save_excel(final_df, OUTPUT_EXCEL):
            print(f"\n{'='*70}")
            print("SUCCESS - EXCEL ONLY MODE")
            print(f"{'='*70}")
            print(f"Input: {len(excel_df)} rows from Excel")
            print(f"Output: {len(final_df)} rows x {len(final_df.columns)} columns")
            print(f"Saved: {OUTPUT_EXCEL}")
            print(f"Time: {time.time()-start:.2f}s")
            print(f"{'='*70}")
            return 0
        else:
            print("ERROR: Failed to save Excel!")
            return 1
    
    # mode 4: both exist (merge)
    print("Both JSON and Excel found - Merging...")
    
    json_df = json_to_dataframe_smart(INPUT_JSON)
    excel_df = load_excel_dataframe(INPUT_EXCEL)
    
    if json_df.empty and excel_df.empty:
        print("ERROR: Both sources are empty!")
        return 1
    
    # smart merge
    final_df = smart_merge_dataframes(json_df, excel_df)
    
    # cleanup
    final_df = remove_junk_columns(final_df)
    
    # save
    if not final_df.empty:
        if save_excel(final_df, OUTPUT_EXCEL):
            print(f"\n{'='*70}")
            print("SUCCESS - MERGED MODE")
            print(f"{'='*70}")
            print(f"Input: {len(json_df)} JSON + {len(excel_df)} Excel")
            print(f"Output: {len(final_df)} merged records")
            print(f"Time: {time.time()-start:.2f}s")
            print(f"{'='*70}")
            return 0
        else:
            print("ERROR: Failed to save Excel!")
            return 1
    else:
        print("ERROR: No data to save!")
        return 1

if __name__ == "__main__":
    exit(main())