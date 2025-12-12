import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime

# ============================================================================
# section 1: helper functions
# ============================================================================

def merge_cell_values(values):
    """merge cell values with duplicate removal"""
    clean_values = [str(v).strip() for v in values if pd.notna(v) and str(v).strip() != '']
    
    if not clean_values:
        return ''
    
    seen = set()
    unique_values = []
    for val in clean_values:
        if val not in seen:
            seen.add(val)
            unique_values.append(val)
    
    if len(unique_values) == 1:
        return unique_values[0]
    
    return ' | '.join(unique_values)

def extract_base_name(col_name):
    """extract base name from column (without number)"""
    patterns = [
        r'^(.+?)\d+$',
        r'^(.+?)_\d+$',
        r'^(.+?)s\d+$',
    ]
    
    for pattern in patterns:
        match = re.match(pattern, col_name)
        if match:
            return match.group(1)
    
    return col_name

# ============================================================================
# section 2: cleaning functions
# ============================================================================

def clean_company_id(company_id_str):
    """extract main companyid from merged string"""
    if pd.isna(company_id_str):
        return company_id_str
    
    pattern = r'COMP_UNKNOWN_[A-F0-9]+'
    match = re.search(pattern, str(company_id_str))
    
    if match:
        return match.group(0)
    
    parts = str(company_id_str).split('|')
    return parts[0].strip()

def extract_json_fields(text):
    """extract name and position from json fields"""
    if pd.isna(text) or text == '':
        return None, None
    
    text = str(text)
    
    name_pattern = r"'name':\s*'([^']+)'"
    position_pattern = r"'position':\s*'([^']+)'"
    
    names = re.findall(name_pattern, text)
    positions = re.findall(position_pattern, text)
    
    name = ' | '.join(set(n for n in names if n and n != 'None')) if names else None
    position = ' | '.join(set(p for p in positions if p and p != 'None')) if positions else None
    
    return name, position

def remove_json_artifacts(text):
    """remove json remnants from text"""
    if pd.isna(text) or text == '':
        return text
    
    text = str(text)
    
    patterns = [
        r"\{'name':[^}]+\}",
        r"'name':\s*'[^']*'",
        r"'position':\s*'[^']*'",
        r"\|\s*'position':[^|]*",
        r"\s*\|\s*\|+\s*",
    ]
    
    for pattern in patterns:
        text = re.sub(pattern, '', text)
    
    text = text.strip()
    text = re.sub(r'^\|+\s*|\s*\|+$', '', text)
    text = re.sub(r'\s*\|\s*\|\s*', ' | ', text)
    
    return text.strip() if text.strip() else None

# ============================================================================
# section 3: merge numbered columns
# ============================================================================

def find_numbered_groups(columns):
    """find groups of columns with numbers"""
    from collections import defaultdict
    
    groups = defaultdict(list)
    
    for col in columns:
        base_name = extract_base_name(col.lower())
        groups[base_name].append(col)
    
    result = {k: v for k, v in groups.items() if len(v) > 1}
    return result

def merge_numbered_columns(df, verbose=True):
    """merge numbered columns"""
    if verbose:
        print("identifying numbered columns...")
    
    groups = find_numbered_groups(df.columns.tolist())
    
    if not groups:
        if verbose:
            print("no numbered groups found")
        return df, 0
    
    if verbose:
        print(f"{len(groups)} numbered groups found")
    
    merged_count = 0
    
    for base_name, cols in groups.items():
        cols_sorted = sorted(cols, key=lambda x: (len(x), x))
        main_col = cols_sorted[0]
        other_cols = cols_sorted[1:]
        
        for idx in df.index:
            values = []
            for col in cols_sorted:
                val = df.at[idx, col]
                if pd.notna(val) and str(val).strip() != '':
                    values.append(str(val).strip())
            
            unique_values = list(dict.fromkeys(values))
            
            if unique_values:
                if len(unique_values) == 1:
                    df.at[idx, main_col] = unique_values[0]
                else:
                    df.at[idx, main_col] = ' | '.join(unique_values)
        
        df.drop(columns=other_cols, inplace=True)
        merged_count += len(other_cols)
    
    if verbose:
        print(f"{merged_count} numbered columns removed")
    
    return df, merged_count

# ============================================================================
# section 4: merge duplicate columns
# ============================================================================

def merge_duplicate_columns(df, verbose=True):
    """merge duplicate columns (case-insensitive)"""
    if verbose:
        print("checking duplicate columns...")
    
    column_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower not in column_map:
            column_map[col_lower] = []
        column_map[col_lower].append(col)
    
    duplicated_groups = {k: v for k, v in column_map.items() if len(v) > 1}
    
    if not duplicated_groups:
        if verbose:
            print("no duplicate columns found")
        return df, 0
    
    if verbose:
        print(f"{len(duplicated_groups)} duplicate column groups found")
    
    merged_count = 0
    new_df = pd.DataFrame()
    processed_cols = set()
    
    for col in df.columns:
        if col in processed_cols:
            continue
        
        col_lower = col.lower()
        
        if col_lower not in duplicated_groups:
            new_df[col] = df[col]
            processed_cols.add(col)
        else:
            all_same_cols = column_map[col_lower]
            
            if all_same_cols[0] in processed_cols:
                continue
            
            merged_col = []
            for idx in df.index:
                values = [df[c].iloc[idx] for c in all_same_cols]
                merged_col.append(merge_cell_values(values))
            
            final_name = all_same_cols[0]
            new_df[final_name] = merged_col
            
            for c in all_same_cols:
                processed_cols.add(c)
            
            merged_count += (len(all_same_cols) - 1)
    
    if verbose:
        print(f"{merged_count} duplicate columns removed")
    
    return new_df, merged_count

# ============================================================================
# section 5: merge bilingual columns
# ============================================================================

def find_bilingual_pairs(columns):
    """find english/persian column pairs"""
    pairs = []
    processed = set()
    
    patterns = [
        ('EN', 'FA'),
        ('_en', '_fa'),
        ('English', 'Persian'),
        ('', 'FA'),
        ('', '_translated'),
    ]
    
    for col in columns:
        if col in processed:
            continue
        
        for en_suffix, fa_suffix in patterns:
            if en_suffix and col.endswith(en_suffix):
                base = col[:-len(en_suffix)]
                fa_col = base + fa_suffix
                
                if fa_col in columns and fa_col not in processed:
                    pairs.append((col, fa_col))
                    processed.add(col)
                    processed.add(fa_col)
                    break
            
            elif not en_suffix:
                fa_col = col + fa_suffix
                
                if fa_col in columns and fa_col not in processed:
                    pairs.append((col, fa_col))
                    processed.add(col)
                    processed.add(fa_col)
                    break
    
    return pairs

def merge_bilingual_columns(df, verbose=True):
    """merge english/persian columns"""
    if verbose:
        print("identifying bilingual columns...")
    
    pairs = find_bilingual_pairs(df.columns.tolist())
    
    if not pairs:
        if verbose:
            print("no bilingual pairs found")
        return df, 0
    
    if verbose:
        print(f"{len(pairs)} bilingual pairs found")
    
    merged_count = 0
    
    for en_col, fa_col in pairs:
        for idx in df.index:
            en_val = df.at[idx, en_col]
            fa_val = df.at[idx, fa_col]
            
            values = []
            if pd.notna(en_val) and str(en_val).strip() != '':
                values.append(str(en_val).strip())
            if pd.notna(fa_val) and str(fa_val).strip() != '':
                values.append(str(fa_val).strip())
            
            unique_values = list(dict.fromkeys(values))
            
            if unique_values:
                if len(unique_values) == 1:
                    df.at[idx, en_col] = unique_values[0]
                else:
                    df.at[idx, en_col] = ' | '.join(unique_values)
            else:
                df.at[idx, en_col] = None
        
        df.drop(columns=[fa_col], inplace=True)
        merged_count += 1
    
    if verbose:
        print(f"{merged_count} bilingual columns merged")
    
    return df, merged_count

# ============================================================================
# section 6: merge specific columns
# ============================================================================

def merge_specific_columns(df, verbose=True):
    """merge specific columns that should be combined"""
    
    if verbose:
        print("merging specific columns...")
    
    merged_count = 0
    
    merge_groups = [
        ('fax', ['faxes', 'Fax']),
        ('phones', ['phone1', 'Phone1', 'Phone', 'phone']),
        ('urls', ['url', 'Website', 'website', 'URL']),
        ('company_names', ['CompanyNameFA_translated', 'CompanyNameEN']),
        ('emails', ['Email', 'OtherEmails']),
    ]
    
    for main_col_pattern, other_patterns in merge_groups:
        main_col = None
        for col in df.columns:
            if col.lower() == main_col_pattern.lower():
                main_col = col
                break
        
        other_cols = []
        for pattern in other_patterns:
            for col in df.columns:
                if col.lower() == pattern.lower() and col != main_col:
                    other_cols.append(col)
                    break
        
        if main_col and other_cols:
            if verbose:
                print(f"merging '{main_col}' with {other_cols}")
            
            for idx in df.index:
                values = []
                
                main_val = df.at[idx, main_col]
                if pd.notna(main_val) and str(main_val).strip() != '':
                    values.append(str(main_val).strip())
                
                for col in other_cols:
                    val = df.at[idx, col]
                    if pd.notna(val) and str(val).strip() != '':
                        values.append(str(val).strip())
                
                unique_values = list(dict.fromkeys(values))
                
                if unique_values:
                    if len(unique_values) == 1:
                        df.at[idx, main_col] = unique_values[0]
                    else:
                        df.at[idx, main_col] = ' | '.join(unique_values)
            
            df.drop(columns=other_cols, inplace=True)
            merged_count += len(other_cols)
            
            if verbose:
                print(f"{len(other_cols)} columns merged")
        
        elif not main_col and other_cols:
            main_col = other_cols[0]
            remaining_cols = other_cols[1:]
            
            if remaining_cols:
                if verbose:
                    print(f"merging '{main_col}' with {remaining_cols}")
                
                for idx in df.index:
                    values = []
                    
                    for col in other_cols:
                        val = df.at[idx, col]
                        if pd.notna(val) and str(val).strip() != '':
                            values.append(str(val).strip())
                    
                    unique_values = list(dict.fromkeys(values))
                    
                    if unique_values:
                        if len(unique_values) == 1:
                            df.at[idx, main_col] = unique_values[0]
                        else:
                            df.at[idx, main_col] = ' | '.join(unique_values)
                
                df.drop(columns=remaining_cols, inplace=True)
                merged_count += len(remaining_cols)
                
                if verbose:
                    print(f"{len(remaining_cols)} columns merged")
    
    if verbose:
        if merged_count > 0:
            print(f"total {merged_count} specific columns merged")
        else:
            print("no specific columns found to merge")
    
    return df, merged_count

def merge_rows_by_company_id(df, company_id_col=None, verbose=True):
    """merge rows with same company_id"""
    if company_id_col is None:
        for col in df.columns:
            if 'company' in col.lower() and 'id' in col.lower():
                company_id_col = col
                break
    
    if company_id_col is None:
        company_id_col = df.columns[0]
        if verbose:
            print(f"using first column: {company_id_col}")
    else:
        if verbose:
            print(f"company_id column: {company_id_col}")
    
    grouped = df.groupby(company_id_col, dropna=False)
    
    if verbose:
        print(f"merging {len(grouped)} groups...")
    
    merged_rows = []
    for company_id, group in grouped:
        if len(group) == 1:
            merged_rows.append(group.iloc[0].to_dict())
        else:
            merged_row = {}
            for col in df.columns:
                if col == company_id_col:
                    merged_row[col] = company_id
                else:
                    values = group[col].tolist()
                    merged_row[col] = merge_cell_values(values)
            merged_rows.append(merged_row)
    
    df_merged = pd.DataFrame(merged_rows)
    
    return df_merged