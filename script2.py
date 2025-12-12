import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime


# section 1: helper functions
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


# section 2: cleaning functions

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
    #remove json remnants from text
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


# section 3: merge numbered columns
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
    #merge numbered columns
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


# section 4: merge duplicate columns
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


# section 5: merge bilingual columns
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
    #merge english/persian columns
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


# section 6: merge specific columns
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


# section 7: final cleaning
def clean_data(df, verbose=True):
    #final data cleaning
    
    if verbose:
        print("cleaning companyid...")
    
    if 'CompanyID' in df.columns:
        df['CompanyID'] = df['CompanyID'].apply(clean_company_id)
        if verbose:
            print("companyid cleaned")
    
    if verbose:
        print("general cleanup...")
    
    skip_columns = ['Name', 'Position', 'ContactName', 'name', 'position']
    
    cleaned_count = 0
    for col in df.columns:
        if col in skip_columns or col == 'CompanyID':
            continue
            
        for idx in df.index:
            val = df.at[idx, col]
            if pd.notna(val) and isinstance(val, str):
                if ('{' in val or "'name':" in val or "'position':" in val) and col not in ['file_name']:
                    cleaned = remove_json_artifacts(val)
                    if cleaned != val and cleaned:
                        df.at[idx, col] = cleaned
                        cleaned_count += 1
    
    if verbose and cleaned_count > 0:
        print(f"{cleaned_count} cells cleaned")
    
    return df


# section 8: final cleaning and standardization
def standardize_url(url):
    """standardize url to https://www.domain.com format"""
    if pd.isna(url) or str(url).strip() == '':
        return None
    
    url = str(url).strip()
    url = url.replace(' ', '')
    
    if not url.startswith('http://') and not url.startswith('https://'):
        url = 'https://' + url
    
    if url.startswith('http://'):
        url = url.replace('http://', 'https://', 1)
    
    if url.startswith('https://') and not url.startswith('https://www.'):
        domain_part = url.replace('https://', '')
        if domain_part.count('.') <= 1:
            url = 'https://www.' + domain_part
    
    return url

def find_duplicate_urls(urls_list):
    """find duplicate urls that differ only in format"""
    from urllib.parse import urlparse
    
    url_map = {}
    
    for url in urls_list:
        if not url:
            continue
        
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            domain_without_www = domain.replace('www.', '')
            
            if domain_without_www in url_map:
                existing = url_map[domain_without_www]
                if url.startswith('https://www.') and not existing.startswith('https://www.'):
                    url_map[domain_without_www] = url
            else:
                url_map[domain_without_www] = url
        except:
            url_map[url] = url
    
    return list(url_map.values())

def remove_duplicates_from_cell(value, is_url=False):
    #remove duplicate values from a cell
    if pd.isna(value) or str(value).strip() == '':
        return None
    
    items = [item.strip() for item in str(value).split('|')]
    
    if is_url:
        standardized = []
        for item in items:
            std_url = standardize_url(item)
            if std_url:
                standardized.append(std_url)
        
        unique_urls = find_duplicate_urls(standardized)
        
        if not unique_urls:
            return None
        return ' | '.join(unique_urls)
    
    else:
        normalized = []
        seen = set()
        
        for item in items:
            item_compare = item.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
            
            if item_compare and item_compare not in seen:
                seen.add(item_compare)
                normalized.append(item)
        
        if not normalized:
            return None
        
        return ' | '.join(normalized)

def clean_urls_and_phones(df, verbose=True):
    #clean and remove duplicates from urls and phones columns
    
    if verbose:
        print("cleaning urls and phones...")
    
    cleaned_count = 0
    
    if 'urls' in df.columns:
        if verbose:
            print("standardizing urls...")
        
        for idx in df.index:
            val = df.at[idx, 'urls']
            if pd.notna(val) and str(val).strip() != '':
                cleaned = remove_duplicates_from_cell(val, is_url=True)
                if cleaned != val:
                    df.at[idx, 'urls'] = cleaned
                    cleaned_count += 1
        
        if verbose:
            print(f"{cleaned_count} urls cleaned")
    
    phone_count = 0
    if 'phones' in df.columns:
        if verbose:
            print("removing duplicates from phones...")
        
        for idx in df.index:
            val = df.at[idx, 'phones']
            if pd.notna(val) and str(val).strip() != '':
                cleaned = remove_duplicates_from_cell(val, is_url=False)
                if cleaned and cleaned != val:
                    df.at[idx, 'phones'] = cleaned
                    phone_count += 1
        
        if verbose:
            print(f"{phone_count} phone numbers cleaned")
    
    if verbose:
        total = cleaned_count + phone_count
        if total > 0:
            print(f"total {total} cells cleaned")
        else:
            print("no duplicates found")
    
    return df

def process_company_data(
    input_file,
    output_file=None,
    keep_empty_columns=True,
    company_id_col=None,
    verbose=True
):
    #complete processing of company data
    
    if verbose:
        print("="*60)
        print("starting complete company data processing")
        print("="*60)
    
    if verbose:
        print(f"reading: {input_file}")
    
    if str(input_file).endswith('.csv'):
        df = pd.read_csv(input_file, encoding='utf-8-sig')
    else:
        df = pd.read_excel(input_file)
    
    initial_rows = len(df)
    initial_cols = len(df.columns)
    
    if verbose:
        print(f"{initial_rows} rows, {initial_cols} columns")
    
    if verbose:
        print("\n" + "="*60)
        print("step 1: merge numbered columns")
        print("="*60)
    
    df, numbered_merged = merge_numbered_columns(df, verbose=verbose)
    
    if verbose:
        print("\n" + "="*60)
        print("step 2: merge duplicate columns")
        print("="*60)
    
    df, duplicate_merged = merge_duplicate_columns(df, verbose=verbose)
    
    if verbose:
        print("\n" + "="*60)
        print("step 3: merge bilingual columns")
        print("="*60)
    
    df, bilingual_merged = merge_bilingual_columns(df, verbose=verbose)
    
    if verbose:
        print("\n" + "="*60)
        print("step 3.5: merge specific columns")
        print("="*60)
    
    df, specific_merged = merge_specific_columns(df, verbose=verbose)
    
    if verbose:
        print("\n" + "="*60)
        print("step 4: merge rows")
        print("="*60)
    
    df = merge_rows_by_company_id(df, company_id_col, verbose=verbose)
    
    rows_after_merge = len(df)
    
    if verbose:
        print("\n" + "="*60)
        print("step 5: final cleaning")
        print("="*60)
    
    df = clean_data(df, verbose=verbose)
    
    if verbose:
        print("\n" + "="*60)
        print("step 5.5: standardize urls and phones")
        print("="*60)
    
    df = clean_urls_and_phones(df, verbose=verbose)
    
    if verbose:
        print("\n" + "="*60)
        print("step 6: manage empty columns")
        print("="*60)
    
    empty_cols_removed = 0
    
    if not keep_empty_columns:
        empty_before = len(df.columns)
        df = df.dropna(axis=1, how='all')
        empty_cols_removed = empty_before - len(df.columns)
        if verbose and empty_cols_removed > 0:
            print(f"{empty_cols_removed} empty columns removed")
    else:
        if verbose:
            print("all columns preserved")
    
    if output_file is None:
        input_path = Path(input_file)
        base_name = input_path.stem
        extension = input_path.suffix
        output_file = input_path.parent / f"{base_name}_processed{extension}"
        
        counter = 1
        while output_file.exists():
            try:
                with open(output_file, 'a'):
                    pass
                break
            except PermissionError:
                output_file = input_path.parent / f"{base_name}_processed_{counter}{extension}"
                counter += 1
    
    if verbose:
        print(f"saving: {output_file}")
    
    try:
        if str(output_file).endswith('.csv'):
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
        else:
            df.to_excel(output_file, index=False, engine='openpyxl')
        
        if verbose:
            print("file saved")
    
    except PermissionError:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_path = Path(input_file)
        output_file = input_path.parent / f"{input_path.stem}_processed_{timestamp}{input_path.suffix}"
        
        if verbose:
            print(f"file is open, saving with name: {output_file}")
        
        if str(output_file).endswith('.csv'):
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
        else:
            df.to_excel(output_file, index=False, engine='openpyxl')
    
    if verbose:
        print("\n" + "="*60)
        print("final summary")
        print("="*60)
        print(f"input rows: {initial_rows}")
        print(f"output rows: {rows_after_merge}")
        print(f"input columns: {initial_cols}")
        print(f"output columns: {len(df.columns)}")
        
        total_merged = numbered_merged + duplicate_merged + bilingual_merged + specific_merged
        if total_merged > 0:
            print(f"\nmerged columns:")
            if numbered_merged > 0:
                print(f"   - numbered: {numbered_merged}")
            if duplicate_merged > 0:
                print(f"   - duplicate: {duplicate_merged}")
            if bilingual_merged > 0:
                print(f"   - bilingual: {bilingual_merged}")
            if specific_merged > 0:
                print(f"   - specific: {specific_merged}")
            print(f"   - total: {total_merged}")
        
        print("="*60)
        print(f"file: {output_file}")
        print("="*60)
    
    return df


# execution
if __name__ == "__main__":
    
    input_file = "Exhibition_QC_Data - Sheet1.csv"
    
    try:
        df_result = process_company_data(
            input_file=input_file,
            keep_empty_columns=True,
            verbose=True
        )
        
        print("\nprocessing completed successfully!")
        
    except FileNotFoundError:
        print(f"error: file '{input_file}' not found!")
    except Exception as e:
        print(f"error: {str(e)}")
        import traceback
        traceback.print_exc()


# wrapper for use in pipeline
def script2_process_file(input_path, output_path):
    """
    simple wrapper function for calling from pipeline
    
    args:
        input_path: input file path
        output_path: output file path
    
    returns:
        processed dataframe
    """
    return process_company_data(
        input_file=input_path,
        output_file=output_path,
        keep_empty_columns=True,
        company_id_col=None,
        verbose=False
    )

    