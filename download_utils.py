# Databricks notebook source
!pip install tqdm

# COMMAND ----------

from pathlib import Path
import requests
from tqdm import tqdm
import zipfile

# COMMAND ----------

# NOTE: this configuration worked for downloading VA PBM data
def download_file_like_browser_using_session(url, path, filename, chunk_size=8192):
    # NOTE the stream=True parameter below
    session = requests.Session()
    if not Path(f"{path}/{filename}").exists():
        print(f"[INFO] {filename} downloading to {path}/{filename}")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        with session.get(url, headers=headers, verify=False, timeout=30) as r:
            r.raise_for_status()
            with open(f"{path}/{filename}", 'wb') as f:
                for chunk in tqdm(r.iter_content(chunk_size=chunk_size)): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)

# COMMAND ----------

def _download_like_browser(url, path, filename, chunk_size=8192):
    # NOTE the stream=True parameter below
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.google.com/',
        'DNT': '1',  # Do Not Track Request Header
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    with requests.get(f"{url}", headers=headers, timeout=10, stream=True) as r:
        r.raise_for_status()
        with open(f"{path}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

def download_file_like_browser(url, path, filename, chunk_size=8192):
    # NOTE the stream=True parameter below
    if not Path(f"{path}/{filename}").exists():
        print(f"[INFO] {filename} downloading to {path}/{filename}")
        _download_like_browser(url, path, filename, chunk_size)

def _download(url, path, filename, chunk_size=8192, verify_ssl=True):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True, verify=verify_ssl) as r:
        r.raise_for_status()
        with open(f"{path}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

def download_file(url, path, filename, chunk_size=8192, verify_ssl=True):
    # NOTE the stream=True parameter below
    if not Path(f"{path}/{filename}").exists():
        print(f"[INFO] {filename} downloading to {path}/{filename}")
        _download(url, path, filename, chunk_size, verify_ssl)

def download_files(urls, path, filenames = None, chunk_size=8192, verify_ssl=True):
    for i, url in enumerate(urls):
        filename = url.split("/")[-1]
        if filenames is not None:
            filename = filenames[i]
        # Check if the file exists
        if Path(f"{path}/{filename}").exists():
            #print(f"{filename} already exists, skipping...")
            continue
        else:
            print(f"[INFO] {filename} downloading to {path}/{filename}")
            _download(url, path, filename, chunk_size, verify_ssl)

# COMMAND ----------

def _unzip_base(path_zip, path_unzip, file_filter=None, name_mapper=None):
    """Base unzip function with filtering and naming capabilities"""
    try:
        if not zipfile.is_zipfile(path_zip):
            print(f"Not a valid ZIP file: {path_zip}")
            return False
            
        with zipfile.ZipFile(path_zip, "r") as zip_ref:
            extracted_count = 0
            
            for file_info in zip_ref.infolist():
                original_name = file_info.filename
                
                # Apply filter if provided
                if file_filter and not file_filter(original_name):
                    continue
                
                # Apply name mapping if provided
                if name_mapper:
                    new_name = name_mapper(original_name)
                    file_info.filename = new_name
                    target_path = Path(f"{path_unzip}/{new_name}")
                else:
                    target_path = Path(f"{path_unzip}/{original_name}")
                
                if target_path.exists():
                    continue
                
                try:
                    print(f"[INFO] Extracting {file_info.filename}...")
                    zip_ref.extract(file_info, path=path_unzip)
                    extracted_count += 1
                except Exception as e:
                    print(f"[ERROR] {target_path}; {e}")
            
            print(f"[INFO] Extracted {extracted_count} files from {path_zip}")
            return True
            
    except zipfile.BadZipFile:
        print(f"Corrupted ZIP file: {path_zip}")
        return False
    except Exception as e:
        print(f"Error processing {path_zip}: {e}")
        return False

def unzip(path_zip, path_unzip):
    """Extract all files from ZIP"""
    return _unzip_base(path_zip, path_unzip)

def unzip_with_name(path_zip, path_unzip, newname):
    """Extract files and rename them to a single name"""
    def name_mapper(original_name):
        return newname
    
    return _unzip_base(path_zip, path_unzip, name_mapper=name_mapper)

def unzip_matching_ext(path_zip, path_unzip, ext):
    """Extract only files with specific extension"""
    def file_filter(filename):
        return filename.endswith(ext)
    
    return _unzip_base(path_zip, path_unzip, file_filter=file_filter)
