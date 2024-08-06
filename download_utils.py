# Databricks notebook source
!pip install tqdm

# COMMAND ----------

from pathlib import Path
import requests
from tqdm import tqdm
import zipfile

# COMMAND ----------

def _download(url, path, filename, chunk_size=8192):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{path}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

def download_file(url, path, filename, chunk_size=8192):
    # NOTE the stream=True parameter below
    if not Path(f"{path}/{filename}").exists():
        print(f"[INFO] {filename} downloading to {path}/{filename}")
        _download(url, path, filename, chunk_size)

def download_files(urls, path, filenames = None, chunk_size=8192):
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
            _download(url, path, filename, chunk_size)

# COMMAND ----------

def unzip(path_zip, path_unzip):
    with zipfile.ZipFile(path_zip, "r") as zip_ref:
        for member in zip_ref.namelist():
            if Path(f"{path_unzip}/{member}").exists():
                continue
            try:
                print(f"[INFO] Extracting {member}...")
                zip_ref.extract(member, path=path_unzip)
            except Exception as e:
                print(f"[ERROR] {path_unzip}/{member}; {e}")

def unzip_with_name(path_zip, path_unzip, newname):
    with zipfile.ZipFile(path_zip, "r") as zip_ref:
        for file_info in zip_ref.infolist():
            file_info.filename = newname
            if Path(f"{path_unzip}/{newname}").exists():
                continue
            try:
                print(f"[INFO] Extracting {newname}...")
                zip_ref.extract(file_info, path=path_unzip)
            except Exception as e:
                print(f"[ERROR] {path_unzip}/{newname}; {e}")

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
