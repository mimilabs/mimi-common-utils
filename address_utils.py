# Databricks notebook source
!pip install usaddress us h3

# COMMAND ----------

import re
import usaddress
import us
import sqlite3
import csv 
import requests
import h3

# COMMAND ----------

def remove_fractions(address_str):
    # This pattern matches common fraction formats like 1/2, 3/4, etc.
    return re.sub(r'\b\d+/\d+\b', '', address_str)

def remove_special_characters(address_str):
    return re.sub(r'[^a-zA-Z0-9\s]', '', address_str)

def remove_double_spaces(address_str):
    return re.sub(r'\s+', ' ', address_str)

def standardize_abbreviations(address_str):
    
    address_str = address_str.upper()

    # Standardize common abbreviations
    abbreviations = {
        'STREET': 'ST',
        'AVENUE': 'AVE',
        'BOULEVARD': 'BLVD',
        'DRIVE': 'DR',
        'LANE': 'LN',
        'ROAD': 'RD',
        'CIRCLE': 'CIR',
        'COURT': 'CT',
        'PLACE': 'PL',
        'SQUARE': 'SQ',
        'APARTMENT': 'APT',
        'SUITE': 'STE',
        'NORTH': 'N',
        'SOUTH': 'S',
        'EAST': 'E',
        'WEST': 'W'
    }

    for full, abbr in abbreviations.items():
        address_str = re.sub(r'\b' + full + r'\b', abbr, address_str)

    return address_str

def clean_address(address_str):

    address_str = remove_fractions(address_str)
    address_str = remove_special_characters(address_str)
    address_str = standardize_abbreviations(address_str)
    address_str = remove_double_spaces(address_str)

    return address_str.strip()

def create_address_key(street, city, state, zipcode):
    address_key = '|'.join([clean_address(str(street or '')),
                        str(city or '').strip(),
                        str(state or '').strip(),
                        str(zipcode or '')[:5].strip()])
    return address_key

# COMMAND ----------

def is_po_box_m1(address_str):
    # Convert address to uppercase for case-insensitive matching
    address_str = str(address_str or '')
    
    address_str = address_str.upper()
    
    # Patterns to match various PO Box formats
    po_box_patterns = [
        r'\bP\.?\s*O\.?\s*(BOX|B\.?)\s*\d+',
        r'\bPOST\s*OFFICE\s*(BOX|B\.?)\s*\d+',
        r'\bPOB\s*\d+',
        r'\bPMB\s*\d+', # Private Mail Box
        r'\bBOX\s\d+'
        ]
    
    # Check if any of the patterns match
    for pattern in po_box_patterns:
        if re.search(pattern, address_str):
            return True
    
    # Additional keywords that might indicate a PO Box
    po_box_keywords = ['MAILBOX', 'LOCKBOX']
    
    # Check for keywords
    if any(keyword in address_str for keyword in po_box_keywords):
        return True
    
    return False

# COMMAND ----------

def is_po_box_m2(address_str):
    address_str = str(address_str or '')
    # Parse the address
    try:
        parsed, address_type = usaddress.tag(address_str)
    except usaddress.RepeatedLabelError:
        # If parsing fails, return False (default answer)
        return False
    
    if (parsed.get('USPSBoxType', '') != '' or 
        parsed.get('USPSBoxID', '') != ''):
        return True
    else:
        return False

# COMMAND ----------

# the sqlite db is downloaded from https://pypi.org/project/uszipcode/#about-the-data
uszipcode_conn = sqlite3.connect("/Volumes/mimi_ws_1/default/libraries/uszipcode_simple_db.db")
uszipcode_cur = uszipcode_conn.cursor()
uszipcode_res = uszipcode_cur.execute("SELECT zipcode, zipcode_type, lat, lng FROM simple_zipcode")
uszipcode_data = {d[0]: d[1:] for d in uszipcode_res.fetchall()}

def is_po_box_m3(zipcode_str):
    zipcode_str = str(zipcode_str or '')[:5]
    uszipcode_query_res = uszipcode_data.get(zipcode_str, ('STANDARD', None, None))
    if uszipcode_query_res[0] == 'PO BOX':
        return True
    else:
        return False

# COMMAND ----------

def is_po_box(street, city, state, zipcode, threshold = 2):
    res_m1 = is_po_box_m1(street)
    res_m2 = is_po_box_m2(street)
    res_m3 = is_po_box_m3(zipcode)
    return (res_m1 + res_m2 + res_m3) >= threshold

# COMMAND ----------

def get_census_style_street(address_org):
    try:
        parsed, address_type = usaddress.tag(address_org)
    except usaddress.RepeatedLabelError:
        # If parsing fails, return the original address in uppercase
        return address_org
    
    street_number = parsed.get('AddressNumber', '')
    street_name = parsed.get('StreetName', '')
    street_type = parsed.get('StreetNamePostType', '')
    street_name = re.sub(r'\b(APT|UNIT|#).*', '', street_name.upper()).strip()
    street_name = re.sub(r'(\d+)(ST|ND|RD|TH)', r'\1', street_name)
    street_new = f"{street_number} {street_name} {street_type}"

    return street_new

# COMMAND ----------

def query_census_bulk_api(address_keys):
    idx2key = {}
    idx = 0
    inputfile = []
    url = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
    for address_key in address_keys:
        street, city, state, zipcode = address_key.split('|')
        address_org = f"{street}, {city}, {state} {zipcode}"
        street_v2 = get_census_style_street(address_org)
        idx += 1
        idx2key[str(idx)] = address_key
        inputfile.append(",".join([str(idx), street, city, state, zipcode]))
        if street != street_v2:
            idx += 1
            idx2key[str(idx)] = address_key
            inputfile.append(",".join([str(idx), street_v2, city, state, zipcode]))
    # https://geocoding.geo.census.gov/geocoder/benchmarks
    # benchmark ids: 4, 8, 2020
    res = requests.post(url, files={"addressFile": ("inputfile.csv", 
                                                    "\n".join(inputfile), "text/csv")}, 
                            data={"benchmark": "4", "vintage": "4"})
    reader = csv.reader(res.text.split("\n"))
    output = []
    for row in reader:
        if len(row) == 0:
            continue
        if row[0] not in idx2key:
            # API error/fail
            continue
        row[0] = idx2key[row[0]]
        if len(row) < 12:
            row += [None] * (12 - len(row)) 
        output.append(row)
    return output

# COMMAND ----------

def get_h3(lat, lng, res):
    if lat is None or lng is None:
        return ""
    else:
        return h3.geo_to_h3(lat, lng, res)

# COMMAND ----------

from pyspark.sql.functions import udf

@udf("boolean")
def is_po_box_udf(street, city, state, zipcode, threshold = 2):
    return is_po_box(street, city, state, zipcode, threshold)

@udf("string")
def create_address_key_udf(street, city, state, zipcode):
    return create_address_key(street, city, state, zipcode)

@udf("string")
def get_h3_udf(lat, lng, res):
    return get_h3(lat, lng, res)
