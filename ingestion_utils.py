# Databricks notebook source
!pip install openpyxl

# COMMAND ----------

import re
from datetime import datetime
from dateutil.parser import parse, ParserError
import pandas as pd
from pathlib import Path
import csv

def split_camel2(text):
    # Insert space before uppercase letters that follow lowercase letters or digits
    return re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', text)

def split_camel(text):
    pattern = re.compile(r'(?<!^)(?=[A-Z][a-z])')
    return pattern.sub(' ', text)

def reduce_spaces(text):
    return ' '.join(text.split())

def remove_special_chars(text):
    return re.sub(r'[^a-zA-Z0-9 _]', '', text)

def format_header_varname2(varname):
    # a new version
    varname0 = varname
    if varname[0] == "#":
        # if the first character is '#', if often means 'number'
        varname0 = 'num_' + varname
    varname1 = remove_special_chars(varname0)
    varname2 = varname1.replace('_', ' ')
    varname3 = split_camel(varname2)
    varname4 = reduce_spaces(varname3)
    varname5 = varname4.replace(' ', '_')
    varname6 = varname5.lower()
    
    return varname6

def format_header_varname(varname):
    varname0 = varname
    if varname[0] == "#":
        # if the first character is '#', if often means 'number'
        varname0 = 'num_' + varname
    varname1 = remove_special_chars(varname0)
    varname2 = varname1.replace('_', ' ')
    varname3 = split_camel(varname2)
    varname4 = reduce_spaces(varname3)
    varname5 = varname4.replace(' ', '_')
    varname6 = varname5.lower()
    
    return varname6

def change_header2(header_org):
    return [format_header_varname2(column)
            for column in header_org]
    
def change_header(header_org):
    return [format_header_varname(column)
            for column in header_org]

# COMMAND ----------

def standardize_entity(name):
    name = re.sub(r'\([^)]*\)', '', str(name))  # Remove parentheses content
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)  # Remove special characters
    name = re.sub(r'\b(Inc|LLC|Corp|Ltd|Co|Corporation|Incorporated|Limited|Company|LP|LLP)\b', '', name, flags=re.IGNORECASE)  # Remove suffixes
    return re.sub(r'\s+', ' ', name).strip() 

# COMMAND ----------

def compare_columns(df1, df2):
    dtypes1 = {col: dtype for col, dtype in df1.dtypes}
    dtypes2 = {col: dtype for col, dtype in df2.dtypes}
    print("df1 size: ", len(dtypes1))
    print("df2 size: ", len(dtypes2))
    for col in df1.columns:
        if col not in df2.columns:
            print(f"Column '{col}' is missing in df2.")
        elif dtypes1[col] != dtypes2[col]:
            print(f"Column '{col}' has different data types in df1 and df2.")
    for col in df2.columns:
        if col not in df1.columns:
            print(f"Column '{col}' is missing in df1.")
