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


