# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

from collections import defaultdict
from itertools import combinations
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, IntegerType
import re

# COMMAND ----------

dlt_path_metadata = 'mimi_ws_1.cdc.nhanes_metadata'
volume_path = f'/Volumes/mimi_ws_1/cdc/src/nhanes/'
volume_path2 = f'/Volumes/mimi_ws_1/cdc/src/nhanes_vynguyen/' #https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9934713/#SD2

# COMMAND ----------

sheet_names = ['Table S1-Mortality',
 'Table S3-Demograhics',
 'Table S5-Questionnaire',
 'Table S7-Dietary',
 'Table S9-Medications',
 'Table S11-Occupation',
 'Table S13-Chemicals & Comments',
 'Table S14-Weights',
 'Table S15-Response']
manual_harmonizations = defaultdict(dict)
with pd.ExcelFile(f'{volume_path2}media-2.xlsx') as file:
    for sheet_name in sheet_names:
        pdf = pd.read_excel(file, sheet_name=sheet_name)
        pdf = pdf.loc[:,['variable_codename', 
                         'variable_codename_use', 
                         'file_name']].drop_duplicates()
        pdf = pdf.loc[pdf['variable_codename_use']!=pdf['variable_codename'],:]
        for _, row in pdf.iterrows():
            src = row['file_name']
            var_old = row['variable_codename'].lower().strip()
            var_new = row['variable_codename_use'].lower().strip()
            if var_old == var_new:
                continue
            if var_old == 'mcq500' and src == 'MCQ_J':
                # this is a bug from the manual harmonization file
                continue
            manual_harmonizations[src][var_old] = var_new 

# COMMAND ----------

# to make the file description a bit simpler for the table name representation
file_desc_mapping = {
    "Blood Pressure - Oscillometric Measurement": "Blood Pressure",
    "Cholesterol - High - Density Lipoprotein (HDL)": "Cholesterol - HDL",
    "Cholesterol - High-Density Lipoprotein (HDL)": "Cholesterol - HDL",
    "Cholesterol - LDL, Triglyceride & Apoliprotein (ApoB)": "Cholesterol - LDL & Triglycerides",
    "Cholesterol - Low - Density Lipoprotein (LDL) & Triglycerides": "Cholesterol - LDL & Triglycerides",
    "Cholesterol - Low-Density Lipoproteins (LDL) & Triglycerides": "Cholesterol - LDL & Triglycerides",
    "Demographic Variables and Sample Weights": "Demographic Variables & Sample Weights",
    "Glyphosate (GLYP) - Urine (Surplus)": "Glyphosate (GLYP) - Urine",
    "High-Sensitivity C-Reactive Protein (hs-CRP)": "High-Sensitivity C-Reactive Protein",
    "Kidney Conditions - Urology": "Kidney Conditions",
    "Plasma Fasting Glucose & Insulin": "Plasma Fasting Glucose",
    "Plasma Fasting Glucose, Serum C-peptide & Insulin": "Plasma Fasting Glucose",
    "Standard Biochemistry Profile & Hormones": "Standard Biochemistry Profile",
    "Smoking - Cigarette/Tobacco Use - Adult": "Smoking - Cigarette Use",
}

# COMMAND ----------

metadata = spark.read.table(dlt_path_metadata).toPandas()
metadata['data_file_desc'] = (metadata['data_file_desc']
                                  .apply(lambda x: file_desc_mapping.get(x,x)))
metadata['var_name'] = (metadata['var_name'].apply(lambda x: x.lower()))

# COMMAND ----------

comp2abbr = {'Demographics': 'demo', 
                'Dietary': 'diet', 
                'Examination': 'exam', 
                'Laboratory': 'lab', 
                'Questionnaire': 'qre'}

# COMMAND ----------

def prep(target_desc, component):

    files = {}
    var2desc = {}
    
    for _, row in (metadata.loc[((metadata['data_file_desc']==target_desc) &
                                 (metadata['component']==component)), :]
                   .sort_values('end_year')
                   .iterrows()):
        src = row["data_file_name"]
        desc = row["var_desc"].strip()
        var = row["var_name"].lower().strip()
        var2desc[var] = desc
        mimi_src_file_date = parse(f'{row["end_year"]}-12-31').date()    
        matches = list(Path(volume_path).glob(f'{src}.[Xx][Pp][Tt]'))
        if matches:
            files[matches[0]] = mimi_src_file_date

    return files, var2desc

# COMMAND ----------

def get_tabname(txt):
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9\s]', '', txt.lower())).replace(' ','_')

# COMMAND ----------

def convert_safe_doubles_to_int(df):
    # Get a list of all Double columns
    double_cols = [field.name for field in df.schema.fields 
                    if isinstance(field.dataType, DoubleType)]
    
    # Function to check if a column contains only integer values
    def is_integer(col):
        return (f.count(f.when(f.col(col).cast(IntegerType()) == f.col(col), True)) == f.count(f.when(f.col(col).isNotNull(), True)))
    
    # Identify columns that can be safely converted to Integer
    convertible_cols = [col for col in double_cols if df.select(is_integer(col)).first()[0]]
    
    # Perform the conversion
    for col in convertible_cols:
        df = df.withColumn(col, f.col(col).cast(IntegerType()))
    
    return df
