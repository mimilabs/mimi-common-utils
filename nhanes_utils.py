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
            var_old = row['variable_codename'].lower()
            var_new = row['variable_codename_use'].lower()
            manual_harmonizations[src][var_old] = var_new 

# COMMAND ----------

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

focus_files = ['Acculturation',
 'Air Quality',
 'Albumin & Creatinine - Urine',
 'Alcohol Use',
 'Alpha-1-Acid Glycoprotein - Serum (Surplus)',
 'Blood Pressure',
 'Blood Pressure & Cholesterol',
 'Body Measures',
 'Bowel Health',
 'Cardiovascular Health',
 'Cholesterol - HDL',
 'Cholesterol - LDL & Triglycerides',
 'Cholesterol - Total',
 'Demographic Variables & Sample Weights',
 'Diabetes',
 'Fasting Questionnaire',
 'Glycohemoglobin',
 'Glyphosate (GLYP) - Urine',
 'High-Sensitivity C-Reactive Protein',
 'Hospital Utilization & Access to Care',
 'Income',
 'Insulin',
 'Kidney Conditions',
 'Medical Conditions',
 'Oral Glucose Tolerance Test',
 'Plasma Fasting Glucose',
 'Preventive Aspirin Use',
 'Smoking - Adult Recent Tobacco Use & Youth Cigarette/Tobacco Use',
 'Smoking - Cigarette Use',
 'Smoking - Household Smokers',
 'Smoking - Recent Tobacco Use',
 'Standard Biochemistry Profile']

# COMMAND ----------

metadata = spark.read.table(dlt_path_metadata).toPandas()
metadata['data_file_desc'] = (metadata['data_file_desc']
                                  .apply(lambda x: file_desc_mapping.get(x,x)))
metadata['var_name'] = (metadata['var_name'].apply(lambda x: x.lower()))

# COMMAND ----------

def prep(target_desc):

    files = {}
    src2var = defaultdict(list) # source => varname
    var2desc2src = {} # varname => desc => source (list)

    def get_common_key(var1, var2):
        if var1 == var2:
            return var1
        elif var1[:-1] == var2[:-1]:
            return var1[:-1] + "_"
        elif var1[:-1] == var2:
            return var1[:-1] + "_"
        elif var1 == var2[:-1]:
            return var2[:-1] + "_"
        else:
            return None
    
    component = ''
    for _, row in (metadata.loc[metadata['data_file_desc']==target_desc, :].iterrows()):
        src = row["data_file_name"]
        desc = row["var_desc"]
        var = row["var_name"]
        component = row["component"]
        mimi_src_file_date = parse(f'{row["end_year"]}-12-31').date()    
        filepath = Path(f'{volume_path}/{src}.XPT')
        if filepath.exists():
            files[filepath] = mimi_src_file_date
            src2var[src].append(var)
            if var not in var2desc2src:
                var2desc2src[var] = defaultdict(list)
            var2desc2src[var][desc].append(src)

    blacklist = []
    for src, var_lst in src2var.items():
        for var1, var2 in combinations(var_lst, 2):
            common_key = get_common_key(var1, var2)
            if common_key is not None:
                blacklist.append(common_key)
    blacklist = set(blacklist)

    var2desc = {}
    for var, desc2src in var2desc2src.items():
        desc_lst = [(desc + ' (from ' + ', '.join(src_lst) + ')') 
                            for desc, src_lst in desc2src.items()]
        var2desc[var] = desc_lst
    file_lst = sorted([(k,v) for k, v in files.items()], key=lambda x: x[1], reverse=True)
    var_lst = [k for k in var2desc.keys()]

    var2var = {}
    for var1, var2 in combinations(var_lst, 2):
        common_key = get_common_key(var1, var2)
        if common_key is None:
            continue
        elif common_key in blacklist:
            continue
        
        var2var[var1] = common_key
        var2var[var2] = common_key
        if common_key not in var2desc:
            var2desc[common_key] = []
        var2desc[common_key] += var2desc[var1]
        var2desc[common_key] += var2desc[var2]

    # augment with the manual harmonizations
    for src, var_lst in src2var.items():
        for varold, varnew in manual_harmonizations.get(src, {}).items():
            if varold == 'mcq500':
                # I think this is a bug... so we skip.
                continue
            if varold not in var2var:
                var2var[varold] = varnew
                if varnew not in var2desc:
                    var2desc[varnew] = []
                if varold in var2desc:
                    var2desc[varnew] += var2desc[varold]

    var2desc = {k: ', '.join(list(set(v))) for k, v in var2desc.items()}

    comp2abbr = {'Demographics': 'demo', 
                 'Dietary': 'diet', 
                 'Examination': 'exam', 
                 'Laboratory': 'lab', 
                 'Questionnaire': 'qre'}

    return file_lst, var2var, var2desc, comp2abbr[component]

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
