# Databricks notebook source
# MAGIC %run ../Includes/Common-Notebooks/Common

# COMMAND ----------

# DBTITLE 1,func_extract_excel
def save_extract_data(process:dict, df:pd.Series) -> None:
  
  drop_cols = process.get('drop_vals', None)
  if drop_cols:
    vals = list(set(df.columns)-set(process['pk'] + process['drop_vals']))      
  else:  
    vals = list(set(df.columns)-set(process['pk']))    
  df = df.withColumn('BK_PK_HASH',udf_convert_md5_hex_hash_to_big_int(concat_ws('',*process['pk']))) \
         .withColumn('VALUE_HASH', udf_convert_md5_hex_hash_to_big_int(concat_ws('',*vals))) \
         .withColumn('BATCH', lit(process['batch'])) \
         .withColumn('SOURCE', lit(process['file'])) \
         .withColumn('INGEST_DATE', current_timestamp())
  
  if process['filters']:
    for each_filter in process['filters']:
      df = df.filter(eval(each_filter))
    

  df.persist()
  inserts = df.count()
#   display(df)
  save_delta_lake(df, process, mode_append)
  df.unpersist()
  return None
  


# func_extract_excel
def func_extract_csv(process: dict) -> pd.Series:  
  
  if process["schema"]:
    df = spark.read.option("delimiter", process['delimiter'])\
                   .option("encoding", process['encoding'])\
                   .csv(f"/mnt/{process['file']}", 
                        header=process['header'],
                        schema=eval(process['schema'])
                       )
  else:
    df = pd.read_csv(f"/dbfs/mnt/{process['file']}",
                     header=process['header'],
                     names=process["names"],
                     sep=process['delimiter'], 
                     encoding=process['encoding'])
    
    # Create df spark
    df = spark_session.createDataFrame(df)
  return save_extract_data(process, df)


def func_extract_json(process) -> None:
  df = spark.read \
            .json(f"/mnt/{process['file']}")
  
  for c in df.columns:
    df = df.withColumnRenamed(c, c.upper())
  return save_extract_data(process, df) 

# func_extract_excel
def func_extract_excel(process) -> None:
  if process['sheet_name']:
    df = pd.read_excel(process['file'],
                         sheet_name = process['sheet_name'], 
                         header= process['header'], 
                         dtype = process['dtype'], 
                         usecols = process['usecols'], 
                         skiprows = process['skiprows'], 
                         names = process['names'],
                         engine='openpyxl' 
                        )
  else:
    df = pd.read_excel(process['file'],
                         header= process['header'], 
                         dtype = process['dtype'],
                         usecols = process['usecols'], 
                         skiprows = process['skiprows'], 
                         names = process['names'],
                         engine='openpyxl',
                         encode = 'latin1'
                        )
    df = df.astype(str).replace('nan',None)
    
  # Create df spark
  df = spark_session.createDataFrame(df) 
  return save_extract_data(process, df)

# COMMAND ----------

# DBTITLE 1,Main
process = dbutils.widgets.get("config_file")
process = json.loads(process.replace("'", "\""))

if process['ext'] == 'csv':
  df = func_extract_csv(process)
elif process['ext'] == 'json':
  df = func_extract_json(process)
elif 'xls' in process['ext']:
  df = func_extract_excel(process)
else:
  print('ERROR EXT')


# display(df)
# df = None
# print(process)
