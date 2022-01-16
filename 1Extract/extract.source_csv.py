# Databricks notebook source
# MAGIC %run ../Includes/Common-Notebooks/Common

# COMMAND ----------

# DBTITLE 1,Widgets
config_file = dbutils.widgets.get("config_file")
from_date = dbutils.widgets.get('from_date')
file = dbutils.widgets.get('file')
nickname = dbutils.widgets.get('nickname')
indice = dbutils.widgets.get('indice')

indice = int(indice) if indice != '' else ''

# COMMAND ----------

# DBTITLE 1,func_extract
# @logging_time
def func_extract(process: dict) -> pd.Series:  
  
  if process["nickname"] != 'TRIPADVISOR':
    df = spark.read.option("delimiter", process['delimiter'])\
                   .option("encoding", process['encoding'])\
                   .csv(process['file'], 
                        header=process['header'],                     
                        mode="DROPMALFORMED", 
                        schema=eval(process['schema'])
                       )
  else:
    df = pd.read_csv(f"/dbfs/{process['file']}", sep=process['delimiter'], encoding=process['encoding'])
    # Create df spark
    df = spark_session.createDataFrame(df)
  
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
  save_delta_lake(df, process, mode_append)
  df.unpersist()
  df = None

  return df 

# COMMAND ----------

# DBTITLE 1,Main
cnf = sc.broadcast(json.loads(' '.join(dbutils.fs.head(config_file).split())))
cnf = cnf.value
process = cnf['extract'][indice]

process['batch_process']= cnf['global_vars']['batch_process']
process['year'] = year
process['from_date'] = from_date
process['nickname'] = nickname
process['file'] = file
# process['table_ID'] = get_table_processID(process) 

df = func_extract(process)
display(df)