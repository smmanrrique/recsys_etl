# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %run ../Includes/connectSQLDB

# COMMAND ----------

# MAGIC %run ../Includes/UDFs2.0

# COMMAND ----------

# MAGIC %run ../Includes/Logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Extract

# COMMAND ----------

config_file = dbutils.widgets.get("config_file")
year = dbutils.widgets.get('year')
from_date = dbutils.widgets.get('from_date') 
id_file = dbutils.widgets.get('id_file')
nickname = dbutils.widgets.get('nickname')
indice = dbutils.widgets.get('indice') 

indice = int(indice) if indice != '' else ''

# COMMAND ----------

# DBTITLE 1,func_extract
@logging_time
def func_extract(process):
  update_sheet = process.get('update_sheet', False)
  sheet = f"{process['sheet']}_{process['year']}" if update_sheet else process['sheet']
  fill_cols = process.get('fill_cols', False)
  
  if fill_cols:
    if sheet:
      df = pd.read_excel(process['file'],
                         sheet_name = sheet, 
                         header= process['header'], 
                         dtype = process['schema'], 
                         usecols = process['cols'], 
                         skiprows = process['skip_rows'], 
                         names = process['cols_name']
                        )
    else:
      df = pd.read_excel(process['file'],
                       header= process['header'], 
                       dtype = process['schema'], 
                       usecols = process['cols'], 
                       skiprows = process['skip_rows'], 
                       names = process['cols_name']
                      )
    df[df.columns[:fill_cols]]=df[df.columns[:fill_cols]].fillna(method='ffill')

  else: 
    if sheet:
      df = pd.read_excel(process['file'],
                       sheet_name = sheet, 
                       header= process['header'], 
                       dtype = process['schema'], 
                       usecols = process['cols'], 
                       skiprows = process['skip_rows'], 
                       names = process['cols_name']
                      ).astype("str")
    else:
      df = pd.read_excel(process['file'],
                       header= process['header'], 
                       dtype = process['schema'], 
                       usecols = process['cols'], 
                       skiprows = process['skip_rows'], 
                       names = process['cols_name']
                      ).astype("str")
  
  # Create df spark
  df = spark_session.createDataFrame(df)

  df = df.select(*[udf_clean_values(column).alias(column) for column in df.columns])
  df = df.na.drop(subset=process['drop_na_cols'], how = 'all')
  
  vals = list(set(process['cols_name'])-set(process['pk']))
  df = df.withColumn('BK_PK_HASH', udf_convert_md5_hex_hash_to_big_int(concat_ws('',*process['pk']))) \
         .withColumn('VALUE_HASH', udf_convert_md5_hex_hash_to_big_int(concat_ws('',*vals))) \
         .withColumn('BATCH', lit(process['batch'])) \
         .withColumn('SOURCE', lit(process['file'])) \
         .withColumn('ANYO', lit(process['year'])) \
         .withColumn('FECHA_CREACION', current_timestamp())
  
  
  if process['filters']:
    for each_filter in process['filters']:
      df = df.filter(eval(each_filter))
  
  df.persist()
  inserts = df.count()

  try:
    df_stg = spark.read.jdbc(url = jdbcStgUrl, table = process['table_sql'], properties = connectionProperties)
    df_aux = df.join(df_stg, ['BK_PK_HASH', 'VALUE_HASH'], how='leftanti')
    write_df_to_db(df_aux, 'STG', process['table_sql'], mode_append)
  
  except:
    write_df_to_db(df, 'STG', process['table_sql'], mode_append)
    
  finally: 
    save_delta_lake(df, process, mode_append)
    df.unpersist()
    df = None
    df_stg = None
    df_aux = None

  return inserts 

# COMMAND ----------

cnf = sc.broadcast(json.loads(' '.join(dbutils.fs.head(config_file).split())))
cnf = cnf.value
process = cnf['extract'][indice]

process['batch_process']= cnf['global_vars']['batch_process']
process['year'] = year
process['from_date'] = from_date
process['nickname'] = nickname
process['file'] = id_file
process['processing'] = id_file

#Configuración de los tipos
process['schema']  = dict((key, str) for key, value in process['schema'].items())

process['table_ID'] = get_table_processID(process) 
func_extract(process)