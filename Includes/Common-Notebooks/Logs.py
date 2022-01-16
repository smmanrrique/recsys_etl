# Databricks notebook source
# DBTITLE 1,Imports 
from dataclasses_json import dataclass_json
from dataclasses import dataclass, field
from functools import wraps
import pandas as pd
from datetime import *
from typing import List
import time

# COMMAND ----------

# MAGIC %run ./connectSQLDB

# COMMAND ----------

# def get_table_processID(process):
#   """
#     Get the ProcessLogID  from the LOG_PROCESS self.
#     If the source does not exists, insert it to the source self.
#   """
#   db_table = '[log].[TABLE_PROCESS]'
#   df_table_aux = '[log].[CONFIG_FILE]'
#   df = select_to_db_table('STG', db_table)\
#                     .filter(col('TableProcessName').contains(process["table_mt"])) 
  
#   # Get parentID
#   config_file_id = select_to_db_table('STG', df_table_aux)\
#                         .filter(col('ConfigPath').contains(process['config_file']))\
#                         .take(1)[0]["ConfigFileID"]
  
#   # The dataframe is empty or the source does not exist
#   if df.rdd.isEmpty():
#     df = spark.createDataFrame([(process['business_unit'], process['table_mt'],
#                                  process['path'], process['table_mt'], 
#                                  process['table_sql'], config_file_id)], 
#                                    ['BusinessUnitID', 'TableProcessName', 
#                                     'TableProcessPath', 'DataBricksName', 'SQLName', 'ConfigFileID'])     
    
#     write_df_to_db(df, 'STG', db_table, 'append')
#     df = select_to_db_table('STG', db_table)\
#                     .filter(col('TableProcessName').contains(process["table_mt"]))

#   return df.take(1)[0]["TableID"]

# COMMAND ----------

# DBTITLE 1,Process
def get_file_processID(process):
  """
    Get the ProcessLogID  from the LOG_PROCESS self.
    If the source does not exists, insert it to the source self.
  """
#   file = process['file'].split("/")[-1:][0]
#   ext = file.split('.')[-1:][0]
  db_table = '[log].[FILE_PROCESS]'

  # Load dependency table
  df = select_to_db_table('STG', db_table)\
                        .filter(col('ProcessFileName').contains(process['file']))
  

  if df.rdd.isEmpty():
    df = spark.createDataFrame([(process['table_ID'], process['ext'], 
                                 process['src_path'], process['dest_path'], process['file'], 
                                 "-1", "-1", "-1", 
                                 "-1", datetime.now())], 
                               ['TableID', 'FileExtension', 
                                'FilePathOrigin', 'FilePathDest', 'ProcessFileName', 
                                'ProcessFileSize', 'ProcessRegexFileName', 
                                'CompressFileName', 'CompressFileType', 'UploadDate']) # AÑADIDO UPLOADDATE   
    write_df_to_db(df, 'STG', db_table, 'append')
    print(f'Save new row in {db_table} =[{process["file"]}]')

    df = select_to_db_table('STG', db_table)\
                        .filter(col('ProcessFileName').contains(process['file']))

  return df.take(1)[0]["ProcessFileID"]



def get_table_processID(process):
  """
    Get the ProcessLogID  from the LOG_PROCESS self.
    If the source does not exists, insert it to the source self.
  """
  db_table = '[log].[TABLE_PROCESS]'
  df = select_to_db_table('STG', db_table)\
                    .filter(col('TableProcessName').contains(process["table_mt"])) 

  # The dataframe is empty or the source does not exist
  if df.rdd.isEmpty():
    df = spark.createDataFrame([(process['business_unit'], process['table_mt'],
                                 process['path'], process['table_mt'], 
                                 process['table_sql'], process['config_file_id'])], 
                                   ['BusinessUnitID', 'TableProcessName', 
                                    'TableProcessPath', 'DataBricksName', 'SQLName', 'ConfigFileID'])     
    
    write_df_to_db(df, 'STG', db_table, 'append')
    df = select_to_db_table('STG', db_table)\
                    .filter(col('TableProcessName').contains(process["table_mt"]))

  return df.take(1)[0]["TableID"]


def set_table_depency(process):
  """
    Get the ProcessLogID  from the LOG_PROCESS self.
    If the source does not exists, insert it to the source self.
  """
  db_table = '[log].[TABLE_PROCESS]'
  db_dependencies = '[log].[TABLE_DEPENDENCIES]'

  # Get parentID
  parent_id = select_to_db_table('STG', db_table)\
                        .filter(col('TableProcessName').contains(process['parent']))\
                        .take(1)[0]["TableID"]

  # Load dependency table
  df = select_to_db_table('STG', db_dependencies)\
            .filter( (col('TableID') == process["table_ID"] ) & (col('ParentTableID') == parent_id ))
  
  if df.rdd.isEmpty():
    df = spark.createDataFrame([(process["table_ID"], parent_id,)], 
                                   ['TableID', 'ParentTableID'])        
    write_df_to_db(df, 'STG', db_dependencies, 'append')
    print(f'Save new row in {db_dependencies} = {process["table_ID"]} - {parent_id}')
  return parent_id


def get_config_file(process):
  """
    Get the ProcessLogID  from the LOG_PROCESS self.
    If the source does not exists, insert it to the source self.
  """
  db_table = '[log].[CONFIG_FILE]'

  # Get parentID
  df = select_to_db_table('STG', db_table)\
                        .filter(col('ConfigPath').contains(process['config_file']))
  
  if df.rdd.isEmpty():    
    df = spark.createDataFrame([( process['config_file'], process['business_unit'],  process['active'], process['order_execution'],)], 
                                   ['ConfigPath', 'BusinessUnit', 'Active', 'OrderExecution'])    
    write_df_to_db(df, 'STG', db_table, 'append')
    df = select_to_db_table('STG', db_table)\
                        .filter(col('ConfigPath').contains(process['config_file']))
  
  return df.take(1)[0]["ConfigFileID"]

# COMMAND ----------

def get_congif_file_id(process):
  df_table = '[log].[CONFIG_FILE]'
  config_file_id = select_to_db_table('STG', df_table)\
                        .filter(col('ConfigPath').contains(process['config_file']))\
                        .take(1)[0]["ConfigFileID"]
  return config_file_id

# COMMAND ----------

# DBTITLE 1,LOG_PROCESS
@dataclass
class LOG_PROCESS:
  
  TableID: int 
  StartDate: str
  LayerID: int
  EndDate: str
  Batch:str 
  Duration: float = 0
  Inserts: int = 0
  Updates: int = 0
  Deletes: int = 0
  ErrorName: str = "INICIADO"
  Status: str = "PROCESANDO"
  

  def __init__(self, table_id:str, layer:str, batch:str): 
    self.Duration = time.time()
    self.LayerID = int(layer.split('-')[0])+1
    self.TableID = table_id
    self.StartDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.EndDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.Batch =  batch
    
  def __post_init__(self):
    self.Inserts = 0
    self.Updates = 0
    self.Deletes= 0
    self.ErrorName = "INICIADO"
    self.Status= "PROCESANDO"
    

  def __repr__(self):
    return f"""<TABLE_PROCESS( TableID:'{self.TableID}',  StartDate:'{self.StartDate}', 
              EndDate:'{self.EndDate}', Duration:'{self.Duration}', 
              Inserts:'{self.Inserts}', Updates:'{self.Updates}', 
              Deletes:'{self.Deletes}', ErrorName:'{self.ErrorName}', 
              Status:'{self.Status}', LayerID:'{self.LayerID}', Batch:'{self.Batch}')>"""

  def finish(self, sms, status):
      self.Status = status
      self.ErrorName = sms
      self.EndDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      self.Duration = time.time() - self.Duration
      self.save_state()

  def save_state(self):
    db_table = '[log].[LOG_PROCESS]'
    
    df = spark.createDataFrame([(self.TableID, self.StartDate, self.EndDate, self.Duration, self.Inserts,
                                 self.Updates, self.Deletes, self.ErrorName[:1024], self.Status, self.LayerID, self.Batch,)], 
                               ['TableID', 'StartDate', 'EndDate', 'Duration','Inserts', 
                                'Updates', 'Deletes', 'ErrorName', 'Status', 'LayerID', 'Batch'])

    write_df_to_db(df, 'STG', db_table, 'append')
    print(f'Save new row in {db_table}')
    return None

# COMMAND ----------

# DBTITLE 1,Time Measure
def logging_time(f):  
  @wraps(f)
  def wrapper(*args, **kwargs):
    process = args[0]
    log_process = LOG_PROCESS(process["table_ID"], process["layer"], process["batch_process"])
    try:
      log_process.Inserts = f(*args, **kwargs)
      status = "OK"  
      error_sms = f'PROCESO OK FUNCTION:{f.__name__}[{process["processing"]}]'
    except Exception as e:
      status = "ERROR"
      # error_sms = f'ERROR FUNCTION:{f.__name__}:[{e.message}]' if e  and hasattr(e, 'message')  else f'ERROR FUNCTION:{f.__name__}:[{e.message}]'
      error_sms = f'ERROR FUNCTION:{f.__name__}[{process["processing"]}]:[{e.message}]' if hasattr(e, 'message')  else f'ERROR FUNCTION:{f.__name__}[{process["processing"]}]:[{e}]'
      raise
    finally:
      log_process.finish(error_sms, status)
    return log_process.Inserts 
  return wrapper

# COMMAND ----------

#PARA SACAR LOS NOMBRES DE LAS COLUMNAS Y EL ESQUEMA INICIAL
def get_initial_values():
  config_file = dbutils.widgets.get("config_file")
  cnf = sc.broadcast(json.loads(' '.join(dbutils.fs.head(config_file).split())))
  for i in range(0, len(cnf.value['extract'])):
    print('Iteración:',i)
    config = cnf.value['extract'][i]
    df = pd.read_excel(config['file'], sheet_name = config['sheet'], header=config['header'], usecols = config['cols']).astype("str")
    dict_names = make_dict_cols_names(df.columns,config['sep'])
    df = df.rename(columns=dict_names)
    print(df.columns)
    print(df.dtypes.to_dict())
    
#DESCOMENTAR PARA SACAR VALORES INICIALES
# get_initial_values()