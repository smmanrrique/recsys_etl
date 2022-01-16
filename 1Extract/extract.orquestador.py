# Databricks notebook source
# MAGIC %run ../Includes/Common-Notebooks/Common

# COMMAND ----------

def widgets_values(config_file:str, year:str, from_date:str ) -> (int,int,int):
  CONFIG_PATH = '/mnt/tfm-recsys/0-Ingest/config/'
  config_file = CONFIG_PATH + config_file if (CONFIG_PATH not in config_file) else config_file
  from_date = str(date.today()+ relativedelta(months=-6)) if from_date == '' else from_date
  year = str(date.today().year) if year == '' else year
  return config_file , year, from_date
  
def set_config_batch(config_file:str) -> None:
  # Write new batch in config file
  batch = datetime.now().strftime("%Y%m%d%H%M%S")
  cnf = sc.broadcast(json.loads(' '.join(dbutils.fs.head(config_file).split())))
  cnf = cnf.value
  cnf['global_vars']['batch_process'] = batch
  dbutils.fs.put(config_file, json.dumps(cnf, ensure_ascii=False), True)

# COMMAND ----------

# DBTITLE 1,Global vars
config_file , year, from_date = widgets_values(dbutils.widgets.get('config_file'), 
                                                 dbutils.widgets.get('year'), 
                                                 dbutils.widgets.get('from_date')) 


set_config_batch(config_file)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Extract

# COMMAND ----------

# DBTITLE 1,share_ingest_table_delete
def share_ingest_table_delete(process, each_file):
  aux_counter = 10
  while aux_counter >0:
    try:
      name = each_file
      container = process['src_path'].split('/')[3]
      process['processing'] = '/dbfs/mnt/' + container + '/' + name
      dst_name = ingest_mt.split('.')[1]
      delta_table = DeltaTable.forPath(spark, f'/mnt/florette-bi/{process_ingest}/{schema_ingest}/{dst_name}')
      delta_table.delete(f"NAME = '{name}'")
      aux_counter = 0
    except Exception as e:
      time.sleep(10)
      aux_counter -= 1
      print(e)
  return None

# COMMAND ----------

# DBTITLE 1,func_orchestrator_extract
@logging_time
def func_orchestrator_extract(process, i, files_to_delete):
  df = spark.table(ingest_mt)
#   file_path = '/'.join(process['src_path'].split('/')[4:])
#   file_path = f'{file_path}/{process["year"]}' if process['path_year'] == 'true' else file_path
#   process['not_contains'] = process.get("not_contains", None)
  
#   if file_path == '':
#     df = df.filter(df['PATH'] == '') \
#            .where(df['NICKNAME'].contains(process['nickname'])) \
#            .orderBy('FILE')
#   else:
#     df = df.filter(df['PATH'] == file_path) \
#            .where(df['NICKNAME'].contains(process['nickname'])) \
#            .orderBy('FILE')
    
#   if process['not_contains']:
#     df = df.where(~df['NICKNAME'].contains(process['not_contains']))
    
#   df.persist()
#   inserts = df.count()
#   process['processing'] = ''
#   if inserts > 0:
#     total_files = list(df.select('NAME').toPandas()['NAME'])
#     total_files.sort()
#     if (process['feed_multiple_tables'].lower() == 'false'):
#         files_to_delete.append(total_files)
    
#     if (DeltaTable.isDeltaTable(spark, process['path'])) & (process['delete_table']):
#       delete_extract_tables(process) # DELETE EXTRACT TABLES IF EXITS AND IT'S NOT THE SAME ONE THAN IN THE PREVIOUS ITERATION
    
#     for each_file in total_files:
#       name = each_file
#       container = process['src_path'].split('/')[3]
#       each_file = '/dbfs/mnt/florette-bi/0-Ingest/' + container + '/' + name
      
#       # Rename variables to register the files in log table
#       process['src_path'] = '/dbfs/mnt/' + container + '/' + name
#       process['dest_path'] = each_file
#       process['file'] = name.split("/")[-1:][0]
#       process['file_ID'] = get_file_processID(process)
      
#       # Variable needed for the log --> log contains the final status (OK/ERROR) + file path which is contained in process['processing']
#       process['processing'] = process['src_path']
      
#       notebook = process.get('notebook', False)    
#       if notebook and ( 'xls' in process ['ext']):        
#           dbutils.notebook.run('extract.source_xlsx_pivot', 0, 
#                                {'config_file': config_file, 
#                                 'id_file': each_file, 
#                                 'nickname': process['nickname'], 
#                                 'year': year, 
#                                 'from_date': from_date, 
#                                 'indice': i}) 
#       else:
#         if (process ['ext'] == 'csv') | (process ['ext'] == 'txt'):
#           dbutils.notebook.run('extract.source_csv', 0, 
#                                {'config_file': config_file, 
#                                 'id_file': each_file, 
#                                 'nickname': process['nickname'],
#                                 'year': year, 
#                                 'from_date': from_date, 'indice': i})
#         if (process ['ext'] == 'xlsx') | (process ['ext'] == 'xlsm') | (process ['ext'] == 'xls'):        
#           dbutils.notebook.run('extract.source_xlsx', 0, 
#                                {'config_file': config_file, 
#                                 'id_file': each_file, 
#                                 'nickname': process['nickname'], 
#                                 'year': year, 
#                                 'from_date': from_date, 
#                                 'indice': i})
    
#   # Delete files from temporal table (ingest.new_files_to_upload) if the extraction was succesfull
#   if i == process['last_iteration']:
#     files_to_delete = list(set([item for sublist in files_to_delete for item in sublist]))
#     for each_file in files_to_delete:
#       share_ingest_table_delete(process, each_file)

#   df.unpersist()
#   return inserts
  return 0

# COMMAND ----------

cnf = sc.broadcast(json.loads(' '.join(dbutils.fs.head(config_file).split())))
cnf = cnf.value
config = cnf['extract']
last_iteration = len(config)-1

files_to_delete = []
check_tables_path = []
results = 0

for i, process in enumerate(config):
  process['last_iteration'] = last_iteration
  process['from_date'] = from_date
  process['year'] = year
  process['batch_process']= cnf['global_vars']['batch_process']
  process['active']= cnf['global_vars']['active']
  process['order_execution']= cnf['global_vars']['order_execution']
  process['config_file'] = config_file
  process['config_file_id'] = get_config_file(process)
  process['table_ID'] = get_table_processID(process)
  
  if (process ['table_mt'] not in check_tables_path) or (results == 0):
    process['delete_table'] = True
    check_tables_path.append(process['table_mt'])
  else:
    process['delete_table'] = False
  
  
  result = func_orchestrator_extract(process,i,files_to_delete)
  results += result