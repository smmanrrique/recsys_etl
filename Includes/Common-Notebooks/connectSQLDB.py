# Databricks notebook source
jdbcHostname = 'hiberusdatapoc.database.windows.net'
jdbcPort = 1433
jdbcDatabase = 'AdventureWorksLT'
jdbcUsername = 'HiberusAdmin'
jdbcPassword = 'P@22W0rd!'

# COMMAND ----------

# DBTITLE 1,Credentials
jdbcDwUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

def write_df_to_db(df, database, db_table, mode):
  df.write.jdbc(url = jdbcDwUrl, table = db_table, properties = connectionProperties, mode = mode)

# COMMAND ----------

def select_to_db_table(pushdown_query):
  """pushdown_query: query to ejecutar. Se antepone el nombre de la base de datos de databricks a la que pertenece la tabla. Por ejemplo: 'Transform.Categorias_horas_budget'.
     database: base de datos SQL a donde ira dirigida la copia. Puede ser 'DWH' o 'STG'.
     Le pasamos una tabla, por ejemplo: select_to_db_table('DWH', 'CDG.DIM_CATEGORIA')
  """
  df = spark.read.jdbc(url=jdbcDwUrl, table=pushdown_query, properties=connectionProperties)
  return df