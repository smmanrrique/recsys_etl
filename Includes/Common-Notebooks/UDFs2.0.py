# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# DBTITLE 1,Imports
import os
import re
import sys
import json
import numpy as np
import unidecode
import collections
import pandas as pd
import hashlib
import math
import calendar
from datetime import *
from dateutil.relativedelta import *
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from dataclasses import dataclass
from functools import wraps
from time import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
# from multiprocessing.pool import ThreadPool

from pyspark.sql import SparkSession
spark_session = SparkSession.builder.getOrCreate()

# COMMAND ----------

#Configuration string to connect Azure Store Blob Containers
AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=florettedatalake;AccountKey=uW8xl3sBzXcbq7Kr1qgh+LxXYaZiUZaOwUNyYtclur5qVZbfFeAYVpOrZDr/6KMP8ukqfsLdRaONWcCeDmi0YA==;EndpointSuffix=core.windows.net'

#Containers
source_storage_account_name = 'florettedatalake'
destination_storage_account_name = source_storage_account_name
target_container_name = 'florette-bi'

# COMMAND ----------

# MAGIC %md
# MAGIC # Global vars

# COMMAND ----------

# DBTITLE 1,Global vars
chunck_len = 20000

mode_overwrite = 'overwrite'
mode_append = 'append'

schema_cdg = 'cdg'
schema_sc = 'supply_chain'
schema_master = 'master'
schema_log = 'logistica'
schema_ingest = 'ingest'

path_ext = '1-Extract'
path_transf = '2-Transform'
path_dwh = '3-DWH'

process_ingest= '0-Ingest'
process_ext = '1-Extract'
process_transf = '2-Transform'
process_dwh = '3-DWH'

num_vals = []
update_cols = []
check_pk_centros = ['TORREPACHECO','NOBLEJAS','CANARIAS','MILAGRO','INIESTA','FLORISTAN','TORTOSA','ARGUEDAS', 'INCONNU']

date_default='1600-01-01'
none_default = '*viene_none*'

# ============ dias_festivos ======================= #
# feriados2021=['2021-01-01','2021-01-06','2021-03-19','2021-04-02','2021-05-01','2021-08-15','2021-10-12','2021-11-01','2021-12-06','2021-12-08','2021-12-25']
# feriados2020 =['2020-01-01', '2020-04-10','2020-12-25', '2020-05-01', '2020-12-25'  ] 
# feriados2019 =['2019-01-01', '2019-04-19', '2019-05-01', '2019-08-15', '2019-10-12', '2019-11-01', '2019-12-25'] 
# feriados2018 = ['2018-01-01', '2018-01-06', '2018-03-30', '2018-05-01', '2018-08-15', '2018-10-12', '2018-11-01', '2018-12-06','2018-12-08','2018-12-25']
# feriados2017 =['2017-01-06', '2017-04-17', '2017-08-15', '2017-10-12', '2017-11-01', '2017-12-06', '2017-12-25'] 

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataframes

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Pivot

# COMMAND ----------

def unpivot(df, by, new_column_key_name, new_column_value_name):

    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Create and explode an array of (column_name, column_value) structs
    kvs = explode(array([
      struct(lit(c).alias(new_column_key_name), col(c).alias(new_column_value_name)) for c in cols
    ])).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs." + new_column_key_name, "kvs." + new_column_value_name])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get batch

# COMMAND ----------

def remove_historical_cols(df, ind):
  cols = df.columns[ind:]
  cols_to_drop = []
  check = []
  for col in cols:
    is_daily = re.findall('(\d+-*\d+-*\d+)', col)
    check.append(True) if is_daily else check.append(False)
    cols_to_drop if is_daily else cols_to_drop.append(col)
  if (True in check) & (len(cols_to_drop)>0):
    df = df.drop(cols_to_drop, axis = 1)
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning names

# COMMAND ----------

def handler_center_name(name):
  """
    Function to split Fabric's name and get only first elemnt
    str: fabric's name
    ELIMINAR -> 'ARGUEDAS'
    ['TORREPACHECO','NOBLEJAS','CANARIAS','MILAGREEO','INIESTA','FLORISTAN','TORTOSA','INNCONU' ]
  """
  if '+' in name:
    name = name.split('+')[0]
    
  name = unidecode.unidecode(name).strip().upper()
  name = name.replace('99', 'MILAGRO')
  name = name.replace('ARGUEDAS', 'FLORISTAN')
  name = name.replace('BOENN', 'TORREPACHECO BOENN')
  return name

udf_handler_center_name = spark.udf.register("udf_handler_center_name", handler_center_name, StringType())

# COMMAND ----------

def clean_cols_name(name):
  name = '_'.join(re.sub('[^a-zA-Z0-9]', ' ', name).split())
  name = re.sub('_ANO_', '_ANYO_', name)
  name = re.sub('_BUDGET_', '_BDG_', name)
  name = re.sub('_BUDGET', '_BDG', name)
  name = re.sub('BUDGET_', 'BDG_', name)
  name = re.sub('_CENTRODO_', '_FABRICADO_', name)
  name = re.sub('_DE_', '_', name)
  name = re.sub('_DEL_', '_', name)
  name = re.sub('_DE_LA_', '_', name)
  name = re.sub('_DIRECTA_', '_DIRT_', name)
  name = re.sub('_EMBOLSADA_BOLSAS_', '_EMBOLSADA_BOLS_', name)
  name = re.sub('_EN_', '', name)
  name = re.sub('_EXTERNE_', '_EXT_', name)
  name = re.sub('_HORAIRE_', '_HORARIO_', name)
  name = re.sub('_KG_P_H_SELECCION_MP_', '_MP_', name)
  name = re.sub('_LA_', '_', name) 
  name = re.sub('_MAQUINA_OF_', '_OF_', name)
  name = re.sub('_MAQUINA_SOBRE_OF_', '_OF_', name)
  name = re.sub('_MATERIA_PRIMA_', '_MP_', name)
  name = re.sub('_MATERIA_PRIMA_', '_MP_', name)
  name = re.sub('_MO_DIRT_EMBOLSADO_SOBRE_OF_', '_MO_DIR_EMBOL_OF_', name) 
  name = re.sub('_MO_VARIABLE_', '_MO_', name) 
  name = re.sub('_MP_ENTREPOT_', '_MP_', name)
  name = re.sub('_MPKILO_', '_MP_KILO_', name) 
  name = re.sub('_MPOF_', '_MP_OF_', name)
  name = re.sub('_PAR_MP_', '_MP_', name)
  name = re.sub('_PER_', '_', name)
  name = re.sub('_POR_OF_', '_OF_', name)
  name = re.sub('_PROD_', '_DIRT_', name)
  name = re.sub('_PROD_', '_DIRT_', name)
  name = re.sub('_REEL_', '_REAL_', name)
  name = re.sub('_REEL', '_REAL', name)
  name = re.sub('_KG/P/H_SELECCION_', '_', name)
  name = re.sub('_REEMBOLSADA_UL_', '_REEMB_UL_', name)
  name = re.sub('_SOBRE_OF_', '_OF_', name)
  name = re.sub('_STOCK_MP_FIABILITE_DONNEES_', '_STOCK_MP_', name)
  name = re.sub('_TIRADA_PRODUCTO_TERMINADO_EXCLUIDO_EL_SUBCONTRATADO_', '_TIR_PT_EXCL_SUBCON_', name)
  name = re.sub('_UL_NEGOCIO_', '_UL_NEG_', name) 
  name = re.sub('AJUSTEMENT', 'AJUSTE', name)
  name = re.sub('BUDGET_', 'BDG_', name)
  name = re.sub('CANDIDAD', 'CANTIDAD', name)
  name = re.sub('COUT_POSSESSION_', 'COSTE_POSESION_', name)
  name = re.sub('IMP_PRODUCTO_TIRADO_MP_BU_REAL', 'IMP_PROD_TIRADO_MP_BU_REAL', name) 
  name = re.sub('IMP_PRODUCTO_TIRADO_MP_GPR_REAL', 'IMP_PROD_TIRADO_MP_GPR_REAL', name) 
  name = re.sub('IMPORTE_', 'IMP_', name)
  name = re.sub('REEL_', 'REAL_', name)
  name = re.sub('RENDIMIENTO_', 'PCT_REND_', name)
  name = re.sub('TARIFA_HORARIA_MO_VARIABLE_EXT_REAL', 'TARIFA_HORARIA_MO_EXT_REAL', name) 
  name = re.sub('TAUX_', 'TARIFA_', name) 
  name = re.sub('_BOLSA_PERSONA_HORA_EMBOLSADO_', '_BPH_EMBOLS_', name)
  name = re.sub('_(EXTERNE)_', '_EXT_', name) 
  name = re.sub('CANTIDAD_CONSUMIDA_MP_', 'CANTIDAD_MP_CONSUMIDA_', name)
  name = re.sub('_CONSUMIDA_MP_', '_MP_CONSUMIDA_', name)
  return name

udf_clean_cols_name = spark.udf.register("udf_clean_cols_name", clean_cols_name, StringType())


# COMMAND ----------

def clean_cols_name2(name):
  name = re.sub('[^a-zA-Z0-9]', ' ', name)
  name = re.sub('ACHETEE','COMPRADA', name)
  name = re.sub('ACHETE','COMPRADA', name)
  name = re.sub('AJUSTE','AJUSTE', name)
  name = re.sub('AJUSTEMENT','AJUSTE', name)
  name = re.sub('AJUSTES','AJUSTE', name)
  name = re.sub('APPROVISIONAMIENTO','APROVISIONAMIENTO', name)
  name = re.sub('APRO','APROVI', name)
  name = re.sub('APROVIVISIONAMIENTO','APROVI', name)
  name = re.sub('ATELIER','TALLER', name)
  name = re.sub(' AVEC ',' ', name) #sd
  name = re.sub('BOLSA PERSONA HORA','BOLSA_PERS_HORA', name)
  name = re.sub('BOLSA P H','BOLSA_PERS_HORA', name)
  name = re.sub('BOLSAS','BOLSA', name)
  name = re.sub('BOX','CAJA', name)
  name = re.sub('BUDGET','BDG', name)
  name = re.sub('CANDIDAD','CANT', name)
  name = re.sub('CANTIDAD','CANT', name)
  name = re.sub('COMPTAGE','CUENTA', name)
  name = re.sub('CONSOMMABLE','CONSUMIDO', name)
  name = re.sub('CONSOMMEES','CONSUMIDO', name)
  name = re.sub('CONSUMIDAS','CONSUMIDO', name)
  name = re.sub('CONSUMIDA','CONSUMIDO', name)
  name = re.sub('CONSUMOS','CONSUMIDO', name)
  name = re.sub('CONSUMO','CONSUMIDO', name)
  name = re.sub('CONCUMO','CONSUMIDO', name)
  name = re.sub('CORRECTION','CORRECCION', name)
  name = re.sub('COUT','COSTE', name)
  name = re.sub('DESTINATION','DESTINO', name)
  name = re.sub('DIRECTAS','DIR', name)
  name = re.sub(' DONNEES ',' ', name)
  name = re.sub('REEMBOLSADAS','REEMBOLS', name)
  name = re.sub('REEMBLOLSADOS','REEMBOLS', name)
  name = re.sub('REEMBOLSADOS','REEMBOLS', name)
  name = re.sub('REEMBOLSADA','REEMBOLS', name)
  name = re.sub('REEMBOLSADO','REEMBOLS', name)
  name = re.sub('EMBLOLSADOS','EMBOLS', name)
  name = re.sub('EMBOLSADAS','EMBOLS', name)
  name = re.sub('EMBOLSADOS','EMBOLS', name)
  name = re.sub('EMBOLSADA','EMBOLS', name)
  name = re.sub('EMBOLSADO','EMBOLS', name)
  name = re.sub('EMBOLSRA','EMBOLSADORA', name)
  name = re.sub('REEMBOLSRA','REEMBOLSADORA', name)
  name = re.sub(' ENTREPOT ',' ', name)
  name = re.sub('EPLUCHEES','PELADA', name)
  name = re.sub('EXCLUIDO','EX', name)
  name = re.sub('EXTERNE','EXTERNO', name)
  name = re.sub('FABRICADAS','FABRICADA', name)
  name = re.sub(' FIABILITE ',' ', name)
  name = re.sub('FILM','PELICULA', name)
  name = re.sub('GRP','GPR', name)
  name = re.sub('HORAIRE','HORAS', name)
  name = re.sub('IMPORTA','IMP', name)
  name = re.sub('IMPORTE','IMP', name)
  name = re.sub('INDIRECTAS','INDIR', name)
  name = re.sub('KGHPERSONA','KG_PERS_HORA', name)
  name = re.sub('KGPH','KG_PERS_HORA', name)
  name = re.sub('KILOS','KG', name)
  name = re.sub('KILO','KG', name)
  name = re.sub('LINEAS','LINEA', name)
  name = re.sub('MELANGE','MEZCLADA', name)
  name = re.sub('MONTANT','IMP', name)
  name = re.sub('NEGOCE','NEGOCIO', name)
  name = re.sub('MERCADERIAS','NEGOCIO', name)
  name = re.sub('NUMERO','NUM', name)
  name = re.sub('PARAGE','RECORTADO', name)
  name = re.sub('PASADO','PESADO', name)
  name = re.sub('PESADA','PESADO', name)
  name = re.sub('PESEES','PESADO', name)
  name = re.sub('PICKING','COSECHA', name)
  name = re.sub('POSSESSION','PROPIEDAD', name)
  name = re.sub('PREPARADE','PREPARADO', name)
  name = re.sub('PRESUPUESTO','BDG', name)
  name = re.sub('PRODUCTIVIDAD','PRODUCTIV', name)
  name = re.sub('PRODUCTIVAD','PRODUCTIV', name)
  name = re.sub('PRODUCTIVITAD','PRODUCTIV', name)
  name = re.sub('PRODUCTSPRESUPUESTO','PRODUCT_BDG', name)
  name = re.sub('PRODUCTOS','PRODUCT', name)
  name = re.sub('PRODUCTO','PRODUCT', name)
  name = re.sub('PRODUCTS','PRODUCT', name)
  name = re.sub('QTES','CANT', name)
  name = re.sub('QTE','CANT', name)
  name = re.sub('REAL','ACTUAL', name)
  name = re.sub('REEL','ACTUAL', name)
  name = re.sub('REFERENCIAS','REFERENCIA', name)
  name = re.sub('RENDEMIENTO','RENDIMIENTO', name)
  name = re.sub('RENDEMENT','RENDIMIENTO', name)
  name = re.sub('SERIEMINUTO','SERIE_MINUTO', name)
  name = re.sub('SITE','SITIO', name)
  name = re.sub('SOBRE','', name)
  name = re.sub('SOCIALES','SOCIAL', name)
  name = re.sub('STCOK','STOCK', name)
  name = re.sub('SOCK','STOCK', name)
  name = re.sub('SUBCONTRATADO','SUBCONT', name)
  name = re.sub('TAUX','TASA', name)
  name = re.sub('TERMINADO','TERMINADO', name)
  name = re.sub('THEORIQUES','TEORICO', name)
  name = re.sub('TIRADA','TIRADO', name)
  name = re.sub('TOUTE','TOTAL', name)
  name = re.sub('ULKG','UL_KG', name)
  name = re.sub('UTILIZZACION','UTILIZACION', name)
  name = re.sub('VARIABLES','VARIABLE', name)
  name = re.sub('VARIACION','VARIABLE', name)
  name = re.sub(' Y ', ' ', name)
  name = re.sub(' OF ',' ', name)
  name = re.sub(' OF',' ', name)
  name = re.sub(' EL ', ' ', name)
  name = re.sub(' PAR ', ' ', name)
  name = re.sub(' POR ', ' ', name)
  name = re.sub(' PER ', ' ', name)
  name = re.sub(' DE ', ' ', name)
  name = re.sub(' SE ', ' ', name)
  name = re.sub(' LA ', ' ', name)
  name = re.sub(' EN ', ' ', name)
  name = re.sub(' SF', ' SE', name)
  name = name.rstrip().lstrip()
  name = re.sub('\s+', '_', name)
  name = re.sub('_MATERIA_PRIMA_', '_MP_', name)
  name = re.sub('_MANO_OBRA_', '_MO_', name)
  name = re.sub('KG_H_PERSONA', 'KG_PERS_HORA', name)
  name = re.sub('KG_P_H', 'KG_PERS_HORA', name)
  name = re.sub('_P_H', '_PERS_HORA', name)
  
  return name

udf_clean_cols_name2 = spark.udf.register("udf_clean_cols_name2", clean_cols_name2, StringType())


def clean_cols_name_fact_actividad_centro(name):
  name = clean_cols_name2(name)
  name = re.sub('[^a-zA-Z0-9]', ' ', name)
  name = name.rstrip().lstrip()
  name = re.sub('\s+', '_', name)
  name = re.sub('MP_BRUTA', 'CANT_MP_BRUTA', name)
#   name = re.sub('CANT_PREPARADO_UL', 'CANT_PREPARADO_UL_BDG', name) ESTE CAMBIO NO HAY QUE HACERLO, SE IGNORA ESA COLUMNA
#   name = re.sub('CANT_PREPARADO_UL_BDG_BDG', 'CANT_PREPARADO_UL_BDG', name)
#   name = re.sub('CANT_PREPARADO_UL_BDG_CAJA', 'CANT_PREPARADO_UL_CAJA', name)
  name = re.sub('IMP_CONSUMIDO_MPNV_BU', 'IMP_CONSUMIDO_MPNV_LEMA_BU', name)
#   name = re.sub('HORAS_MAQUINA_EMBOLS', 'HORAS_MAQUINA', name) ESTO NO HAY QUE HACERLO, SON DOS METRICAS DIFERENTES
  name = re.sub('HORAS_MAQUINA', 'HORAS', name) #AÑADIDO
  name = re.sub('PRODUCTIV_DIRECTA_KG_PERS_HORA_SELECCION', 'PRODUCTIV_KG_PERS_HORA_SELECCION', name)
  name = re.sub('PRODUCTIV_KG_PERS_HORA_SELECCION_BDG', 'PRODUCTIV_DIRECTA_KG_PERS_HORA_SELECCION_BDG', name)
  name = re.sub('VARACION_STOCK', 'VARIABLE_STOCK_SE', name)
  
  return name

udf_clean_cols_name_fact_actividad_centro = spark.udf.register("udf_clean_cols_name_fact_actividad_centro", clean_cols_name_fact_actividad_centro, StringType())

def make_dict_cols_names(columns, sub):
  dict_columns = {}
  for c in columns:
    name = sub.join(re.sub('[^a-zA-Z0-9]', ' ', unidecode.unidecode(c).upper()).split())
    dict_columns[c] = clean_cols_name(name)
  return dict_columns


# COMMAND ----------

def support_cdg_tables(df, metric_column, total_cols, process):
  cols_names = get_values_column (df, metric_column)
  ignore_cols = process.get("ignore_cols", [])
  missing_cols = list(set(cols_names)-set(total_cols)-set(ignore_cols))
  
  if len(missing_cols)>0:
    data = [(process['table_mt'], process['batch_process'], missing_cols)]
    columns = ['TABLE', 'BATCH', 'COLUMNS']
    df = spark.createDataFrame(data, columns) \
              .withColumn('DATE', current_timestamp())
    df.write \
      .format("delta") \
      .mode(mode_append) \
      .option("overwriteSchema", "false") \
      .option('path', "/mnt/florette-bi/3-DWH/cdg/support_cdg_tables")\
      .saveAsTable('dwh.support_cdg_tables')
    print('CAUTION: THERE ARE COLUMNS THAT ARE NOT INCLUDED BECAUSE OF A NAMING PROBLEM: CHECK TABLE dwh.support_cdg_tables')

# COMMAND ----------

def replace_columns_name(df, columns):
    if isinstance(columns, dict):
        current_columns = df.columns
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

# COMMAND ----------

def set_gama(value): 
  '''Para la dim_articulo'''
  if str(value) == '100':
    output = 'I GAMA'
  elif str(value) == '400':
    output = 'IV GAMA'
  else:
    output = 'DESCONOCIDO'
  return output
udf_set_gama = spark.udf.register("udf_set_gama",set_gama, StringType())

# COMMAND ----------

def get_grms_bolsa(value):
  '''Para la dim_articulo'''
  value = re.sub('GRMS', 'GR', value)
  value = re.sub('GRS', 'GR', value)
  value = re.sub('GRAMO', 'GR', value)
  value = re.sub('GRAMOS', 'GR', value)
  value = re.sub('KILOS', 'KG', value)
  value = re.sub('KGS', 'KG', value)
  value = re.sub('KILO', 'KG', value)
  
  grms_units = re.findall('(\d+|\d+\.+\d+|\d+\,+\d+)(\s*)(GR|KG|K|G)(\W|$)', value)
  if len(grms_units)>0:
    grms = grms_units[-1][0].strip()
    grms = re.sub('\,', '.', grms)
    grms = float(grms)
    units = grms_units[-1][2].strip()
    if (units == 'KG')|(units == 'K'):
      grms = grms*1000
    return grms
  else:
    return None
udf_get_grms_bolsa = spark.udf.register("udf_get_grms_bolsa",get_grms_bolsa, StringType())

# COMMAND ----------

def get_unidades_bolsa(value):
  '''Para la dim_articulo'''
  unidades_bolsa = re.findall('(\/+)(\s*)(\d*)', value)
  if len(unidades_bolsa)>0:
    unidades_bolsa = unidades_bolsa[0][2].strip()
    return unidades_bolsa
  else:
    return None
udf_get_unidades_bolsa = spark.udf.register("udf_get_unidades_bolsa",get_unidades_bolsa, StringType())

# COMMAND ----------

# def get_marca_ze(value):
#   '''Para la dim_articulo'''
#   marca_ze = re.findall('( |-|)(NU ES|NUES|ES|FR)( )([A-Z]*)( )', value)
#   if len(marca_ze)>0:
#     marca_ze = marca_ze[0][3]
#     return marca_ze
#   else:
#     return None
# udf_get_marca_ze = spark.udf.register("udf_get_marca_ze",get_marca_ze, StringType())

def get_marca_ze(value):
  '''Para la dim_articulo'''
  marca_ze = re.findall('(-*)(NU ES|NUES|ES|FR)( )([A-Z]*)( )', value)
  if len(marca_ze)>0:
    marca_ze = marca_ze[0][3]
    return marca_ze
  else:
    return None
udf_get_marca_ze = spark.udf.register("udf_get_marca_ze",get_marca_ze, StringType())

# COMMAND ----------

def get_almacen_from_source(source):
  almacen = source.split("/")[-1][0]
  return almacen 
udf_get_almacen_from_source = spark.udf.register("udf_get_almacen_from_source", get_almacen_from_source, StringType())

# COMMAND ----------

def set_grupo(cod_almacen):
  if cod_almacen[0:2] == 'D5':
    result = 'MIL'
  elif cod_almacen[0:2] == 'F5':
    result = 'INI'
  elif cod_almacen[0:2] == 'H5':
    result = 'NOB'
  elif cod_almacen[0:2] == 'X5':
    result = 'TPA'
  elif cod_almacen[0:2] == 'M5':
    result = 'CAN'
  elif cod_almacen[0:2] == 'K5':
    result = 'FLO'
  else:
    result = None
  return result
udf_set_grupo = spark.udf.register("udf_set_grupo", set_grupo, StringType())

# COMMAND ----------

def filter_total_cols(batch, cols):
  '''
    Esta funcion se usa para eliminar las columnas sobrantes en la extraccion se debe indicar si es daily o hostorico para saber que columnas eliminar
  '''
  columns = []
  if batch == 'daily':
#     columns = [ v for v in ['2021-02-08', '2021-02-09', 'Total'] if not re.match('(\d+-*\d+-*\d+)', v) ]
    columns = list(filter(lambda v: not re.match('(\d+-*\d+-*\d+)', v), cols))
  elif batch == 'historico':
#     columns = [ v for v in ['2021-02-08', '2021-02-09', 'Total'] if not re.match('(\d*\s*\d*(-|_)*\s*(S|s)\d+(-|_)*\s*\d*)+', v) ]
    columns = list(filter(lambda v: not re.match('(\d*\s*\d*(-|_)*\s*(S|s)\d+(-|_)*\s*\d*)+', v), cols))
  print(f'{batch.upper()} columns to drop=[{columns}]')
  return columns

# COMMAND ----------

def get_end_description(value):
  return value.split(' ')[-1]

udf_get_end_description = spark.udf.register("udf_get_end_description", get_end_description, StringType())

def get_start_description(value, start=True):
  return ' '.join(value.split(' ')[:2])

udf_get_start_description = spark.udf.register("udf_get_start_description", get_start_description, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning values / formats

# COMMAND ----------

# def format_historic_date(c):
#   c = c.replace('\s*', '')  
#   if  re.match('(\d*(-|_)*(S|s)\d+(-|_)*\d*)+', c) and re.match('\d+(-|_)+(S|s)\d+', c):
#     c = c.replace('_', '-')
#     c = c.split('-')
#     c = f'{c[1]}-{c[0]}'
#   return c


# def format_historic_date(c):
#   c = c.replace(' ', '')  
#   if  re.match('(\d*(-|_)*(S|s)\d+(-|_)*\d*)+', c):
#     c = c.replace('_', '-')
#     if re.match('\d+(-|_)+(S|s)\d+', c):
#       c = c.split('-')
#       c = f'{c[1]}-{c[0]}'
#   return c

# udf_format_historic_date = spark.udf.register("udf_format_historic_date",format_historic_date, StringType())

def format_historic_date(c):
  c = c.replace(' ', '')  
  if  re.match('(\d*(-|_)*(S|s)\d+(-|_)*\d*)+', c):
    c = c.replace('_', '-')
    if re.match('\d+(-|_)+(S|s)\d+', c):
      c = c.split('-')
      c = f'{c[1]}-{c[0]}'
  if re.search('S', c):
    week = c.split('-')[0]
    week = re.sub('S','', week)
    week = week.rjust(2,'0')
    week = 'S'+ week
    year = c.split('-')[1]
    c = f'{week}-{year}'
  return c

udf_format_historic_date = spark.udf.register("udf_format_historic_date",format_historic_date, StringType())

# COMMAND ----------

def clean_values(name):
  name = str(name)
  if name:
    name = unidecode.unidecode(name).strip().upper()
  if name == 'NAN' or name == 'nan' or name == 'NAT' or name == 'NONE' or name == '':
    name = None
  return name
udf_clean_values = spark.udf.register("udf_clean_values",clean_values, StringType())

# COMMAND ----------

def numeric_format(num):
  '''
  Elimina espacios de los números y cambia la coma por el punto. Se devuelve tipo string para poder castear en Int o Float
  '''
  num = str(num)
  num = num.replace(',', '.').replace(' ','')
  return num
udf_numeric_format = spark.udf.register("udf_numeric_format",numeric_format, StringType())

# COMMAND ----------

def round_num(num):
  '''
  Redondea la columna a dos decimales. Se utiliza cuando vienen mezclados números con strings.
  '''
  try:
    num = float(num.replace(',', '.').replace(' ',''))
    num = "%.2f" %(num)
  except:
    pass
  return num
udf_round_num = spark.udf.register("udf_round_num",round_num, StringType())

# COMMAND ----------

def round_num_2(num):
  '''
  Redondea la columna a dos decimales. Se utiliza cuando vienen mezclados números con strings.
  '''
  try:
    num = float(num.replace(',', '.').replace(' ',''))
    num = "%.5f" %(num)
  except:
    pass
  return num
udf_round_num_2 = spark.udf.register("udf_round_num_2", round_num_2, StringType())

# COMMAND ----------

def check_date_format(date):
  '''
  Homogeneizar el formato de tipo fecha. Debe entrar un string  --> limpiar la columna con udf_clean_values para luego pasarle esta función
  '''
  try:
    date_aux = datetime.fromtimestamp((float(date)-25569)*86400.0)
  except:
    if date is None:
      date_aux = date_default
    else:
      date = str(date)
      date_aux = re.findall('(\d+)(-|\/)(\d+)(-|\/)(\d+)', date)
      if len(date_aux[0][0]) == 4:
        day = date_aux[0][4]
        month =  date_aux[0][2]
        year = date_aux[0][0]
        date_aux = year + '-' + month + '-' + day
      elif len(date_aux[0][0]) == 2:
        day = date_aux[0][0]
        month =  date_aux[0][2]
        year = date_aux[0][4]
        date_aux = year + '-' + month + '-' + day
    date_aux = datetime.strptime(date_aux, '%Y-%m-%d')
  return date_aux

udf_check_date_format = spark.udf.register("udf_check_date_format",check_date_format, TimestampType())

# COMMAND ----------

def check_time_format(time):
  try:
    time = re.sub(',', '.', time)
    time_aux = datetime.fromtimestamp((float(time)-25569)*86400.0)
  except:
    time = re.sub('^24', '00', time)
    time = re.sub('^\d+-\d+-\d+ ', '', time)
    time = re.sub('\.\d+$', '', time)
    time = time+':00' if len(time)<6 else time
    time_aux = datetime.strptime(time, '%H:%M:%S')
  time_aux = re.findall('(\d+:\d+:\d+)$', str(time_aux))[0]
  return time_aux

udf_check_time_format = spark.udf.register("udf_check_time_format", check_time_format, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Get dates

# COMMAND ----------

def  df_min_max_fecha(df, column_name):
  min_fecha = df.agg({column_name: 'min'}).collect()[0][0]
  max_fecha = df.agg({ column_name: 'max'}).collect()[0][0]
  print(f'Getting range date from df min=[{min_fecha}] and max=[{max_fecha}]' )
  return min_fecha, max_fecha

# COMMAND ----------

def day_of_year(pk_fecha):
  day = int(pk_fecha.strftime('%j'))
  return day
udf_day_of_year = spark.udf.register("udf_day_of_year",day_of_year, IntegerType())

# COMMAND ----------

def month_of_year(pk_fecha):
  pk_fecha = pk_fecha.upper()
  month_trans = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
  month = month_trans.index(pk_fecha) + 1
  return month 
udf_month_of_year = spark.udf.register("udf_month_of_year",month_of_year, StringType())

# COMMAND ----------

def monthConvert(monthstring):
  '''
  Dictionary with equivalences between month name and month number
  '''
  switcher={
    'ENE':'01',
    'FEB':'02',
    'MAR':'03',
    'ABR':'04',
    'MAY':'05',
    'JUN':'06',
    'JUL':'07',
    'AGO':'08',
    'SEPT':'09',
    'OCT':'10',
    'NOV':'11',
    'DIC':'12'
     }
  return switcher.get(monthstring,"Mes invalido")

def monthyear_to_numeric(monthyear):
  '''
  Converter to anyomes: 'OCT.-2019' to '201910'
  '''
  if monthyear is not None:
    monthstring = monthyear.partition('.-')[0]
    year = monthyear.partition('.-')[2]
    monthnum = monthConvert(monthstring)
    yearmonth = str(year) + str(monthnum)
    return yearmonth
  else:
    return monthyear

udf_monthyear_to_numeric = spark.udf.register("udf_monthyear_to_numeric",monthyear_to_numeric, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Fill values

# COMMAND ----------

def fill_columns(df, schema_val = {}):
  '''
    function to complete and homogenize the columns in a dataframe 
    df -> dataframe
    schema_val -> schema dictionary to homogenize df {'columna':'type'}
  '''
  cols_num = ['int','float','double','decimal(10,2)', 'decimal(10,3)','decimal(10,4)', 'decimal(5,2)', 'decimal(5,3)']
  default_val = {'string':None, 'int':None, 'float':None, 'date':'1600-01-01', 'double':None, 'decimal(10,2)':None, 'decimal(10,4)': None, 'decimal(5,2)': None, 'decimal(5,3)': None, 'decimal(10,3)': None} 
  if schema_val:
    df_cols = df.columns
    for col_name, col_type in schema_val.items():
      val = default_val[col_type]
      if col_name in df_cols:
        if  (col_type == 'date'):
          df = df.withColumn(col_name, udf_check_date_format(col_name).cast(col_type))
        elif col_type in cols_num:
          df = df.withColumn(col_name, udf_numeric_format(col_name).cast(col_type))
        else:  
          df = df.withColumn(col_name, col(col_name).cast(col_type))
      else:
        df= df.withColumn(col_name, lit(val).cast(col_type))
  else:
    print('Schema is empty')
  return df

# COMMAND ----------

def get_daily_value(df, column_days ,columns):
  """
    df -> Receive df and split weekly amount in dayly amount using work days
    columns, column_days -> array with columns to change
  """
  cols = df.columns
  for column in columns:
    if column in cols:
      df = df.withColumn(column, col(column)/col(column_days))
    else:
      print(f'Missing column => [{column}] IS NOT IN DF')
  return df

# COMMAND ----------

def get_daily_value_per_year(df, columns):
  """
    df -> Receive df and split weekly amount in dayly amount using work days
    columns, column_days -> array with columns to change
  """
  cols = df.columns
  for column in columns:
    if column in cols:
      df = df.withColumn(column, when((col('MES_SEMANA_INICIO') == 12) & (col('MES_SEMANA_FIN')==1), col(column)/col('DIAS_LABORABLES')).otherwise(col(column)/col('DIAS_LABORABLES_ACC')))
    else:
      print(f'Missing column => [{column}] IS NOT IN DF')
  return df

# COMMAND ----------

def df_drop_columns(df, final_columns):
  drop_c = []
  for name in df.columns:
    if name not in final_columns:
      drop_c.append(name)
  
  df = df.drop(*drop_c)
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Hashes

# COMMAND ----------

def get_hashes(df):
  df = df.select('BK_PK_HASH', 'VALUE_HASH').collect() 
  bk_pk_hash, value_hash =[], []
  for row in df:  bk_pk_hash.append(row.BK_PK_HASH);value_hash.append(row.VALUE_HASH);
  return list_to_str(bk_pk_hash), list_to_str(value_hash)

def list_to_str(list_):
  return str(list_).replace('[','').replace(']','')

def list_partition(list_, max_len= 10000):
  return [list_[x:x+max_len] for x in range(0, len(list_), max_len)]

def handler_hashes(df,   max_len = 10000):
  df = df.select('BK_PK_HASH', 'VALUE_HASH').collect() 
  bk_pk_hash, value_hash =[], []
  for row in df:  bk_pk_hash.append(row.BK_PK_HASH);value_hash.append(row.VALUE_HASH);
  len_pk, len_val = len(bk_pk_hash), len(value_hash)
  print(f'Hash values bk_pk_hash_len={len_pk} value_hash_val={len_val}')
  if len_pk > max_len :
    bk_pk_hash, value_hash = list_partition(bk_pk_hash, max_len), list_partition(value_hash, max_len)
    print(f'Num of partition pk_temp={len(bk_pk_hash)}, value_temp={len(value_hash)}')
    bk_pk_hash = [list_to_str(pk) for pk in bk_pk_hash ]
    value_hash =  [list_to_str(pk) for pk in value_hash ]
  else:
    bk_pk_hash = [ list_to_str(bk_pk_hash)]
    value_hash =  [list_to_str(value_hash)]
    print("no creo chuncks")
  
  return bk_pk_hash, value_hash

def handler_pk(df, pk, max_len = 10000):
  pk_values = {}
  cols_pk, pk = pk, pk[0]
  
  # get pk 
  df = df.select(*cols_pk).distinct()
  for col in cols_pk:  pk_values[col] = list(map( lambda x: list_to_str(x), list_partition(df.select(col).rdd.flatMap(lambda x: x).collect(), max_len)));
  return pk_values

# COMMAND ----------

def truncate_hash(row):
  return row[:19]
udf_truncate_hash = spark.udf.register("udf_truncate_hash",truncate_hash, StringType())

# COMMAND ----------

def convert_md5_hex_hash_to_big_int(row):
    md5_hash = hashlib.md5(str(row).encode("utf-8"))
    hex_hash = md5_hash.hexdigest()
    hex_hash_int= int(hex_hash, 16)
    # hash_hex_val is 32 hex characters long = 128 bit
    # hex_val_as_int is still 128 bits in size, but it's now an int
    
    lower_limit_big_int = -2**63
    upper_limit_big_int = 2**63 - 1
    range_64 = upper_limit_big_int - lower_limit_big_int
    range_128 = 2**128
    
    #  scale 128bit to 64bit
    factor = hex_hash_int / range_128
    
    my_big_int = int(factor * range_64 + lower_limit_big_int)
    if my_big_int > upper_limit_big_int: my_big_int = upper_limit_big_int
    return my_big_int
    

udf_convert_md5_hex_hash_to_big_int = spark.udf.register("udf_convert_md5_hex_hash_to_big_int",convert_md5_hex_hash_to_big_int, LongType())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Joins

# COMMAND ----------

def condition_generator(primary_key):
  condition = []
  for pk in primary_key:
    condition.append(F' src.{pk}=dst.{pk} ')
  return 'AND'.join(condition)

# COMMAND ----------

def check_new_data_df(df, df_dwh, pk):
  condition = condition_generator(pk)
  df_to_add = df.alias('src').join(df_dwh.alias('dst'), expr(condition), how = 'leftanti')
  return df_to_add

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Collect

# COMMAND ----------

def get_values_column (df, col):
  column = df.select(col).collect() 
  arr = []
  for row in column: arr.append(row[col])
  arr = list(set(arr))
  return arr

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Time

# COMMAND ----------

# UDF
def generate_date_series(start, stop):
  return [start + timedelta(days=x) for x in range(0, (stop-start).days + 1)]    

# Register UDF for later usage
spark.udf.register("generate_date_series", generate_date_series, ArrayType(DateType()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Files

# COMMAND ----------

def get_all_files(path):
  files=[file for file in os.listdir(path)]
  return files

# COMMAND ----------

def get_year(row):
  if re.search('(\d\d\d\d)', row):
    row = re.findall('(\d\d\d\d)', row)[0]
  else:
    row = str(date.today().year)
  return row

udf_get_year = spark.udf.register("udf_get_year", get_year, StringType())


def get_month(row):
  row = row.upper()
  row = re.sub('INFORME', '', row)
  row = re.sub('EXPEDI', '', row)

  if re.search('EN', row):
    month = '01'
  elif re.search('FE', row):
    month = '02'
  elif re.search('MAR', row):
    month = '03'
  elif re.search('AB', row):
    month = '04'
  elif re.search('MA', row):
    month = '05'
  elif re.search('JUN', row):
    month = '06'
  elif re.search('JUL', row):
    month = '07'
  elif re.search('AG', row):
    month = '08'
  elif re.search('SEP', row):
    month = '09'
  elif re.search('OCT', row):
    month = '10'
  elif re.search('NOV', row):
    month = '11'
  elif re.search('DIC', row):
    month = '12'
  else:
    pass
  return month


def get_first_day(row):
  row = row.upper()
  try:
    day = re.findall('(\s|-)(\d+)(\s|-)', row)
    day = day[-2][1].strip()
  except:
    day =  '01'
  finally:
    day = day.rjust(2, '0')
  return day


def get_last_day(row):
  row = row.upper()
  try:
    day = re.findall('(\s|-)(\d+)(\s|-)', row)
    day = day[0][1].strip()
  except:
    day =  '31'
  finally:
    day = day.rjust(2, '0')
  return day

def get_order_expediciones(row):
  row = row.split('/')[-1]
  year = get_year(row)
  month = get_month(row)
  first_day = get_first_day(row)
  last_day = get_last_day(row)
  order = year+month+last_day+first_day
  return order
udf_get_order_expediciones = spark.udf.register("udf_get_order_expediciones", get_order_expediciones, StringType())

# COMMAND ----------

def get_date(row):
  days = list(range(1,32))
  days = [str(day).rjust(2, '0') for day in days]
  row = row.upper()
  row = re.sub('REPARTO', ' ', row)
  row = re.sub(' DE ', ' ', row)
  row = re.sub('TRANSPORTE', ' ', row)
  row = re.sub('COSTES', '', row)
  row = re.sub(' EN ', ' ', row)
  row = re.sub(' DEL ', ' ', row)
  row = re.sub(' AL ', ' ', row)
  row = re.sub('-', ' ', row)
  row = re.sub('.TXT', ' ', row)
  row = row.strip()
  
  if re.search('(F\d+)', row):
    f_type = re.findall('(F\d+)', row)[0]
    row = re.sub(f_type, ' ', row)
  else:
    f_type = ''
  
  
  for i in [1,2]:
    print('i', i)
    print(row)
    if re.search('EN', row):
      month = '01'
      row = re.sub('EN', '', row)
    elif re.search('FE', row):
      month = '02'
      row = re.sub('FE', '', row)
    elif re.search('MAR', row):
      month = '03'
      row = re.sub('MAR', '', row)
    elif re.search('AB', row):
      month = '04'
      row = re.sub('AB', '', row)
    elif re.search('MAY', row):
      month = '05'
      row = re.sub('MAY', '', row)
    elif re.search('JUN', row):
      month = '06'
      row = re.sub('JUN', '', row)
    elif re.search('JUL', row):
      month = '07'
      row = re.sub('JUL', '', row)
    elif re.search('AG', row):
      month = '08'
      row = re.sub('AG', '', row)
    elif re.search('SEP', row):
      month = '09'
      row = re.sub('SEP', '', row)
    elif re.search('OCT', row):
      month = '10'
      row = re.sub('OCT', '', row)
    elif re.search('NOV', row):
      month = '11'
      row = re.sub('NOV', '', row)
    elif re.search('DIC', row):
      month = '12'
      row = re.sub('DIC', '', row)
    else:
      pass
    
    try:
      day = re.findall('(\d+)', row)
      day = day[0].strip()
      row = re.sub(day,'',row)
      day = day.rjust(2, '0')

    except:
      day = row
      
    if i==1:
      month_1 = month
      day_1 = '01' if day not in days else day
      print(month_1,day_1)
    else:
      month_2 = month
      day_2 = '31' if day not in days else day
      print(month_2,day_2)
    row = row.strip()
  
  row = month_2+day_2+month_1+day_1+f_type
  return row

def get_order_reparto_costes_agrupado(row):
  row = row.split('/')[-1]
  year = get_year(row)
  date = get_date(row)
  
  order = year+date
  return order
udf_get_order_reparto_costes_agrupado = spark.udf.register("udf_get_order_reparto_costes_agrupado", get_order_reparto_costes_agrupado, StringType())

# COMMAND ----------

def get_order_zonas(row):
  row = row.split('/')[-1]
  year = get_year(row)
  row = re.sub('_01.', 'ENERO.', row)
  row = re.sub('_02.', 'FEBRERO.', row)
  row = re.sub('_03.', 'MARZO.', row)
  row = re.sub('_04.', 'ABRIL.', row)
  row = re.sub('_05.', 'MAYO.', row)
  row = re.sub('_06.', 'JUNIO.', row)
  row = re.sub('_07.', 'JULIO.', row)
  row = re.sub('_08.', 'AGOSTO.', row)
  row = re.sub('_09.', 'SEPT.', row)
  row = re.sub('_10.', 'OCT.', row)
  row = re.sub('_11.', 'NOV.', row)
  row = re.sub('_12.', 'DIC.', row)
  date = get_date(row)
  order = year+date
  return order
udf_get_order_zonas = spark.udf.register("udf_get_order_zonas", get_order_zonas, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Save data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta lake

# COMMAND ----------

def save_delta_lake(df, process, mode, overwrite_schema="false",  partition=""):
  """
    partition = "PK_FECHA" por defecto no me realiza particiones 
    overwrite_schema="true" si se quiere remplazar el schema existente  
  """
  
  if partition:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .option('path',process['path'])\
      .partitionBy(partition) \
      .saveAsTable(process['table_mt'])
  else:
    df.write \
      .format("delta") \
      .mode(mode) \
      .option("overwriteSchema", overwrite_schema) \
      .option('path', process['path'])\
      .saveAsTable(process['table_mt'])
    
  print(f"Saved DATA in TABLE=[{process['table_mt']}] LOCATION=[{process['path']}]")
  return None

# COMMAND ----------

# def save_delta_lake(df, table_name, process, schema, mode, overwrite_schema="false",  partition=""):
#   """
#     partition = "PK_FECHA" por defecto no me realiza particiones 
#     overwrite_schema="true" si se quiere remplazar el schema existente  
#   """
  
#   dst_name = table_name.split('.')[1]
#   path = f'/mnt/florette-bi/{process}/{schema}/{dst_name}'
#   print(f'Working table mode=[{mode}] table_mt=[{table_name}] dst_name=[{path}] partition=[{partition}] overwrite_schema=[{overwrite_schema}] path=[{path}]')
  
#   if partition:
#     df.write \
#       .format("delta") \
#       .mode("overwrite") \
#       .option("overwriteSchema", "true") \
#       .option('path',path)\
#       .partitionBy(partition) \
#       .saveAsTable(f"{table_name}")
#   else:
#     df.write \
#       .format("delta") \
#       .mode(mode) \
#       .option("overwriteSchema", overwrite_schema) \
#       .option('path',path)\
#       .saveAsTable(f"{table_name}")
    
#   print(f"Saved DATA in TABLE=[{table_name}] LOCATION=[{path}]")
#   return None

# COMMAND ----------

def save_dl_merge(df, pk, delta_table):
  '''Se utiliza para actualizar los datos y añadir los nuevos'''
  # Upsert deltalake
  cond = condition_generator(pk)
  DeltaTable.forName(spark, delta_table).alias("dst")\
            .merge(source = df.alias("src"), condition = expr(cond)) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
  print(f"Saved DATA in TABLE=[{delta_table}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DWH

# COMMAND ----------

def handler_dwh_data(df, process, mode, min_date, max_date, chunck_len, elemts, repeated_values, row_num):
  try: 
    if repeated_values > 0:
      print(f'''Updating {repeated_values} rows''')
      sql_Server = SQLServer('DWH')  
      bk_pk_hash, value_hash = handler_hashes(df, max_len= chunck_len)
      updated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      
      for chunck_hash in bk_pk_hash:
        print(f'''Working with chunck {len(chunck_hash)}''')
        # Update dwh table
        spark.sql(f'''UPDATE {process['table_mt']} SET  IND_ACTIVO=0, FECHA_ACTUALIZACION='{updated_date}' 
                                      WHERE PK_FECHA >= '{min_date}' AND PK_FECHA <= '{max_date}'
                                      AND BK_PK_HASH IN ({chunck_hash}) 
                                      AND IND_ACTIVO = 1''')
        # Update SQLServer table
        sql_Server.query_to_db(f"""UPDATE {process['table_sql']} SET IND_ACTIVO=0, FECHA_ACTUALIZACION='{updated_date}' 
                                                       WHERE PK_FECHA >= '{min_date}' AND PK_FECHA <= '{max_date}'
                                                       AND BK_PK_HASH IN ({chunck_hash}) 
                                                       AND IND_ACTIVO = 1 """)

        # Saving changes in SQLServer
        sql_Server.cursor.commit()
        print(f'''Updated tables {process['table_mt']} and {process['table_sql']}''')

    # Writing data in SQLServer
    write_df_to_db(df, 'DWH', process['table_sql'], mode)
    print(f"Saved new rows=[{elemts}] in table {process['table_sql']}")

    # Writing table
    save_delta_lake(df, process, mode)
    print(f"Saved new rows=[{elemts}] in table {process['table_mt']}")
    return df

  except:
    sql_Server = SQLServer('DWH')
    sql_Server.query_to_db(f"""DELETE FROM {process['table_sql']} WHERE PK_HASH > {row_num}""")
    sql_Server.cursor.commit()
    if repeated_values > 0:
      # Añadir volver a la versión anterior
      sql_Server.query_to_db(f"""UPDATE {process['table_sql']} SET IND_ACTIVO=1 WHERE FECHA_ACTUALIZACION = '{updated_date}'""")
      sql_Server.cursor.commit()
      spark.sql(f"""UPDATE {process['table_mt']} SET IND_ACTIVO=1 WHERE FECHA_ACTUALIZACION = '{updated_date}'""")
    raise
  
def dwh_data(df, process, mode):
  print(f'elementos de df=[{df.count()}]')

  min_date, max_date = df_min_max_fecha(df, 'PK_FECHA') 
  df_dwh = select_to_db_table('DWH', process['table_sql'])
  row_num = df_dwh.select("PK_HASH").rdd.max()[0]
  df_dwh = df_dwh.filter((col('PK_FECHA') >= min_date) & (col('PK_FECHA') <= max_date))
  
  print(f"SELECT DWH df_dwh=[{df_dwh.count()}] between [{min_date}] and [{max_date}]")
  
  # Validating new rows
  df = check_new_data_df(df, df_dwh, process['pk']).persist()
  elemts = df.count()
  print(f'New elements=[{elemts}]') 
  
  # Validating updating rows
  df_repeated = df.join(df_dwh, 'BK_PK_HASH', how = 'inner')
  repeated_values = df_repeated.count()
  print(f'Repeated elements =[{repeated_values}]')  
  
  if elemts > 0 :
    df.persist()
    df = handler_dwh_data(df, process, mode, min_date, max_date, chunck_len, elemts, repeated_values, row_num)
    df.unpersist()
  else:
    print("There're not new elements")    
  return elemts

# COMMAND ----------

# def handler_dwh_data(df, process, mode, min_date, max_date, chunck_len, elemts, repeated_values):
  
#   if repeated_values > 0:
#     print(f'''Updating {repeated_values} rows''')
#     sql_Server = SQLServer('DWH')  
#     bk_pk_hash, value_hash = handler_hashes(df, max_len= chunck_len)

#     for chunck_hash in bk_pk_hash:
#       print(f'''Working with chunck {len(chunck_hash)}''')
#       # Update dwh table
#       spark.sql(f'''UPDATE {process['table_mt']} SET  IND_ACTIVO=0, FECHA_ACTUALIZACION=CURRENT_TIMESTAMP 
#                                     WHERE PK_FECHA >= '{min_date}' AND PK_FECHA <= '{max_date}'
#                                     AND BK_PK_HASH IN ({chunck_hash}) 
#                                     AND IND_ACTIVO = 1''')
#       # Update SQLServer table
#       sql_Server.query_to_db(f"""UPDATE {process['table_sql']} SET IND_ACTIVO=0, FECHA_ACTUALIZACION=getdate() 
#                                                      WHERE PK_FECHA >= '{min_date}' AND PK_FECHA <= '{max_date}'
#                                                      AND BK_PK_HASH IN ({chunck_hash}) 
#                                                      AND IND_ACTIVO = 1 """)

#       # Saving changes in SQLServer
#       sql_Server.cursor.commit()
#       print(f'''Updated tables {process['table_mt']} and {process['table_sql']}''')
  
#   # Writing data in SQLServer
#   write_df_to_db(df, 'DWH', process['table_sql'], mode)
#   print(f"Saved new rows=[{elemts}] in table {process['table_sql']}")

#   # Writing table
#   save_delta_lake(df, process, mode)
#   print(f"Saved new rows=[{elemts}] in table {process['table_mt']}")

#   return df

# def dwh_data(df, process, mode):
#   print(f'elementos de df=[{df.count()}]')

#   min_date, max_date = df_min_max_fecha(df, 'PK_FECHA') 
  
#   df_dwh = select_to_db_table('DWH', process['table_sql']).filter((col('PK_FECHA') >= min_date) & 
#                                                                   (col('PK_FECHA') <= max_date))
  
#   print(f"SELECT DWH df_dwh=[{df_dwh.count()}] between [{min_date}] and [{max_date}]")
  
#   # Validating new rows
#   df = check_new_data_df(df, df_dwh, process['pk']).persist()
#   elemts = df.count()
#   print(f'New elements=[{elemts}]') 
  
#   # Validating updating rows
#   df_repeated = df.join(df_dwh, 'BK_PK_HASH', how = 'inner')
#   repeated_values = df_repeated.count()
#   print(f'Repeated elements =[{repeated_values}]')  
  
#   if elemts > 0 :
#     df.persist()
#     df = handler_dwh_data(df, process, mode, min_date, max_date, chunck_len, elemts, repeated_values)
#     df.unpersist()
#   else:
#     print("There're not new elements")    
#   return elemts

# COMMAND ----------

def handler_dwh_data_no_date(df, process, mode, chunck_len, elemts, repeated_values, row_num):
  try:
    if repeated_values > 0:
      print(f'''Updating {repeated_values} rows''')
      sql_Server = SQLServer('DWH')  
      bk_pk_hash, value_hash = handler_hashes(df, max_len= chunck_len)
      updated_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      
      for chunck_hash in bk_pk_hash:
        print(f'''Working with chunck {len(chunck_hash)}''')
        # Update dwh table
        spark.sql(f'''UPDATE {process['table_mt']} SET  IND_ACTIVO=0, FECHA_ACTUALIZACION='{updated_date}'  
                                      WHERE BK_PK_HASH IN ({chunck_hash}) 
                                      AND IND_ACTIVO = 1''')
        # Update SQLServer table
        sql_Server.query_to_db(f"""UPDATE {process['table_sql']} SET IND_ACTIVO=0, FECHA_ACTUALIZACION='{updated_date}'
                                                       WHERE BK_PK_HASH IN ({chunck_hash}) 
                                                       AND IND_ACTIVO = 1 """)

        # Saving changes in SQLServer
        sql_Server.cursor.commit()
        print(f'''Updated tables {process['table_mt']} and {process['table_sql']}''')

    # Writing data in SQLServer
    write_df_to_db(df, 'DWH', process['table_sql'], mode)
    print(f"Saved new rows=[{elemts}] in table {process['table_sql']}")

    # Writing table
    save_delta_lake(df, process, mode)
    print(f"Saved elements=[{elemts}] in table {process['table_mt']}]")
  
  except:
    sql_Server.query_to_db(f"""DELETE FROM {process['table_sql']} WHERE PK_HASH > {row_num}""")
    sql_Server.cursor.commit()
    if repeated_values > 0:
      # Añadir volver a la versión anterior
      sql_Server.query_to_db(f"""UPDATE {process['table_sql']} SET IND_ACTIVO=1 WHERE FECHA_ACTUALIZACION = '{updated_date}'""")
      sql_Server.cursor.commit()
      spark.sql(f"""UPDATE {process['table_mt']} SET IND_ACTIVO=1 WHERE FECHA_ACTUALIZACION = '{updated_date}'""")
    raise

  return df

def dwh_data_no_date(df, process, mode):
  print(f'elementos de df=[{df.count()}]')
  
  df_dwh = select_to_db_table('DWH', process['table_sql'])
  row_num = df_dwh.select("PK_HASH").rdd.max()[0]
  print(f"SELECT DWH df_dwh=[{df_dwh.count()}]")

  # Validating new rows
  df = check_new_data_df(df, df_dwh, process['pk']).persist()
  elemts = df.count()
  print(f'New elements=[{elemts}]') 
  
  # Validating updating rows
  df_repeated = df.join(df_dwh, 'BK_PK_HASH', how = 'inner')
  repeated_values = df_repeated.count()
  print(f'Repeated elements =[{repeated_values}]') 
  
  if elemts > 0 :
    df.persist()    
    df = handler_dwh_data_no_date(df, process, mode, chunck_len, elemts, repeated_values, row_num)
    df.unpersist()
  else:
    print("There're not new elements")    
  return elemts

# COMMAND ----------

# def handler_dwh_data(df, dwh_mt, dwh_sql, mode, pk_unique, from_date, process, schema, chunck_len, elemts, repeated_values):
#   if repeated_values > 0:
#     print(f'''Updating {repeated_values} rows''')
#     sql_Server = SQLServer('DWH')  
#     bk_pk_hash, value_hash = handler_hashes(df,   max_len= chunck_len)
#   #   pk_hash, value_hash = get_hashes(df,   max_len= chunck_len)

#     for chunck_hash in bk_pk_hash:
#       print(f'''Working with chunck {len(chunck_hash)}''')
#       # Update dwh table
#       spark.sql(f'''UPDATE {dwh_mt} SET  IND_ACTIVO=0, FECHA_ACTUALIZACION=CURRENT_TIMESTAMP 
#                                     WHERE PK_FECHA >= '{min_date}' AND PK_FECHA <= '{max_date}'
#                                     AND BK_PK_HASH IN ({chunck_hash}) 
#                                     AND IND_ACTIVO = 1''')
#       # Update SQLServer table
#       sql_Server.query_to_db(f"""UPDATE {dwh_sql} SET IND_ACTIVO=0, FECHA_ACTUALIZACION=getdate() 
#                                                      WHERE PK_FECHA >= '{min_date}' AND PK_FECHA <= '{max_date}'
#                                                      AND BK_PK_HASH IN ({chunck_hash}) 
#                                                      AND IND_ACTIVO = 1 """)

#       # Saving changes in SQLServer
#       sql_Server.cursor.commit()
#       print(f'''Updated tables {dwh_mt} and {dwh_sql}''')
      
#   # Writing data in SQLServer
#   write_df_to_db(df, 'DWH', dwh_sql, mode)
#   print(f"Saved new rows=[{elemts}] in table {dwh_sql}")

#   # Writing table
#   save_delta_lake(df, dwh_mt, process, schema, mode)
#   print(f"Saved elements=[{elemts}] in table {dwh_mt} from_date=[{from_date}]")

#   return df

# def dwh_data(df, dwh_mt, dwh_sql, mode, pk_unique, from_date, process, schema):
#   print(f'dwh_data elementos de df=[{df.count()}]')

#   min_date, max_date = df_min_max_fecha(df, 'PK_FECHA') 
  
# #   df_dwh = spark.table(dwh_mt)\
# #                  .filter((f.year(col('PK_FECHA')) == year) & 
# #                                  (col('PK_FECHA') >= min_date) & (col('PK_FECHA') <= max_date) & 
# #                                  (col('IND_ACTIVO') == 1))
  
#   df_dwh = select_to_db_table('DWH', dwh_mt).filter((col('PK_FECHA') >= min_date)
#                                                     & (col('PK_FECHA') <= max_date) & 
#                                                     (col('IND_ACTIVO') == 1))
  
#   print(f"SELECT DWH df_dwh=[{df_dwh.count()}] year=[{year}] between [{min_date}] and [{max_date}]")
  
#   # Validating new rows
#   df = check_new_data_df(df, df_dwh, pk_unique)
#   elemts = df.count()
#   print(f'New elements=[{elemts}]') 
  
#   # Validating updating rows
#   df_repeated = check_new_data_df(df, df_dwh, ['BK_PK_HASH'])
#   repeated_values = df_repeated.count() - elemts
#   print(f'Repeated elements =[{repeated_values}]') 
  
#   if elemts > 0 :
#     df.persist()    
#     df = handler_dwh_data(df, dwh_mt, dwh_sql, mode, pk_unique, from_date, process, schema, chunck_len, elemts, repeated_values)
#     df.unpersist()
#   else:
#     print("There're not new elements")    
#   return df

# COMMAND ----------

# def dwh_data_dim(df, pk, delta_table, sql_table):
#   '''Se utiliza para actualizar los datos y sobreescribir en la bbdd'''
#   # Upsert deltalake
#   cond = condition_generator(pk)
#   DeltaTable.forName(spark, delta_table).alias("dst")\
#             .merge(source = df.alias("src"), condition = expr(cond)) \
#             .whenMatchedUpdateAll() \
#             .whenNotMatchedInsertAll() \
#             .execute()
#   print(f'''Update + Insert in table {delta_table}''')
    
#   #Sobreescribir bbdd
#   df_dwh = spark.table(delta_table)
#   write_df_to_db(df_dwh, 'DWH', sql_table, mode_overwrite)
#   print(f'''Overwrite table {sql_table}''')
#   return None

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Exception

# COMMAND ----------

# DBTITLE 1,handler_exception
def handler_exception(func_name, e = None):
  if e:
    if hasattr(e, 'message'):
      func_name = f'PROCESO OK FUNCTION:{func_name} ERROR:[{e.message}]'
      print(e.message)
    else:
      print(e)
    func_name = f'SE HA PRODUCIDO UN ERROR IN FUNCTION:{func_name} ERROR:[{e.message}]'
    raise
  else:
    func_name = f'PROCESO OK FUNCTION:{func_name}'
  print(func_name)  
  return func_name

# COMMAND ----------

def get_notebook_name():
  name = sc.broadcast(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]).value
  process = name.split()
  return name

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest

# COMMAND ----------

def extract_path(row):
  path = '/'.join(row.split('/')[:-1])
  return path
udf_extract_path = spark.udf.register("udf_extract_path",extract_path, StringType())

# COMMAND ----------

def extract_nickname(row):
  row = re.sub('_', '', row)
  row = re.sub(' ', '', row)
  row = re.sub('-', '', row)
  row = unidecode.unidecode(row).upper()
  return row
udf_extract_nickname = spark.udf.register("udf_extract_nickname",extract_nickname, StringType())

# COMMAND ----------

def set_route_config_file(config_file):
  config_path = '/mnt/tfm-recsys/0-Ingest/config/'
  config_file = config_path + config_file if (config_path not in config_file) else config_file
  return config_file

# COMMAND ----------

def ingest_new_data(path):
  try:
    container = 'florette-bi'
    name_starts_with = '0-Ingest/'+'/'.join(path.split('/')[3:])
    container_client = blob_service_client.get_container_client(container)
    columns = ['NAME', 'LAST_MODIFIED']
    arr =[] 
    
    for i, blob in enumerate (container_client.list_blobs(name_starts_with=name_starts_with)):
      arr.append((blob.name, blob.last_modified))
    
    if len(arr) == 0:
      df = None
    else:
      df = spark.createDataFrame(arr, columns) # Create df
      df = df.filter(df['NAME'].rlike('\.[A-Za-z]+$')) # Remove folders

      df = df.withColumn('FILE', reverse(split(df['NAME'],'\/'))[0]) \
             .withColumn('PATH', udf_extract_path('NAME')) \
             .withColumn('NICKNAME', udf_extract_nickname('FILE')) \
             .withColumn('CONTAINER', lit(container))
  
    return df
    handler_exception(table)
  except Exception as e:
    df = None
    handler_exception(table, e)

# COMMAND ----------

def client_new_data(path):
  try:
    container = path.split('/')[3]
    name_starts_with = '/'.join(path.split('/')[4:])
    container_client = blob_service_client.get_container_client(container)
    columns = ['NAME', 'LAST_MODIFIED']
    arr = []
  
    for i, blob in enumerate (container_client.list_blobs(name_starts_with=name_starts_with)):
      arr.append((blob.name, blob.last_modified))

    df = spark.createDataFrame(arr, columns) # Create df
    
    df = df.filter(df['NAME'].rlike('\.[A-Za-z]+$')) # Remove folders
    
    df = df.withColumn('FILE', reverse(split(df['NAME'],'\/'))[0]) \
           .withColumn('PATH', udf_extract_path('NAME')) \
           .withColumn('NICKNAME', udf_extract_nickname('FILE')) \
           .withColumn('CONTAINER', lit(container))

    return df
    handler_exception(table)
  except Exception as e:
    df = None
    handler_exception(table, e)

# COMMAND ----------

def copy_files(source_container_name, source_file_path):
  # Source
  source_blob = blob_service_client.get_blob_client(source_container_name, source_file_path).url

  # Target
  target_file_path = f'0-Ingest/{source_container_name}/{source_file_path}'
  copied_blob = blob_service_client.get_blob_client(target_container_name, target_file_path)
  
  # Copy files
  copied_blob.start_copy_from_url(source_blob)
  props = copied_blob.get_blob_properties()
  return props.copy.status

# COMMAND ----------

def delete_extract_tables(process):
  schema = process['path'].split('/')[-2]
  table_name = process['path'].split('/')[-1]
  delta_table = DeltaTable.forPath(spark, f'/mnt/florette-bi/{process_ext}/{schema}/{table_name}')
  delta_table.delete()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Schedule Orchestrator

# COMMAND ----------

def get_batch_from_config(config_file):
  cnf =json.loads(' '.join(dbutils.fs.head(config_file).split()))
  batch = cnf['global_vars']['batch_process']
  return batch

def get_log_dependency(table_name):
  df = select_to_db_table('STG', '[log].[LOG_DEPENDENCY]').filter(col('TableProcessName').contains(table_name)) \
                      .withColumn("dependency", f.to_json(f.struct("TableProcessName","ParentTableName", "ConfigFile"))) \
                      .select("dependency") \
                      .collect()
  return df

def get_dependency_tables(table_name):
  
  df = get_log_dependency(table_name)
  
  if len(df) == 0:
    #Añadir dependencias
    try:
      all_parents = process['extract_mt'] + process['support_tables'] if process['support_tables'] else process['extract_mt']
      for parent in all_parents:
        process['config_file'] = parameter
        process['config_file_id'] = get_config_file(process)
        process['table_ID'] = get_table_processID(process)
        process['parent'] = parent
        set_table_depency(process)
    except:
      process['config_file'] = parameter
      process['config_file_id'] = get_config_file(process)
      process['table_ID'] = get_table_processID(process)
      set_table_depency(process)
    finally:
      df = get_log_dependency(table_name)

  arr_parent = []
  for row in df:
    row = eval(row[0])
    row['Batch'] = get_batch_from_config(row['ConfigFile'])
    arr_parent.append(row)
  return arr_parent

def get_parent_status(df_log_overview, array_parents):
  window = Window.partitionBy('TableProcessName').orderBy(col('Batch').desc(), col('StartDate').desc())
  
  df = spark.createDataFrame(Row(**x) for x in array_parents)\
            .withColumn('filter', concat(lit('((col("TableProcessName") == '), lit('"'), 
                                         col('ParentTableName'), lit('"') ,lit(') & '), 
                                         lit('(col("Batch") == '), lit('"'), 
                                         col('Batch'), lit('"'), lit('))')))
  
  # creating filters
  filter_exp = '|'.join(get_values_column(df, 'filter'))
  df = df_log_overview.filter(eval(filter_exp))
  df = df.withColumn('row_number', row_number().over(window)).filter(col('row_number')==1)
  
  status_parent = (get_values_column(df, 'Status'))
  status_parent = False if (('ERROR' in status_parent) or not (status_parent)) else True
  return status_parent

def get_execution(business_unit, parameters, parallel = None):
  if parallel != None:
    df = select_to_db_table('STG', config_table_sql).filter((col('Active') == 1) & (col('BusinessUnit') == business_unit) & (col('Parallel') == parallel)) \
                                                    .filter(col('ConfigPath').isin(parameters)) \
                                                    .orderBy('OrderExecution')
  else:
    df = select_to_db_table('STG', config_table_sql).filter((col('Active') == 1) & (col('BusinessUnit') == business_unit)) \
                                                    .filter(col('ConfigPath').isin(parameters)) \
                                                    .orderBy('OrderExecution')
  return get_values_column (df, 'ConfigPath')

# COMMAND ----------

# WITH LOG_OVERVIEW_WITH_ROW AS
# (SELECT *, 
# ROW_NUMBER() OVER (PARTITION BY ParentTableName, Batch ORDER BY StartDate DESC) AS 'RowNumber' 
# FROM STG.log.LOG_OVERVIEW)
# SELECT * FROM LOG_OVERVIEW_WITH_ROW WHERE RowNumber = 1;