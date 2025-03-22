# Databricks notebook source
bronze_folder_path = "/mnt/stmetrodoralakehousedev/bronze"
silver_folder_path = "/mnt/stmetrodoralakehousedev/silver"
gold_folder_path = "/mnt/stmetrodoralakehousedev/gold"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Funcion Flatten

# COMMAND ----------

def flatten(df):
   #compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if isinstance(field.dataType, (ArrayType, StructType))])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      #if StructType then convert all sub element to columns.
      #i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      #if ArrayType then add the Array Elements as Rows using the explode function
      #i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
    
      #recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

# COMMAND ----------

# MAGIC %md
# MAGIC Fecha de hoy para tomar los datos en la carpeta con la descarga diaria

# COMMAND ----------

current_date = spark.sql("SELECT date_format(CURRENT_DATE(),'yyyy/MM/dd')").collect()[0][0]
