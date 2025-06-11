# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_lakehouse.entidad_legal (
# MAGIC     entidad_legal STRING,
# MAGIC     codigo STRING,
# MAGIC     institucion STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

import pandas as pd

data = {
    "entidad_legal": [
        "CESIF", "UNIVERSANIDAD", "OCEANO", "ISEP", "METRODORA LEARNING", "TROPOS", "PLAN EIR", "IEPP", "CIEP",
        "METRODORA FP", "SAIUS", "ENTI", "METRODORAFP ALBACETE", "METRODORAFP AYALA", "METRODORAFP CÁMARA",
        "METRODORAFP EUSES", "OCEANO EXPO-ZARAGOZA", "OCEANO PORCHES-ZARAGOZA", "METRODORAFP GIJÓN",
        "METRODORAFP LOGROÑO", "METRODORAFP SANTANDER", "METRODORAFP VALLADOLID", "METRODORAFP MADRID-RIO"
    ],
    "codigo": [
        "CF", "US", "OC", "IP", "ML", "TP", "PE", "IE", "CI", "MF", "SA", "EN", "AB", "AY", "CA", "EU", "EX", "PO",
        "GI", "LO", "ST", "VL", "RI"
    ],
    "institucion": [
        "CESIF", "UNIVERSANIDAD", "OCEANO", "ISEP", "METRODORA LEARNING", "TROPOS", "PLAN EIR", "IEPP", "CIEP",
        "METRODORA FP", "SAIUS", "ENTI", "METRODORA FP", "METRODORA FP", "METRODORA FP", "METRODORA FP", "OCEANO",
        "OCEANO", "METRODORA FP", "METRODORA FP", "METRODORA FP", "METRODORA FP", "METRODORA FP"
    ]
}

df = spark.createDataFrame(pd.DataFrame(data))
df.createOrReplaceTempView("df_entidades")

# COMMAND ----------

# DBTITLE 1,Merge
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.entidad_legal AS target
# MAGIC USING df_entidades AS source
# MAGIC ON target.codigo = source.codigo
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# MAGIC %sql select * from silver_lakehouse.entidad_legal;
