# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Leer el Parquet mal formateado
df_raw = spark.read.parquet(f"{bronze_folder_path}/lakehouse/budget_ff/Budget_FF")

# 2. Extraer la primera fila (que contiene los nombres correctos)
first_row = df_raw.limit(1).collect()[0]
new_column_names = [str(col).strip() for col in first_row]

# 3. Crear una columna temporal con row_number para filtrar la primera fila
windowSpec = Window.orderBy(F.monotonically_increasing_id())
df_with_row_num = df_raw.withColumn("row_num", F.row_number().over(windowSpec))

# 4. Filtrar para quitar la primera fila
df_clean_data = df_with_row_num.filter("row_num > 1").drop("row_num")

# 5. Renombrar columnas
for old_name, new_name in zip(df_clean_data.columns, new_column_names):
    df_clean_data = df_clean_data.withColumnRenamed(old_name, new_name)

# 7. Seleccionar columnas necesarias
df_final = df_clean_data.select(
    "fecha",
    "escenario",
    "producto",
    "numLeadsNetos",
    "numLeadsBrutos",
    "numMatriculas",
    "importeVentaNeta",
    "importeCaptacion"
)

# 8. Mostrar solo las columnas requeridas
display(df_final)

# COMMAND ----------

df_final.createOrReplaceTempView("budget_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select fecha as fecha
# MAGIC       ,escenario as escenario
# MAGIC       ,producto as producto
# MAGIC       ,numLeadsNetos as num_Leads_Netos
# MAGIC       ,numLeadsBrutos as num_Leads_Brutos
# MAGIC       ,numMatriculas as num_Matriculas
# MAGIC       ,importeVentaNeta as importe_Venta_Neta
# MAGIC       ,importeCaptacion as importe_Captacion
# MAGIC   FROM budget_source_view;

# COMMAND ----------

# MAGIC %sql delete from silver_lakehouse.budget_ff

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.budget_ff AS target
# MAGIC USING budget_source_view AS source
# MAGIC   ON  target.fecha = source.fecha
# MAGIC  AND target.escenario = source.escenario
# MAGIC  AND target.producto = source.producto
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        target.num_Leads_Netos    IS DISTINCT FROM source.numLeadsNetos
# MAGIC     OR target.num_Leads_Brutos   IS DISTINCT FROM source.numLeadsBrutos
# MAGIC     OR target.num_Matriculas     IS DISTINCT FROM source.numMatriculas
# MAGIC     OR target.importe_Venta_Neta IS DISTINCT FROM source.importeVentaNeta
# MAGIC     OR target.importe_Captacion  IS DISTINCT FROM source.importeCaptacion
# MAGIC ) THEN UPDATE SET 
# MAGIC     num_Leads_Netos     = source.numLeadsNetos,
# MAGIC     num_Leads_Brutos    = source.numLeadsBrutos, 
# MAGIC     num_Matriculas      = source.numMatriculas,
# MAGIC     importe_Venta_Neta  = source.importeVentaNeta,
# MAGIC     importe_Captacion   = source.importeCaptacion
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     fecha,
# MAGIC     escenario,
# MAGIC     producto,
# MAGIC     num_Leads_Netos,
# MAGIC     num_Leads_Brutos,
# MAGIC     num_Matriculas,
# MAGIC     importe_Venta_Neta,
# MAGIC     importe_Captacion
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.fecha,
# MAGIC     source.escenario,
# MAGIC     source.producto,
# MAGIC     source.numLeadsNetos,
# MAGIC     source.numLeadsBrutos,
# MAGIC     source.numMatriculas,
# MAGIC     source.importeVentaNeta,
# MAGIC     source.importeCaptacion
# MAGIC );
