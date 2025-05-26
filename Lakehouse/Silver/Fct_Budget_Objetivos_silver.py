# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

dimpais_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/budgetobjetivos/{current_date}/Fct_Budget.parquet")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

# 1️⃣ Leer el parquet original
dimpais_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/budgetobjetivos/{current_date}")

# 2️⃣ Convertir a lista para manipular manualmente
rows = dimpais_df.collect()

# 3️⃣ Tomar la fila 6 como cabecera
header_row = rows[5]
new_column_names = [str(cell) if cell is not None else f"col_{i}" for i, cell in enumerate(header_row)]

# 4️⃣ Tomar los datos desde la fila 7
data_rows = rows[6:]

# 5️⃣ Crear esquema con todos los campos como StringType
schema = StructType([StructField(name, StringType(), True) for name in new_column_names])

# 6️⃣ Crear DataFrame limpio
clean_df = spark.createDataFrame(data_rows, schema)

# 7️⃣ Mostrar columnas clave
# Renombrar columnas a snake_case
clean_df = (clean_df
    .withColumnRenamed("idFctBudget", "id_fct_budget")
    .withColumnRenamed("idDimFechaBudget", "id_dim_fecha_budget")
    .withColumnRenamed("idDimEscenarioPresupuesto", "id_dim_escenario_presupuesto")
    .withColumnRenamed("idDimProducto", "id_dim_producto")
    .withColumnRenamed("numLeadsNetos", "num_leads_netos")
    .withColumnRenamed("numLeadsBrutos", "num_leads_brutos")
    .withColumnRenamed("numMatriculas", "num_matriculas")
    .withColumnRenamed("importeMatriculación", "importe_matriculacion")
    .withColumnRenamed("importeCaptacion", "importe_captacion")
    .withColumnRenamed("precioMedio", "precio_medio")
)

# Visualizar resultado final
display(clean_df.select(
    "id_fct_budget", 
    "id_dim_fecha_budget", 
    "id_dim_escenario_presupuesto", 
    "id_dim_producto", 
    "num_leads_netos", 
    "num_leads_brutos", 
    "num_matriculas",
    "importe_matriculacion",
    "importe_captacion",
    "precio_medio"
))


# COMMAND ----------

clean_df.createOrReplaceTempView("fct_budget_objetivos_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select id_fct_budget,id_dim_fecha_budget,id_dim_escenario_presupuesto,id_dim_producto,num_leads_netos,num_leads_brutos,num_matriculas,importe_matriculacion,importe_captacion,precio_medio 
# MAGIC from fct_budget_objetivos_source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.dim_pais
# MAGIC USING dimpais_source_view 
# MAGIC ON silver_lakehouse.dim_pais.id = dimpais_source_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
