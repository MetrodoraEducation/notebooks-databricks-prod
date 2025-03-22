# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ESTADO_MATRICULA**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_estado_matricula_view AS
# MAGIC     SELECT DISTINCT
# MAGIC         enroll_status_id AS cod_estado_matricula,
# MAGIC         enroll_status AS estado_matricula
# MAGIC     FROM silver_lakehouse.ClasslifeEnrollments;
# MAGIC
# MAGIC select * from dim_estado_matricula_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `id_dim_estado_matricula = -1` existe solo una vez con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_estado_matricula AS target
# MAGIC USING (
# MAGIC     SELECT '-1' AS cod_estado_matricula, 'n/a' AS estado_matricula
# MAGIC ) AS source
# MAGIC ON target.cod_estado_matricula = '-1'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (cod_estado_matricula, estado_matricula, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('-1', 'n/a', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 Insertar nuevos valores desde `dim_estado_matricula_view` sin duplicar registros ni alterar el registro `-1`
# MAGIC MERGE INTO gold_lakehouse.dim_estado_matricula AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT cod_estado_matricula, estado_matricula
# MAGIC     FROM dim_estado_matricula_view
# MAGIC     WHERE cod_estado_matricula <> '-1'
# MAGIC ) AS source
# MAGIC ON target.cod_estado_matricula = source.cod_estado_matricula
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (cod_estado_matricula, estado_matricula, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (
# MAGIC         source.cod_estado_matricula, 
# MAGIC         source.estado_matricula, 
# MAGIC         current_timestamp(), 
# MAGIC         current_timestamp()
# MAGIC     );
# MAGIC
