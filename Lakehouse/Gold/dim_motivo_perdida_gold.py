# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_MOTIVO_PERDIDA**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_motivo_perdida_clientify_view AS
# MAGIC SELECT DISTINCT
# MAGIC     TRIM(lost_reason) AS nombre_Dim_Motivo_Perdida
# MAGIC     ,CASE 
# MAGIC         WHEN lost_reason LIKE 'NV Bot' 
# MAGIC             OR lost_reason LIKE 'NV Test' THEN 'INVÁLIDO'
# MAGIC         ELSE 'BRUTOS'
# MAGIC     END AS cantidad
# MAGIC     ,CASE 
# MAGIC         WHEN lost_reason LIKE 'NV Bot' 
# MAGIC             OR lost_reason LIKE 'NV Test' 
# MAGIC             OR lost_reason LIKE 'NV Datos erróneos' 
# MAGIC             OR lost_reason LIKE 'NV Duplicado' 
# MAGIC             OR lost_reason LIKE 'NV Busca empleo' 
# MAGIC             OR lost_reason LIKE 'NV Niño' 
# MAGIC             OR lost_reason LIKE 'NV Extranjero' 
# MAGIC             OR lost_reason LIKE 'NV Lista Robinson' THEN 'INVÁLIDO'
# MAGIC         ELSE 'NETOS'
# MAGIC     END AS calidad
# MAGIC     ,CASE 
# MAGIC         WHEN calidad = 'INVÁLIDO' THEN 0
# MAGIC         ELSE 1
# MAGIC     END AS esBruto
# MAGIC     ,CASE 
# MAGIC         WHEN CALIDAD = 'INVÁLIDO' THEN 0
# MAGIC         ELSE 1
# MAGIC     END AS esNeto
# MAGIC     ,CURRENT_TIMESTAMP AS ETLcreatedDate
# MAGIC     ,CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC FROM silver_lakehouse.clientifydeals
# MAGIC WHERE lost_reason IS NOT NULL AND lost_reason <> '';
# MAGIC
# MAGIC select * from dim_motivo_perdida_clientify_view

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `idDimMotivoPerdida = -1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_Dim_Motivo_Perdida
# MAGIC           ,'INVALIDO' AS cantidad
# MAGIC           ,'INVALIDO' AS calidad
# MAGIC           ,-1 AS esBruto
# MAGIC           ,-1 AS esNeto
# MAGIC           ,NULL AS ETLcreatedDate
# MAGIC           ,NULL AS ETLupdatedDate
# MAGIC ) AS source
# MAGIC ON target.nombre_Dim_Motivo_Perdida = 'n/a'
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Dim_Motivo_Perdida, cantidad, calidad, esBruto, esNeto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', 'n/a', -1, -1, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- 2️⃣ 🔹 MERGE para insertar o actualizar `dim_motivo_perdida`, excluyendo `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT 
# MAGIC         TRIM(nombre_Dim_Motivo_Perdida) AS nombre_Dim_Motivo_Perdida,
# MAGIC         cantidad,
# MAGIC         calidad,
# MAGIC         esBruto,
# MAGIC         esNeto,
# MAGIC         current_timestamp() AS ETLcreatedDate,
# MAGIC         current_timestamp() AS ETLupdatedDate
# MAGIC     FROM dim_motivo_perdida_clientify_view
# MAGIC     WHERE nombre_Dim_Motivo_Perdida IS NOT NULL 
# MAGIC       AND nombre_Dim_Motivo_Perdida <> '' 
# MAGIC       AND nombre_Dim_Motivo_Perdida <> 'n/a'  -- Evitar modificar el registro especial
# MAGIC ) AS source
# MAGIC ON target.nombre_Dim_Motivo_Perdida = source.nombre_Dim_Motivo_Perdida
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.cantidad = source.cantidad,
# MAGIC         target.calidad = source.calidad,
# MAGIC         target.esBruto = source.esBruto,
# MAGIC         target.esNeto = source.esNeto,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Dim_Motivo_Perdida, cantidad, calidad, esBruto, esNeto ,ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Dim_Motivo_Perdida, source.cantidad, source.calidad, source.esBruto, source.esNeto ,source.ETLcreatedDate, source.ETLupdatedDate);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_motivo_perdida_view AS
# MAGIC     SELECT DISTINCT
# MAGIC         COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) AS nombre_Dim_Motivo_Perdida
# MAGIC         ,CASE 
# MAGIC               WHEN COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Bot' 
# MAGIC                 OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Test' THEN 'INVÁLIDO'
# MAGIC               ELSE 'BRUTOS'
# MAGIC          END AS cantidad
# MAGIC         ,CASE 
# MAGIC             WHEN COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Bot%' 
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Test%' 
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Datos erróneos%' 
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Duplicado%' 
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Busca empleo%' 
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Niño%' 
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Extranjero%'
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'Duplicado%' 
# MAGIC               OR COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) LIKE 'NV Lista Robinson%' THEN 'INVÁLIDO'
# MAGIC             ELSE 'NETOS'
# MAGIC         END AS calidad
# MAGIC         ,CASE 
# MAGIC             WHEN calidad = 'INVÁLIDO' THEN 0
# MAGIC             ELSE 1
# MAGIC         END AS esBruto
# MAGIC         ,CASE 
# MAGIC             WHEN CALIDAD = 'INVÁLIDO' THEN 0
# MAGIC             ELSE 1
# MAGIC         END AS esNeto
# MAGIC         ,CURRENT_TIMESTAMP AS ETLcreatedDate
# MAGIC         ,CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC     FROM silver_lakehouse.zoholeads zl
# MAGIC FULL OUTER JOIN silver_lakehouse.zohodeals zd
# MAGIC ON zl.id = zd.id  -- Asegurar la clave de unión (puede cambiar según los datos)
# MAGIC WHERE COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) IS NOT NULL
# MAGIC   AND COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) != '';
# MAGIC
# MAGIC select * from dim_motivo_perdida_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida AS target
# MAGIC USING dim_motivo_perdida_view AS source
# MAGIC ON target.nombre_Dim_Motivo_Perdida = source.nombre_Dim_Motivo_Perdida
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET
# MAGIC         target.cantidad = source.cantidad,
# MAGIC         target.calidad = source.calidad,
# MAGIC         target.esBruto = source.esBruto,
# MAGIC         target.esNeto = source.esNeto,
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Dim_Motivo_Perdida, cantidad, calidad, esBruto, esNeto,ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Dim_Motivo_Perdida, source.cantidad, source.calidad, source.esBruto, source.esNeto,source.ETLcreatedDate, source.ETLupdatedDate);
