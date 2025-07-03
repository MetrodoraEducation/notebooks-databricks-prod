# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_MOTIVO_PERDIDA**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `idDimMotivoPerdida = -1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_motivo_perdida AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS nombre_Dim_Motivo_Perdida
# MAGIC           ,'BRUTOS' AS cantidad -- Si no tiene motivo de perdida es válido
# MAGIC           ,'NETOS' AS calidad -- Si no tiene motivo de perdida es válido
# MAGIC           ,1 AS esBruto -- Si no tiene motivo de perdida es válido, por defecto 1
# MAGIC           ,1 AS esNeto -- Si no tiene motivo de perdida es válido, por defecto 1
# MAGIC           ,NULL AS ETLcreatedDate
# MAGIC           ,NULL AS ETLupdatedDate
# MAGIC ) AS source
# MAGIC ON target.nombre_Dim_Motivo_Perdida = 'n/a'
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET
# MAGIC         target.cantidad = source.cantidad,
# MAGIC         target.calidad = source.calidad,
# MAGIC         target.esBruto = source.esBruto,
# MAGIC         target.esNeto = source.esNeto,
# MAGIC         target.ETLupdatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Dim_Motivo_Perdida, cantidad, calidad, esBruto, esNeto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'BRUTOS', 'NETOS', 1, 1, current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_motivo_perdida_view AS
# MAGIC     SELECT DISTINCT
# MAGIC         tablon.motivo_perdida AS nombre_Dim_Motivo_Perdida
# MAGIC         ,CASE 
# MAGIC               WHEN upper(tablon.motivo_perdida) LIKE 'NV BOT' 
# MAGIC                 OR upper(tablon.motivo_perdida) LIKE 'NV TEST' THEN 'INVÁLIDO'
# MAGIC               ELSE 'BRUTOS'
# MAGIC          END AS cantidad
# MAGIC         ,CASE 
# MAGIC             WHEN upper(tablon.motivo_perdida) LIKE 'NV BOT%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV TEST%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV DATOS ERR_NEOS%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'DATOS ERR_NEOS%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV DUPLICADO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'DUPLICADO%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV BUSCA EMPLEO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'BUSCA EMPLEO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV NIÑO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'MENOR DE EDAD%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV EXTRANJERO%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NO HABLA ESPAÑOL%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV LISTA ROBINSON%' THEN 'INVÁLIDO'
# MAGIC             ELSE 'NETOS'
# MAGIC         END AS calidad
# MAGIC         ,CASE 
# MAGIC             WHEN upper(tablon.motivo_perdida) LIKE 'NV BOT' 
# MAGIC                 OR upper(tablon.motivo_perdida) LIKE 'NV TEST' 
# MAGIC                 OR upper(tablon.motivo_perdida) LIKE 'NA IMPORTACION' THEN 0
# MAGIC             ELSE 1
# MAGIC         END AS esBruto
# MAGIC         ,CASE 
# MAGIC             WHEN upper(tablon.motivo_perdida) LIKE 'NV BOT%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV TEST%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV DATOS ERR_NEOS%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'DATOS ERR_NEOS%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV DUPLICADO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'DUPLICADO%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV BUSCA EMPLEO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'BUSCA EMPLEO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV NIÑO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'MENOR DE EDAD%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV EXTRANJERO%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NO HABLA ESPAÑOL%'
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NV LISTA ROBINSON%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'ATENCI_N AL ALUMNO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'YA ES ALUMNO%' 
# MAGIC               OR upper(tablon.motivo_perdida) LIKE 'NA IMPORTACION'
# MAGIC               THEN 0
# MAGIC             ELSE 1
# MAGIC         END AS esNeto
# MAGIC         ,CURRENT_TIMESTAMP AS ETLcreatedDate
# MAGIC         ,CURRENT_TIMESTAMP AS ETLupdatedDate
# MAGIC         from silver_lakehouse.tablon_leads_and_deals tablon
# MAGIC         where tablon.motivo_Perdida is not null or tablon.motivo_Perdida <> '';
# MAGIC --    FROM silver_lakehouse.zoholeads zl
# MAGIC --FULL OUTER JOIN silver_lakehouse.zohodeals zd
# MAGIC --ON zl.id = zd.id  -- Asegurar la clave de unión (puede cambiar según los datos)
# MAGIC --WHERE COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) IS NOT NULL
# MAGIC  -- AND COALESCE(zd.motivo_perdida_b2c, zd.motivo_perdida_b2b, zl.motivos_perdida) != '';

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
