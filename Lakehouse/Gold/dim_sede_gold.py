# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_SEDE**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dim_sede_view AS
# MAGIC WITH sede_rankeada AS (
# MAGIC     SELECT DISTINCT 
# MAGIC          TRY_CAST(producto.codigo_sede AS INT) AS number_codigo_sede,
# MAGIC          CASE UPPER(producto.sede)
# MAGIC              WHEN 'ALICANTE' THEN 'ALI'
# MAGIC              WHEN 'BARCELONA' THEN 'BCN'
# MAGIC              WHEN 'BILBAO' THEN 'BIL'
# MAGIC              WHEN 'GIJÓN' THEN 'GIJ'
# MAGIC              WHEN 'IRUN' THEN 'IRN'
# MAGIC              WHEN 'LA CORUÑA' THEN 'LCR'
# MAGIC              WHEN 'LAS PALMAS DE GRAN CANARIA' THEN 'PGC'
# MAGIC              WHEN 'LOGROÑO' THEN 'LOG'
# MAGIC              WHEN 'MADRID' THEN 'MAD'
# MAGIC              WHEN 'MÁLAGA' THEN 'MLG'
# MAGIC              WHEN 'MALAGA' THEN 'MLG'
# MAGIC              WHEN 'MURCIA' THEN 'MUR'
# MAGIC              WHEN 'ONLINE' THEN 'ONL'
# MAGIC              WHEN 'PALMA DE MALLORCA' THEN 'MLL'
# MAGIC              WHEN 'SANTA CRUZ DE TENERIFE' THEN 'SCT'
# MAGIC              WHEN 'SANTANDER' THEN 'SAN'
# MAGIC              WHEN 'SEVILLA' THEN 'SEV'
# MAGIC              WHEN 'VALENCIA' THEN 'VLC'
# MAGIC              WHEN 'VITORIA-GASTEIZ' THEN 'VIT'
# MAGIC              WHEN 'ZARAGOZA' THEN 'ZGZ'
# MAGIC              ELSE NULL
# MAGIC          END AS codigo_sede,
# MAGIC          CASE 
# MAGIC              WHEN producto.sede IS NULL THEN 'NO REGISTRA'
# MAGIC              ELSE producto.sede
# MAGIC          END AS nombre_sede,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY producto.sede ORDER BY TRY_CAST(producto.codigo_sede AS INT)) AS rn
# MAGIC     FROM gold_lakehouse.dim_producto producto
# MAGIC     WHERE producto.cod_producto IS NOT NULL
# MAGIC       AND producto.sede IS NOT NULL
# MAGIC       AND producto.sede <> 'n/a'
# MAGIC )
# MAGIC SELECT number_codigo_sede, codigo_sede, nombre_sede
# MAGIC FROM sede_rankeada
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT number_codigo_sede, COUNT(*)
# MAGIC     FROM dim_sede_view
# MAGIC GROUP BY number_codigo_sede
# MAGIC   HAVING COUNT(*) > 1;

# COMMAND ----------

# DBTITLE 1,Merge Into from sales
# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar que el registro `-1` existe con valores `n/a`
# MAGIC MERGE INTO gold_lakehouse.dim_sede AS target
# MAGIC USING (
# MAGIC     SELECT -1 AS number_codigo_sede, 'n/a' AS nombre_sede, 'n/a' AS codigo_sede
# MAGIC ) AS source
# MAGIC ON target.id_dim_sede = -1
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (number_codigo_sede, nombre_sede, codigo_sede)
# MAGIC     VALUES (-1, 'n/a', 'n/a');
# MAGIC
# MAGIC MERGE INTO gold_lakehouse.dim_sede AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT number_codigo_sede, nombre_sede, codigo_sede FROM dim_sede_view
# MAGIC ) AS source
# MAGIC ON target.nombre_sede = source.nombre_sede
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET target.codigo_sede = source.codigo_sede
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (number_codigo_sede, nombre_sede, codigo_sede, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.number_codigo_sede, source.nombre_sede, source.codigo_sede, current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3️⃣ 🔹 Asegurar que solo haya un único ID `-1`
# MAGIC DELETE FROM gold_lakehouse.dim_sede
# MAGIC WHERE nombre_sede = 'n/a' AND id_dim_sede <> -1;
