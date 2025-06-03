# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_CONCEPTO_COBRO**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_concepto_cobro_view AS
# MAGIC     SELECT DISTINCT
# MAGIC         receipts.receipt_concept AS concepto
# MAGIC         ,CASE WHEN receipt_concept LIKE 'Certificado%' THEN 0
# MAGIC               ELSE 1
# MAGIC          END tipo_reparto
# MAGIC     FROM silver_lakehouse.ClasslifeReceipts receipts
# MAGIC    WHERE receipts.receipt_concept NOT LIKE 'n/a%'
# MAGIC UNION ALL
# MAGIC     SELECT DISTINCT
# MAGIC         receipt_concept AS concepto,
# MAGIC         CASE 
# MAGIC             WHEN receipt_concept LIKE 'Certificado%' THEN 0
# MAGIC             ELSE 1
# MAGIC         END AS tipo_reparto
# MAGIC     FROM silver_lakehouse.ClasslifeReceipts_931
# MAGIC     WHERE receipt_concept NOT LIKE 'n/a%';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1️⃣ 🔹 Asegurar existencia del registro -1 con 'n/a' (solo una vez)
# MAGIC MERGE INTO gold_lakehouse.dim_concepto_cobro AS target
# MAGIC USING (
# MAGIC     SELECT 'n/a' AS concepto, 'n/a' AS tipo_reparto--, CURRENT_TIMESTAMP() AS ETLcreatedDate, CURRENT_TIMESTAMP() AS ETLupdatedDate
# MAGIC ) AS source
# MAGIC ON UPPER(target.concepto) = UPPER(source.concepto)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (concepto, tipo_reparto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES ('n/a', 'n/a', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
# MAGIC
# MAGIC -- 2️⃣ 🔹 Inserción y actualización de los demás registros desde dim_concepto_cobro_view
# MAGIC MERGE INTO gold_lakehouse.dim_concepto_cobro AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT concepto, tipo_reparto 
# MAGIC     FROM dim_concepto_cobro_view 
# MAGIC     WHERE UPPER(concepto) <> 'N/A' AND UPPER(tipo_reparto) <> 'N/A'
# MAGIC ) AS source
# MAGIC ON UPPER(target.concepto) = UPPER(source.concepto)
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.concepto <> source.concepto OR
# MAGIC     target.tipo_reparto <> source.tipo_reparto
# MAGIC ) THEN UPDATE SET
# MAGIC     target.concepto = source.concepto,
# MAGIC     target.tipo_reparto = source.tipo_reparto,
# MAGIC     target.ETLupdatedDate = CURRENT_TIMESTAMP()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (concepto, tipo_reparto, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (
# MAGIC         source.concepto,
# MAGIC         source.tipo_reparto,
# MAGIC         CURRENT_TIMESTAMP(),
# MAGIC         CURRENT_TIMESTAMP()
# MAGIC     );
