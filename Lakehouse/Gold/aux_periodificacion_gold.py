# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW aux_periodificacion
# MAGIC  AS select fctmatricula.fec_matricula as fecha_matricula
# MAGIC           ,'Docencia' as tipo_cargo
# MAGIC           ,matricula.estado_matricula as estado_matricula
# MAGIC           ,fctmatricula.importe_matricula as importe
# MAGIC           ,fctmatricula.id_matricula as id_fct_matricula
# MAGIC           ,fctmatricula.id_dim_estudiante as id_dim_estudiante
# MAGIC           ,fctmatricula.id_dim_producto as id_dim_producto
# MAGIC           ,fctmatricula.id_dim_programa as id_dim_programa
# MAGIC           ,fctmatricula.id_dim_modalidad as id_dim_modalidad
# MAGIC           ,fctmatricula.id_dim_institucion as id_dim_institucion
# MAGIC           ,fctmatricula.id_dim_sede as id_dim_sede
# MAGIC           ,fctmatricula.id_dim_tipo_formacion as id_dim_tipo_formacion
# MAGIC           ,fctmatricula.id_dim_tipo_negocio as id_dim_tipo_negocio
# MAGIC           ,dim_producto.Fecha_Inicio_Reconocimiento as fecha_inicio_reconocimiento
# MAGIC           ,dim_producto.Fecha_Fin_Reconocimiento as fecha_fin_reconocimiento
# MAGIC           ,dim_producto.fecha_Inicio_Curso as fecha_inicio_curso
# MAGIC           ,dim_producto.fecha_Fin_Curso as fecha_fin_curso
# MAGIC           ,dim_producto.modalidad as modalidad
# MAGIC      FROM dbw_metrodoralakehouse_pro.gold_lakehouse.fct_matricula fctmatricula
# MAGIC LEFT JOIN gold_lakehouse.origenClasslife origen ON 1 = origen.id_Dim_Origen_SIS
# MAGIC LEFT JOIN gold_lakehouse.dim_producto dim_producto ON fctmatricula.id_dim_producto = dim_producto.id_Dim_Producto
# MAGIC LEFT JOIN gold_lakehouse.dim_estado_matricula matricula ON fctmatricula.id_dim_estado_matricula = matricula.id_dim_estado_matricula
# MAGIC LEFT JOIN silver_lakehouse.ClasslifeEnrollments enroll ON fctmatricula.cod_matricula = CONCAT(origen.codigo_Origen_SIS, enroll.student_id)
# MAGIC WHERE matricula.id_dim_estado_matricula <> 1 --Status "suspended"
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC  select fctmatricula.fec_matricula as fecha_matricula
# MAGIC           ,'Otros Fees' as tipo_cargo
# MAGIC           ,matricula.estado_matricula as estado_matricula
# MAGIC           ,ROUND(fctmatricula.importe_cobros - fctmatricula.importe_matricula, 2) AS importe
# MAGIC           ,fctmatricula.id_matricula as id_fct_matricula
# MAGIC           ,fctmatricula.id_dim_estudiante as id_dim_estudiante
# MAGIC           ,fctmatricula.id_dim_producto as id_dim_producto
# MAGIC           ,fctmatricula.id_dim_programa as id_dim_programa
# MAGIC           ,fctmatricula.id_dim_modalidad as id_dim_modalidad
# MAGIC           ,fctmatricula.id_dim_institucion as id_dim_institucion
# MAGIC           ,fctmatricula.id_dim_sede as id_dim_sede
# MAGIC           ,fctmatricula.id_dim_tipo_formacion as id_dim_tipo_formacion
# MAGIC           ,fctmatricula.id_dim_tipo_negocio as id_dim_tipo_negocio
# MAGIC           ,dim_producto.Fecha_Inicio_Reconocimiento as fecha_inicio_reconocimiento
# MAGIC           ,dim_producto.Fecha_Fin_Reconocimiento as fecha_fin_reconocimiento
# MAGIC           ,dim_producto.fecha_Inicio_Curso as fecha_inicio_curso
# MAGIC           ,dim_producto.fecha_Fin_Curso as fecha_fin_curso
# MAGIC           ,dim_producto.modalidad as modalidad
# MAGIC      FROM dbw_metrodoralakehouse_pro.gold_lakehouse.fct_matricula fctmatricula
# MAGIC LEFT JOIN gold_lakehouse.origenClasslife origen ON 1 = origen.id_Dim_Origen_SIS
# MAGIC LEFT JOIN gold_lakehouse.dim_producto dim_producto ON fctmatricula.id_dim_producto = dim_producto.id_Dim_Producto
# MAGIC LEFT JOIN gold_lakehouse.dim_estado_matricula matricula ON fctmatricula.id_dim_estado_matricula = matricula.id_dim_estado_matricula
# MAGIC LEFT JOIN silver_lakehouse.ClasslifeEnrollments enroll ON fctmatricula.cod_matricula = CONCAT(origen.codigo_Origen_SIS, enroll.student_id)
# MAGIC     WHERE matricula.id_dim_estado_matricula <> 1 --Status "suspended"
# MAGIC       AND fctmatricula.importe_cobros - fctmatricula.importe_matricula > 0; 
# MAGIC
# MAGIC select * from aux_periodificacion;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW aux_periodificacion_temp
# MAGIC  AS SELECT *
# MAGIC           ,CASE WHEN UPPER(modalidad) = "ONLINE" THEN fecha_matricula
# MAGIC                 WHEN UPPER(tipo_cargo) = "OTROS FEES" THEN fecha_matricula
# MAGIC                 WHEN fecha_inicio_curso IS NULL AND fecha_inicio_reconocimiento IS NULL THEN fecha_matricula
# MAGIC                 WHEN fecha_inicio_reconocimiento IS NULL THEN fecha_inicio_curso
# MAGIC                 ELSE fecha_inicio_reconocimiento
# MAGIC                  END fecha_inicio
# MAGIC           ,CASE WHEN UPPER(modalidad) = "ONLINE" THEN fecha_matricula
# MAGIC                 WHEN UPPER(tipo_cargo) = "OTROS FEES" THEN fecha_matricula
# MAGIC                 WHEN fecha_fin_curso IS NULL AND fecha_fin_reconocimiento IS NULL THEN fecha_matricula
# MAGIC                 WHEN fecha_fin_reconocimiento IS NULL THEN fecha_fin_curso
# MAGIC                 ELSE fecha_fin_reconocimiento
# MAGIC                  END fecha_fin
# MAGIC       FROM aux_periodificacion;
# MAGIC
# MAGIC select * from aux_periodificacion_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW aux_periodificacion_temp_1
# MAGIC  AS SELECT *
# MAGIC             ,CASE
# MAGIC                 WHEN fecha_inicio IS NOT NULL AND fecha_fin IS NOT NULL THEN date_diff(fecha_fin, fecha_inicio) + 1
# MAGIC                 ELSE NULL
# MAGIC              END AS dias_duracion
# MAGIC           --,date_diff(fecha_inicio, fecha_fin) +1 AS dias_duracion
# MAGIC       FROM aux_periodificacion_temp;
# MAGIC
# MAGIC select * from aux_periodificacion_temp_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW periodificacion_matricula AS
# MAGIC SELECT 
# MAGIC   base.*,
# MAGIC   date_add(base.fecha_inicio, offset) AS fecha_periodo,
# MAGIC   TRY_CAST(base.importe / base.dias_duracion AS DECIMAL(18,2)) AS importe_diario
# MAGIC FROM aux_periodificacion_temp_1 base
# MAGIC LATERAL VIEW posexplode(sequence(1, dias_duracion)) AS offset, _
# MAGIC WHERE base.dias_duracion > 0;
# MAGIC
# MAGIC select * from periodificacion_matricula;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW periodificacion_matricula_final AS
# MAGIC SELECT
# MAGIC     DATE_ADD(base.fecha_inicio, rn - 1) AS fecha_devengo,
# MAGIC     base.tipo_cargo,
# MAGIC     base.id_fct_matricula,
# MAGIC     base.id_dim_estudiante,
# MAGIC     base.id_dim_producto,
# MAGIC     base.id_dim_programa,
# MAGIC     base.id_dim_modalidad,
# MAGIC     base.id_dim_institucion,
# MAGIC     base.id_dim_sede,
# MAGIC     base.id_dim_tipo_formacion,
# MAGIC     base.id_dim_tipo_negocio,
# MAGIC     base.fecha_inicio,
# MAGIC     base.fecha_fin,
# MAGIC     base.dias_duracion,
# MAGIC     base.modalidad,
# MAGIC     base.importe,
# MAGIC     base.importe_diario
# MAGIC FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY id_fct_matricula, tipo_cargo
# MAGIC              ORDER BY fecha_periodo
# MAGIC            ) AS rn
# MAGIC     FROM periodificacion_matricula
# MAGIC ) base
# MAGIC WHERE rn <= dias_duracion;
# MAGIC
# MAGIC select * from periodificacion_matricula_final;

# COMMAND ----------

# DBTITLE 1,Delete and Insert
# MAGIC %sql
# MAGIC -- BORRA TODO
# MAGIC DELETE FROM gold_lakehouse.auxiliar_periodificacion;
# MAGIC
# MAGIC -- INSERTA TODO (simula un overwrite)
# MAGIC INSERT INTO gold_lakehouse.auxiliar_periodificacion
# MAGIC SELECT *
# MAGIC FROM periodificacion_matricula_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from gold_lakehouse.auxiliar_periodificacion
# MAGIC select sum(importe) ,sum(importe_diario) from gold_lakehouse.auxiliar_periodificacion
