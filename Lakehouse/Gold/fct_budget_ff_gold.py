# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW staging_budget_ff_view
# MAGIC     AS SELECT budget_ff.fecha as id_dim_fecha_budget
# MAGIC       ,presupuesto.id_dim_escenario_presupuesto as id_dim_escenario_presupuesto
# MAGIC       ,producto.id_Dim_Producto as id_Dim_Producto
# MAGIC       ,budget_ff.escenario as escenario
# MAGIC       ,budget_ff.producto as producto
# MAGIC       ,budget_ff.num_Leads_Netos as num_Leads_Netos
# MAGIC       ,budget_ff.num_Leads_Brutos as num_Leads_Brutos
# MAGIC       ,budget_ff.num_Matriculas as num_Matriculas
# MAGIC       ,budget_ff.importe_Venta_Neta as importe_matriculacion
# MAGIC       ,budget_ff.importe_Captacion as importe_Captacion
# MAGIC       ,programa.id_Dim_Programa as id_Dim_Programa
# MAGIC       ,modalidad.id_dim_modalidad as id_dim_modalidad
# MAGIC       ,institucion.id_dim_institucion as id_dim_institucion
# MAGIC       ,sede.id_dim_sede as id_dim_sede
# MAGIC       ,tipo_formacion.id_dim_tipo_formacion as id_dim_tipo_formacion
# MAGIC       ,tiponegocio.id_dim_tipo_negocio as id_dim_tipo_negocio
# MAGIC   FROM silver_lakehouse.budget_ff budget_ff
# MAGIC   LEFT JOIN gold_lakehouse.dim_escenario_presupuesto presupuesto ON UPPER(presupuesto.nombre_escenario) = UPPER(budget_ff.escenario)
# MAGIC   LEFT JOIN gold_lakehouse.dim_producto producto ON producto.cod_Producto = budget_ff.producto
# MAGIC   LEFT JOIN gold_lakehouse.dim_programa programa ON programa.cod_Programa = producto.cod_Programa
# MAGIC   LEFT JOIN gold_lakehouse.dim_modalidad modalidad ON UPPER(modalidad.nombre_modalidad) = UPPER(producto.modalidad)
# MAGIC
# MAGIC     -- Cambio para usar la nueva tabla de mapeo de instituciones silver_lakehouse.entidad_legal
# MAGIC   --LEFT JOIN gold_lakehouse.dim_institucion institucion ON UPPER(producto.entidad_Legal) = UPPER(institucion.nombre_institucion)
# MAGIC    LEFT JOIN (select ent.entidad_legal, ins.id_dim_institucion from gold_lakehouse.dim_institucion ins
# MAGIC               left join silver_lakehouse.entidad_legal ent on ent.institucion = ins.nombre_institucion) institucion ON UPPER(producto.entidad_Legal) = NULLIF(UPPER(institucion.entidad_legal), '')
# MAGIC   LEFT JOIN gold_lakehouse.dim_sede sede ON UPPER(producto.sede) = UPPER(sede.nombre_sede)
# MAGIC   LEFT JOIN gold_lakehouse.dim_tipo_formacion tipo_formacion ON UPPER(tipo_formacion.tipo_formacion_desc) = UPPER(producto.tipo_Producto)
# MAGIC   LEFT JOIN gold_lakehouse.dim_tipo_negocio tiponegocio ON UPPER(producto.tipo_Negocio) = UPPER(tiponegocio.tipo_negocio_desc);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.fct_budget AS target
# MAGIC USING staging_budget_ff_view AS source
# MAGIC ON 
# MAGIC     target.id_dim_fecha_budget = source.id_dim_fecha_budget AND
# MAGIC     target.id_dim_escenario_presupuesto = source.id_dim_escenario_presupuesto AND
# MAGIC     target.id_Dim_Producto = source.id_Dim_Producto AND
# MAGIC     target.id_Dim_Programa = source.id_Dim_Programa
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        target.num_Leads_Netos       IS DISTINCT FROM source.num_Leads_Netos
# MAGIC     OR target.num_Leads_Brutos      IS DISTINCT FROM source.num_Leads_Brutos
# MAGIC     OR target.num_Matriculas        IS DISTINCT FROM source.num_Matriculas
# MAGIC     OR target.importe_matriculacion IS DISTINCT FROM source.importe_matriculacion
# MAGIC     OR target.importe_Captacion     IS DISTINCT FROM source.importe_Captacion
# MAGIC     OR target.id_Dim_Programa       IS DISTINCT FROM source.id_Dim_Programa
# MAGIC     OR target.id_dim_modalidad      IS DISTINCT FROM source.id_dim_modalidad
# MAGIC     OR target.id_dim_institucion    IS DISTINCT FROM source.id_dim_institucion
# MAGIC     OR target.id_dim_sede           IS DISTINCT FROM source.id_dim_sede
# MAGIC     OR target.id_dim_tipo_formacion    IS DISTINCT FROM source.id_dim_tipo_formacion
# MAGIC     OR target.id_dim_tipo_negocio   IS DISTINCT FROM source.id_dim_tipo_negocio
# MAGIC     OR target.id_dim_escenario_presupuesto IS DISTINCT FROM source.id_dim_escenario_presupuesto
# MAGIC ) THEN UPDATE SET
# MAGIC     num_Leads_Netos       = source.num_Leads_Netos,
# MAGIC     num_Leads_Brutos      = source.num_Leads_Brutos,
# MAGIC     num_Matriculas        = source.num_Matriculas,
# MAGIC     importe_matriculacion = source.importe_matriculacion,
# MAGIC     importe_Captacion     = source.importe_Captacion,
# MAGIC     escenario             = source.escenario,
# MAGIC     producto              = source.producto,
# MAGIC     id_Dim_Programa       = source.id_Dim_Programa,
# MAGIC     id_dim_modalidad      = source.id_dim_modalidad,
# MAGIC     id_dim_institucion    = source.id_dim_institucion,
# MAGIC     id_dim_sede           = source.id_dim_sede,
# MAGIC     id_dim_tipo_formacion = source.id_dim_tipo_formacion,
# MAGIC     id_dim_tipo_negocio   = source.id_dim_tipo_negocio,
# MAGIC     id_dim_escenario_presupuesto = source.id_dim_escenario_presupuesto
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     id_dim_fecha_budget,
# MAGIC     id_dim_escenario_presupuesto,
# MAGIC     id_Dim_Producto,
# MAGIC     escenario,
# MAGIC     producto,
# MAGIC     num_Leads_Netos,
# MAGIC     num_Leads_Brutos,
# MAGIC     num_Matriculas,
# MAGIC     importe_matriculacion,
# MAGIC     importe_Captacion,
# MAGIC     id_Dim_Programa,
# MAGIC     id_dim_modalidad,
# MAGIC     id_dim_institucion,
# MAGIC     id_dim_sede,
# MAGIC     id_dim_tipo_formacion,
# MAGIC     id_dim_tipo_negocio
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.id_dim_fecha_budget,
# MAGIC     source.id_dim_escenario_presupuesto,
# MAGIC     source.id_Dim_Producto,
# MAGIC     source.escenario,
# MAGIC     source.producto,
# MAGIC     source.num_Leads_Netos,
# MAGIC     source.num_Leads_Brutos,
# MAGIC     source.num_Matriculas,
# MAGIC     source.importe_matriculacion,
# MAGIC     source.importe_Captacion,
# MAGIC     source.id_Dim_Programa,
# MAGIC     source.id_dim_modalidad,
# MAGIC     source.id_dim_institucion,
# MAGIC     source.id_dim_sede,
# MAGIC     source.id_dim_tipo_formacion,
# MAGIC     source.id_dim_tipo_negocio
# MAGIC );
# MAGIC

# COMMAND ----------

#%sql select distinct id_dim_modalidad from gold_lakehouse.fct_budget
