# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert fct_budget lakehouse postgresql
from pyspark.sql.functions import col
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en PostgreSQL
def upsert_fct_budget(partition):
    if not partition:
        logger.info("La partición está vacía.")
        return

    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        query = """
        INSERT INTO fct_budget (
            id_fct_budget,
            id_dim_fecha_budget,
            id_dim_escenario_presupuesto,
            id_Dim_Producto,
            escenario,
            producto,
            num_Leads_Netos,
            num_Leads_Brutos,
            num_Matriculas,
            importe_matriculacion,
            importe_Captacion,
            id_Dim_Programa,
            id_dim_modalidad,
            id_dim_institucion,
            id_dim_sede,
            id_dim_tipo_formacion,
            id_dim_tipo_negocio
        )
        VALUES %s
        ON CONFLICT (
            id_dim_fecha_budget,
            id_dim_escenario_presupuesto,
            id_Dim_Producto,
            id_Dim_Programa
        ) DO UPDATE SET
            id_fct_budget = EXCLUDED.id_fct_budget,
            escenario = EXCLUDED.escenario,
            producto = EXCLUDED.producto,
            num_Leads_Netos = EXCLUDED.num_Leads_Netos,
            num_Leads_Brutos = EXCLUDED.num_Leads_Brutos,
            num_Matriculas = EXCLUDED.num_Matriculas,
            importe_matriculacion = EXCLUDED.importe_matriculacion,
            importe_Captacion = EXCLUDED.importe_Captacion,
            id_dim_modalidad = EXCLUDED.id_dim_modalidad,
            id_dim_institucion = EXCLUDED.id_dim_institucion,
            id_dim_sede = EXCLUDED.id_dim_sede,
            id_dim_tipo_formacion = EXCLUDED.id_dim_tipo_formacion,
            id_dim_tipo_negocio = EXCLUDED.id_dim_tipo_negocio;
        """

        values = [(
            row["id_fct_budget"],
            row["id_dim_fecha_budget"],
            row["id_dim_escenario_presupuesto"],
            row["id_Dim_Producto"],
            row["escenario"],
            row["producto"],
            row["num_Leads_Netos"],
            row["num_Leads_Brutos"],
            row["num_Matriculas"],
            row["importe_matriculacion"],
            row["importe_Captacion"],
            row["id_Dim_Programa"],
            row["id_dim_modalidad"],
            row["id_dim_institucion"],
            row["id_dim_sede"],
            row["id_dim_tipo_formacion"],
            row["id_dim_tipo_negocio"]
        ) for row in partition]

        if values:
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"✅ Upsert completado: {len(values)} registros insertados o actualizados en fct_budget.")
        else:
            logger.info("⚠️ No hay registros válidos para insertar en esta partición.")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"💥 Error en upsert fct_budget: {e}")
        raise

# 🔄 Leer datos desde la tabla en Databricks
source_table = (
    spark.table("gold_lakehouse.fct_budget")
    .select(
        "id_fct_budget","id_dim_fecha_budget", "id_dim_escenario_presupuesto", "id_Dim_Producto",
        "escenario", "producto", "num_Leads_Netos", "num_Leads_Brutos",
        "num_Matriculas", "importe_matriculacion", "importe_Captacion",
        "id_Dim_Programa", "id_dim_modalidad", "id_dim_institucion",
        "id_dim_sede", "id_dim_tipo_formacion", "id_dim_tipo_negocio"
    )
)

# 🚀 Aplicar el upsert con foreachPartition
try:
    source_table.foreachPartition(upsert_fct_budget)
    logger.info("🎯 ¡Proceso completado con éxito en fct_budget!")
except Exception as e:
    logger.error(f"💥 Error general en el proceso fct_budget: {e}")

print("✅ ¡Proceso completado!")
