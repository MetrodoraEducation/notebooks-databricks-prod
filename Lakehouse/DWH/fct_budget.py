# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert fct_budget lakehouse postgresql
from pyspark.sql.functions import col
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `fct_budget`
def upsert_fct_budget(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO fct_budget (
            id_budget,
            fec_budget,
            id_dim_escenario_budget,
            id_dim_titulacion_budget,
            centro,
            sede,
            modalidad,
            num_leads_netos,
            num_leads_brutos,
            num_matriculas,
            importe_venta_neta,
            importe_venta_bruta,
            importe_captacion,
            fec_procesamiento
        )
        VALUES %s
        ON CONFLICT (id_budget) DO UPDATE SET
            fec_budget = EXCLUDED.fec_budget,
            id_dim_escenario_budget = EXCLUDED.id_dim_escenario_budget,
            id_dim_titulacion_budget = EXCLUDED.id_dim_titulacion_budget,
            centro = EXCLUDED.centro,
            sede = EXCLUDED.sede,
            modalidad = EXCLUDED.modalidad,
            num_leads_netos = EXCLUDED.num_leads_netos,
            num_leads_brutos = EXCLUDED.num_leads_brutos,
            num_matriculas = EXCLUDED.num_matriculas,
            importe_venta_neta = EXCLUDED.importe_venta_neta,
            importe_venta_bruta = EXCLUDED.importe_venta_bruta,
            importe_captacion = EXCLUDED.importe_captacion,
            fec_procesamiento = EXCLUDED.fec_procesamiento;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_budget"],
            row["fec_budget"],
            row["id_dim_escenario_budget"],
            row["id_dim_titulacion_budget"],
            row["centro"],
            row["sede"],
            row["modalidad"],
            row["num_leads_netos"],
            row["num_leads_brutos"],
            row["num_matriculas"],
            row["importe_venta_neta"],
            row["importe_venta_bruta"],
            row["importe_captacion"],
            row["fec_procesamiento"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"Procesados {len(values)} registros en PostgreSQL (insertados o actualizados).")
        else:
            logger.info("No se encontraron datos válidos en esta partición.")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error al procesar registros: {e}")
        raise

# Leer datos desde la tabla `gold_lakehouse.fct_budget` en Databricks
source_table = (spark.table("gold_lakehouse.fct_budget")
                .select("id_budget", "fec_budget", "id_dim_escenario_budget", "id_dim_titulacion_budget",
                        "centro", "sede", "modalidad", "num_leads_netos", "num_leads_brutos",
                        "num_matriculas", "importe_venta_neta", "importe_venta_bruta",
                        "importe_captacion", "fec_procesamiento"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_fct_budget)
    logger.info("Proceso completado con éxito (Upsert en fct_budget de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
