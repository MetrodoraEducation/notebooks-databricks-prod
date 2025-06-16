# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_escenario_budget lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_programa`
def upsert_dim_programa(partition):
    if not partition:
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        # Query con claves únicas cod_Programa + nombre_Programa
        query = """
        INSERT INTO dim_programa (
            cod_Programa, nombre_Programa, tipo_Programa, entidad_Legal,
            especialidad, vertical, nombre_Programa_Completo, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (cod_Programa) DO UPDATE SET
            nombre_Programa = EXCLUDED.nombre_Programa,
            tipo_Programa = EXCLUDED.tipo_Programa,
            entidad_Legal = EXCLUDED.entidad_Legal,
            especialidad = EXCLUDED.especialidad,
            vertical = EXCLUDED.vertical,
            nombre_Programa_Completo = EXCLUDED.nombre_Programa_Completo,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Lista de valores sin incluir id_Dim_Programa
        values = [(
            row["cod_Programa"],
            row["nombre_Programa"],
            row["tipo_Programa"],
            row["entidad_Legal"],
            row["especialidad"],
            row["vertical"],
            row["nombre_Programa_Completo"],
            row["ETLcreatedDate"],
            row["ETLupdatedDate"]
        ) for row in partition]

        if values:
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

# Leer datos desde la tabla en Databricks
source_table = (spark.table("gold_lakehouse.dim_programa")
                .select("cod_Programa", "nombre_Programa", "tipo_Programa",
                        "entidad_Legal", "especialidad", "vertical",
                        "nombre_Programa_Completo", "ETLcreatedDate", "ETLupdatedDate"))

# Ejecutar foreachPartition
try:
    source_table.foreachPartition(upsert_dim_programa)
    logger.info("Proceso completado con éxito (Upsert en dim_programa de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
