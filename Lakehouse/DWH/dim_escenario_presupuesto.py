# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert dim_escenario_presupuesto lakehouse postgresql
from psycopg2.extras import execute_values
from pyspark.sql.functions import col

# Función para insertar o actualizar registros en dim_escenario_presupuesto
def upsert_dim_escenario_presupuesto(partition):
    if not partition:
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Tu función de conexión
        cursor = conn.cursor()

        query = """
        INSERT INTO dim_escenario_presupuesto (
            id_dim_escenario_presupuesto, nombre_escenario, etlcreateddate, etlupdateddate
        )
        VALUES %s
        ON CONFLICT (id_dim_escenario_presupuesto) DO UPDATE SET
            nombre_escenario = EXCLUDED.nombre_escenario,
            etlupdateddate = EXCLUDED.etlupdateddate;
        """

        values = [(
            row["id_dim_escenario_presupuesto"],
            row["nombre_escenario"],
            row["etlcreateddate"],
            row["etlupdateddate"]
        ) for row in partition]

        if values:
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"Procesados {len(values)} registros en PostgreSQL.")
        else:
            logger.info("No se encontraron datos válidos en esta partición.")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error al procesar registros: {e}")
        raise

# Leer datos desde la tabla en Databricks
source_table = (spark.table("gold_lakehouse.dim_escenario_presupuesto")
                .select("id_dim_escenario_presupuesto", "nombre_escenario", "etlcreateddate", "etlupdateddate"))

# Ejecutar upsert particionado
try:
    source_table.foreachPartition(upsert_dim_escenario_presupuesto)
    logger.info("✅ ¡Proceso completado con éxito (dim_escenario_presupuesto)!")
except Exception as e:
    logger.error(f"💥 Error general del proceso: {e}")

print("✅ ¡Proceso completado!")
