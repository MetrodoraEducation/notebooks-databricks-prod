# Databricks notebook source
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_hora lakehouse postgresql
import psycopg2
from psycopg2.extras import execute_values
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Obtener la contraseña de la base de datos fuera de la función
db_password = dbutils.secrets.get('KeyVaultSecretScope', 'psqlpwd')

# Función para insertar registros nuevos sin duplicados
def insert_new_records(partition, db_password):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se insertarán registros.")
        return

    try:
        # Conexión a PostgreSQL
        conn = psycopg2.connect(
            dbname="lakehouse",
            user="sqladminuser",
            password=db_password,
            host="psql-metrodoralakehouse-pro.postgres.database.azure.com",
            port="5432"
        )
        cursor = conn.cursor()

        # Query para insertar registros con manejo de conflictos
        query = """
        INSERT INTO dim_hora (
            id_dim_hora,
            id_dim_hora_larga,
            hora,
            minuto,
            hora12,
            pmam
        )
        VALUES %s
        ON CONFLICT (id_dim_hora) DO NOTHING;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_hora"],
            row["id_dim_hora_larga"],
            row["hora"],
            row["minuto"],
            row["hora12"],
            row["pmam"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"Insertados {len(values)} registros en PostgreSQL.")
        else:
            logger.info("No se encontraron datos válidos en esta partición.")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error al insertar registros: {e}")
        raise

# Leer datos desde la tabla `gold_lakehouse.dim_hora` en Databricks
source_table = (spark.table("gold_lakehouse.dim_hora")
                .select("id_dim_hora", "id_dim_hora_larga", "hora", "minuto", "hora12", "pmam"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(lambda partition: insert_new_records(partition, db_password))
    logger.info("Inserción completada con éxito.")
except Exception as e:
    logger.error(f"Error general en la inserción: {e}")

print("¡Inserción completada!")

