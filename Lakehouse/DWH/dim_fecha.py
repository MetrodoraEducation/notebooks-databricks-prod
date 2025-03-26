# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_fecha lakehouse postgresql
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
        INSERT INTO dim_fecha (
            id_dim_fecha,
            fecha_larga,
            dia_semana,
            dia_semana_corto,
            dia_semana_numero,
            mes_numero,
            mes_corto,
            mes_largo,
            dia_anio,
            semana_anio,
            numero_dias_mes,
            dia_mes,
            primer_dia_mes,
            ultimo_dia_mes,
            anio_numero,
            trimestre_numero,
            trimestre_nombre,
            mes_fiscal_numero,
            anio_fiscal_numero,
            curso_academico,
            es_laborable,
            es_finde_semana
        )
        VALUES %s
        ON CONFLICT (id_dim_fecha) DO NOTHING;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_fecha"],
            row["fecha_larga"],
            row["dia_semana"],
            row["dia_semana_corto"],
            row["dia_semana_numero"],
            row["mes_numero"],
            row["mes_corto"],
            row["mes_largo"],
            row["dia_anio"],
            row["semana_anio"],
            row["numero_dias_mes"],
            row["dia_mes"],
            row["primer_dia_mes"],
            row["ultimo_dia_mes"],
            row["anio_numero"],
            row["trimestre_numero"],
            row["trimestre_nombre"],
            row["mes_fiscal_numero"],
            row["anio_fiscal_numero"],
            row["curso_academico"],
            row["es_laborable"],
            row["es_finde_semana"]
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

# Leer datos desde la tabla gold_lakehouse.dim_fecha en Databricks
source_table = (spark.table("gold_lakehouse.dim_fecha")
                .select("id_dim_fecha", "fecha_larga", "dia_semana", "dia_semana_corto",
                        "dia_semana_numero", "mes_numero", "mes_corto", "mes_largo",
                        "dia_anio", "semana_anio", "numero_dias_mes", "dia_mes", "primer_dia_mes",
                        "ultimo_dia_mes", "anio_numero", "trimestre_numero", "trimestre_nombre",
                        "mes_fiscal_numero", "anio_fiscal_numero", "curso_academico",
                        "es_laborable", "es_finde_semana"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(lambda partition: insert_new_records(partition, db_password))
    logger.info("Inserción completada con éxito.")
except Exception as e:
    logger.error(f"Error general en la inserción: {e}")

print("¡Inserción completada!")
