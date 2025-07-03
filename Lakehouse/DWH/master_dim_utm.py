# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_ad lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_ad`
def upsert_dim_utm_ad(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_ad (
            id_dim_utm_ad, utm_ad_id, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_dim_utm_ad) DO UPDATE SET
            utm_ad_id = EXCLUDED.utm_ad_id,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_utm_ad"], row["utm_ad_id"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_ad` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_ad")
                .select("id_dim_utm_ad", "utm_ad_id", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_ad)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_ad de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")


# COMMAND ----------

# DBTITLE 1,Insert dim_utm_adset lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_adset`
def upsert_dim_utm_adset(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_adset (
            id_utm_adset_id, utm_adset_id, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_adset_id) DO UPDATE SET
            utm_adset_id = EXCLUDED.utm_adset_id,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_adset_id"], row["utm_adset_id"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_adset` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_adset")
                .select("id_utm_adset_id", "utm_adset_id", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_adset)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_adset de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_campaign lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_campaign`
def upsert_dim_utm_campaign(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_campaign (
            id_utm_campaign_id, utm_campaign_id, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_campaign_id) DO UPDATE SET
            utm_campaign_id = EXCLUDED.utm_campaign_id,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_campaign_id"], row["utm_campaign_id"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_campaign` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_campaign")
                .select("id_utm_campaign_id", "utm_campaign_id", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_campaign)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_campaign de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_campaign_name lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_campaign_name`
def upsert_dim_utm_campaign_name(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_campaign_name (
            id_utm_campaign_name, utm_campaign_name, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_campaign_name) DO UPDATE SET
            utm_campaign_name = EXCLUDED.utm_campaign_name,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_campaign_name"], row["utm_campaign_name"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_campaign_name` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_campaign_name")
                .select("id_utm_campaign_name", "utm_campaign_name", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_campaign_name)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_campaign_name de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_channel lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_channel`
def upsert_dim_utm_channel(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_channel (
            id_utm_channel, utm_channel, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_channel) DO UPDATE SET
            utm_channel = EXCLUDED.utm_channel,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_channel"], row["utm_channel"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_channel` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_channel")
                .select("id_utm_channel", "utm_channel", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_channel)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_channel de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_estrategia lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_channel`
def upsert_dim_utm_estrategia(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_estrategia (
            id_utm_estrategia, utm_estrategia, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_estrategia) DO UPDATE SET
            utm_estrategia = EXCLUDED.utm_estrategia,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_estrategia"], row["utm_estrategia"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_estrategia` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_estrategia")
                .select("id_utm_estrategia", "utm_estrategia", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_estrategia)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_estrategia de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_medium lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_medium`
def upsert_dim_utm_medium(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_medium (
            id_utm_medium, utm_medium, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_medium) DO UPDATE SET
            utm_medium = EXCLUDED.utm_medium,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_medium"], row["utm_medium"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_medium` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_medium")
                .select("id_utm_medium", "utm_medium", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_medium)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_medium de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_perfil lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_perfil`
def upsert_dim_utm_perfil(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_perfil (
            id_utm_perfil, utm_perfil, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_perfil) DO UPDATE SET
            utm_perfil = EXCLUDED.utm_perfil,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_perfil"], row["utm_perfil"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_perfil` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_perfil")
                .select("id_utm_perfil", "utm_perfil", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_perfil)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_perfil de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_source lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_source`
def upsert_dim_utm_source(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_source (
            id_utm_source, utm_source, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_source) DO UPDATE SET
            utm_source = EXCLUDED.utm_source,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_source"], row["utm_source"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_source` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_source")
                .select("id_utm_source", "utm_source", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_source)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_source de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_term lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_term`
def upsert_dim_utm_term(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_term (
            id_utm_term, utm_term, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_term) DO UPDATE SET
            utm_term = EXCLUDED.utm_term,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_term"], row["utm_term"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_term` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_term")
                .select("id_utm_term", "utm_term", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_term)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_term de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

# COMMAND ----------

# DBTITLE 1,Insert dim_utm_type lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_type`
def upsert_dim_utm_type(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_type (
            id_utm_type, utm_type, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_utm_type) DO UPDATE SET
            utm_type = EXCLUDED.utm_type,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_utm_type"], row["utm_type"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_type` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_type")
                .select("id_utm_type", "utm_type", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_type)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_type de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
