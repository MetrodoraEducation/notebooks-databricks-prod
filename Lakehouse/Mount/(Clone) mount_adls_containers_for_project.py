# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksApplicationID')
    tenant_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksTenantID')
    client_secret = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksSecret')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
        # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksApplicationID')
    tenant_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksTenantID')
    client_secret = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksSecret')
    
    # Set spark configurations
    
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }
    mount_point = f"/mnt/{storage_account_name}/{container_name}"
    try:
        # Try to mount the storage account container
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
    except Exception as e:
        if "already mounted" in str(e):
            print(f"{mount_point} already mounted. Attempting to remount.")
            # Optionally, you can unmount and remount here
            # dbutils.fs.unmount(mount_point)
            # dbutils.fs.mount(...)
        else:
            raise e

# COMMAND ----------

    # Get secrets from Key Vault
client_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksApplicationID')
tenant_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksTenantID')
client_secret = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksSecret')

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksApplicationID'),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="KeyVaultSecretScope",key="spDatabricksSecret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@stmetrodoralakehousedev.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Bronze Container

# COMMAND ----------

mount_adls('stmetrodoralakehousedev', 'bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Silver Container

# COMMAND ----------

mount_adls('stmetrodoralakehousedev', 'silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Gold Container

# COMMAND ----------

mount_adls('stmetrodoralakehousedev', 'gold')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "d6e20af7-e417-47ec-8c64-569dc3470ec5",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="KeyVaultSecretScope",key="spDatabricksSecret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/ea02811f-665c-48df-a919-4bdd65678d3f/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@stmetrodoralakehousedev.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()
