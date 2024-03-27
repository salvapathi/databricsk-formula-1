# Databricks notebook source
# MAGIC %md
# MAGIC ##  Connecting to ADLS THROUGH ACCESS KEY from secrets vault

# COMMAND ----------

# MAGIC %md
# MAGIC ### using access from key-valut 
# MAGIC

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list(scope='formula-1_scope')

# COMMAND ----------

formula_access_key=dbutils.secrets.get(scope='formula-1_scope',key='formulaoneacceskey')
spark.conf.set(
    "fs.azure.account.key.storagepraticedl.dfs.core.windows.net",
    formula_access_key)
display(dbutils.fs.ls("abfss://demo@storagepraticedl.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## mounting the containers to notebook 

# COMMAND ----------


storage_account_name = "storagepraticedl"
container_name = "silverdata"

def mount_container(storage_account_name, container_name):
    mount_point = f"/mnt/{container_name}"
    
    mounts = dbutils.fs.mounts()
    if not any(mount.mountPoint == mount_point for mount in mounts):
        dbutils.fs.mount(
            source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
            mount_point=mount_point,
            extra_configs={
                f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": dbutils.secrets.get('formula-1_scope', 'formulaoneacceskey')
            }
        )
        print("THE MOUNT IS SUCCESSFULL")
    else:
        print("It's already mounted")

mount_container(storage_account_name, container_name)
display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/silverdata")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/silverdata"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/rawdata/races.csv"))
display(spark.read.csv("dbfs:/mnt/rawdata/races.csv"))

# COMMAND ----------


