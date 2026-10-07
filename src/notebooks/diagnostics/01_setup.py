# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS learning COMMENT "Catalog learning";
# MAGIC CREATE SCHEMA IF NOT EXISTS learning.bronze COMMENT "Raw data, as it arrived";
# MAGIC CREATE SCHEMA IF NOT EXISTS learning.silver COMMENT "Cleaned data";
# MAGIC CREATE SCHEMA IF NOT EXISTS learning.gold COMMENT "Aggregated data";

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN learning;
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS learning.bronze.raw
# MAGIC COMMENT "Landing zone for raw source files";

# COMMAND ----------

base = "/Volumes/learning/bronze/raw"

for folder in ["customer", "products", "orders"]:
    dbutils.fs.mkdirs(f"{base}/{folder}")

display(dbutils.fs.ls(base))