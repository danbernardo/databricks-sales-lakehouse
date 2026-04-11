# Databricks notebook source
# DBTITLE 1,Criação dos Schemas e Volume - Estrutura Base
# MAGIC %sql
# MAGIC USE CATALOG main;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC   COMMENT 'Camada Bronze: dados brutos ingeridos da origem sem transformação';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC   COMMENT 'Camada Silver: dados limpos, validados e enriquecidos';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC   COMMENT 'Camada Gold: modelo dimensional Star Schema para analytics';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS main.bronze.sales_data
# MAGIC   COMMENT 'Volume para armazenar os CSVs originais do SQL Server';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN main;
# MAGIC SHOW VOLUMES IN main.bronze;