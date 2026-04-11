# Databricks notebook source
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# DBTITLE 1,Transformação Silver - Shippers
# Silver - Shippers | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Limpar, padronizar, validar e enriquecer dados do Bronze 'shippers' e salvar Delta na camada Silver.

from pyspark.sql.functions import current_timestamp

# 1. Leitura Bronze
shippers_bronze = spark.read.format("delta").table("main.bronze.shippers")

# 2. Remover duplicatas pela chave primária
shippers_silver = shippers_bronze.dropDuplicates(["ShipperID"])

# 3. Limpeza e padronização de textos
shippers_silver = clean_string_column(shippers_silver, "CompanyName", case_type="title")

# 4. Preencher nulos conforme regra de negócio
# Nenhuma coluna 'Phone' disponível.

# 5. Flag de qualidade: VALID se CompanyName não nulo
shippers_silver = add_quality_flag(shippers_silver, ["CompanyName"])

# 6. Adicionar timestamp de processamento
shippers_silver = shippers_silver.withColumn("processing_timestamp", current_timestamp())

# 7. Salvar como Delta Silver
shippers_silver.write.format("delta").mode("overwrite").saveAsTable("main.silver.shippers")

# 8. Validação
print("Contagem Silver:", shippers_silver.count())
shippers_silver.printSchema()
display(shippers_silver.limit(5))
