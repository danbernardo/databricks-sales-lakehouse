# Databricks notebook source
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# DBTITLE 1,Transformação Silver - Employees
# Silver - Employees | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Limpar, padronizar, validar e enriquecer dados do Bronze 'employees' e salvar Delta na camada Silver.

from pyspark.sql.functions import current_timestamp

# 1. Leitura Bronze
employees_bronze = spark.read.format("delta").table("main.bronze.employees")

# 2. Remover duplicatas pela chave primária
employees_silver = employees_bronze.dropDuplicates(["EmployeeID"])

# 3. Limpeza e padronização de textos (Region não existe na tabela, apenas PostalCode será preenchida)
employees_silver = clean_string_column(employees_silver, "LastName", case_type="title")
employees_silver = clean_string_column(employees_silver, "FirstName", case_type="title")
employees_silver = clean_string_column(employees_silver, "City", case_type="title")

# 4. Preencher nulos em PostalCode conforme regra de negócio
employees_silver = employees_silver.fillna({"PostalCode": "00000"})

# 5. Flag de qualidade: VALID se LastName e FirstName não nulos
employees_silver = add_quality_flag(employees_silver, ["LastName", "FirstName"])

# 6. Adicionar timestamp de processamento
employees_silver = employees_silver.withColumn("processing_timestamp", current_timestamp())

# 7. Salvar como Delta Silver
employees_silver.write.format("delta").mode("overwrite").saveAsTable("main.silver.employees")

# 8. Validação
print("Contagem Silver:", employees_silver.count())
employees_silver.printSchema()
display(employees_silver.limit(5))
