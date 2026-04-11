# Databricks notebook source
# MAGIC
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# Silver - Customers | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Limpar, padronizar, validar e enriquecer dados do Bronze 'customers' e salvar Delta na camada Silver.

from pyspark.sql.functions import current_timestamp, col, lit

# 1. Leitura Bronze
customers_bronze = spark.read.format("delta").table("main.bronze.customers")

# 2. Remover duplicatas pela chave primária
customers_silver = customers_bronze.dropDuplicates(["CustomerID"])

# 3. Limpeza e padronização dos campos de nome
customers_silver = clean_string_column(customers_silver, "CompanyName", case_type="title")
customers_silver = clean_string_column(customers_silver, "Country", case_type="title")
customers_silver = clean_string_column(customers_silver, "City", case_type="title")

# 4. Padronização do telefone
customers_silver = standardize_phone(customers_silver, "Phone")

# 5. Preencher nulos conforme regras de negócio
customers_silver = customers_silver.fillna({
    "ContactName": "Unknown",
    "Region": "N/A",
    "PostalCode": "00000"
})

# 6. Flag de qualidade: VALID se CompanyName e Country não nulos
customers_silver = add_quality_flag(customers_silver, ["CompanyName", "Country"])

# 7. Adicionar coluna de processamento
customers_silver = customers_silver.withColumn("processing_timestamp", current_timestamp())

# 8. Salvar como Delta Silver
customers_silver.write.format("delta").mode("overwrite").saveAsTable("main.silver.customers")

# 9. Validação
print("Contagem Silver:", customers_silver.count())
customers_silver.printSchema()
display(customers_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Sumário Silver - Customers
# MAGIC %md
# MAGIC ## Sumário da Transformação Silver – Customers
# MAGIC - Todos os dados foram limpos e padronizados segundo as regras de negócio do projeto.
# MAGIC - Nulos preenchidos conforme orientação do negócio (ContactName, Region, PostalCode).
# MAGIC - Duplicatas removidas via chave primária CustomerID.
# MAGIC - Flags de qualidade aplicadas com base em CompanyName e Country.
# MAGIC - Foi adicionado timestamp de processamento para rastreabilidade.
# MAGIC - Resultados salvos com sucesso em main.silver.customers.
# MAGIC
# MAGIC ### Próximas ações sugeridas
# MAGIC - Conferir proporção VALID/INVALID:
# MAGIC ```python
# MAGIC customers_silver.groupBy('data_quality_status').count().show()
# MAGIC ```
# MAGIC - Documentar no dicionário de dados qualquer regra ou transformação importante aplicada no Silver.
# MAGIC - Repetir padrão para as demais entidades Silver.