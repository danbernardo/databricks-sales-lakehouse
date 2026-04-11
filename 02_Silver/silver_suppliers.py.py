# Databricks notebook source
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# DBTITLE 1,Transformação Silver - Suppliers
# Silver - Suppliers | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Limpar, padronizar, validar e enriquecer dados do Bronze 'suppliers' e salvar Delta na camada Silver.

from pyspark.sql.functions import current_timestamp

# 1. Leitura Bronze
suppliers_bronze = spark.read.format("delta").table("main.bronze.suppliers")

# 2. Remover duplicatas pela chave primária
suppliers_silver = suppliers_bronze.dropDuplicates(["SupplierID"])

# 3. Limpeza e padronização de textos
suppliers_silver = clean_string_column(suppliers_silver, "Name", case_type="title")
suppliers_silver = clean_string_column(suppliers_silver, "City", case_type="title")
suppliers_silver = clean_string_column(suppliers_silver, "Province", case_type="upper")

# 4. Preencher nulos conforme regra de negócio
suppliers_silver = suppliers_silver.fillna({
    "Address": "N/A",
    "CreateDate": "N/A",
    "UpdateDate": "N/A"
})

# 5. Flag de qualidade: VALID se Name não nulo
suppliers_silver = add_quality_flag(suppliers_silver, ["Name"])

# 6. Adicionar timestamp de processamento
suppliers_silver = suppliers_silver.withColumn("processing_timestamp", current_timestamp())

# 7. Salvar como Delta Silver
suppliers_silver.write.format("delta").mode("overwrite").saveAsTable("main.silver.suppliers")

# 8. Validação
print("Contagem Silver:", suppliers_silver.count())
suppliers_silver.printSchema()
display(suppliers_silver.limit(5))
