# Databricks notebook source
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# DBTITLE 1,Transformação Silver - Products
# Silver - Products | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Limpar, padronizar, validar e enriquecer dados do Bronze 'products' e salvar Delta na camada Silver.

from pyspark.sql.functions import current_timestamp, col, when
from pyspark.sql.types import DecimalType

# 1. Leitura Bronze
products_bronze = spark.read.format("delta").table("main.bronze.products")

# 2. Converter UnitPrice para DECIMAL (precisão monetária)
products_silver = products_bronze.withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(10, 2)))

# 3. Remover duplicatas pela chave primária
products_silver = products_silver.dropDuplicates(["ProductID"])

# 4. Limpeza de ProductName
products_silver = clean_string_column(products_silver, "ProductName", case_type="title")

# 5. Preencher nulos em colunas numéricas com 0
products_silver = products_silver.fillna({
    "UnitsInStock": 0,
    "UnitsOnOrder": 0,
    "ReorderLevel": 0
})

# 6. Criar coluna is_available (TRUE se UnitsInStock > 0)
products_silver = products_silver.withColumn("is_available", when(col("UnitsInStock") > 0, True).otherwise(False))

# 7. Criar coluna price_category
products_silver = products_silver.withColumn(
    "price_category",
    when(col("UnitPrice") < 10, "Low")
    .when((col("UnitPrice") >= 10) & (col("UnitPrice") <= 50), "Medium")
    .when(col("UnitPrice") > 50, "High")
    .otherwise("Unknown")
)

# 8. Flag de qualidade (INVALID se ProductName nulo OU UnitPrice negativo)
products_silver = add_quality_flag(products_silver, ["ProductName", "UnitPrice"])

# 9. Adicionar timestamp de processamento
products_silver = products_silver.withColumn("processing_timestamp", current_timestamp())

# 10. Salvar como Delta Silver
products_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.products")

# 11. Validação
print("✅ Silver Products processado com precisão DECIMAL")
print("Contagem Silver:", products_silver.count())
products_silver.printSchema()
print("\nPrimeiras 5 linhas:")
display(products_silver.limit(5))