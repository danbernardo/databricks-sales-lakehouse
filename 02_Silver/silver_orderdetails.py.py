# Databricks notebook source
 "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# DBTITLE 1,Transformação Silver - OrderDetails
# Silver - OrderDetails | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Limpar, validar e enriquecer dados do Bronze 'orderdetails' e salvar Delta na camada Silver.

from pyspark.sql.functions import current_timestamp, col, expr
from pyspark.sql.types import DecimalType

# 1. Leitura Bronze
orderdetails_bronze = spark.read.format("delta").table("main.bronze.orderdetails")

# 2. Converter para DECIMAL (precisão monetária)
orderdetails_silver = orderdetails_bronze \
    .withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(10, 2))) \
    .withColumn("Discount", col("Discount").cast(DecimalType(5, 4)))

# 3. Validar regras de negócio (Quantity > 0, UnitPrice >= 0, Discount entre 0 e 1)
orderdetails_silver = orderdetails_silver \
    .withColumn("Quantity", expr("CASE WHEN Quantity > 0 THEN Quantity ELSE NULL END")) \
    .withColumn("UnitPrice", expr("CASE WHEN UnitPrice >= 0 THEN UnitPrice ELSE NULL END")) \
    .withColumn("Discount", expr("CASE WHEN Discount BETWEEN 0 AND 1 THEN Discount ELSE NULL END"))

# 4. Calcular line_total com DECIMAL (Quantity * UnitPrice * (1 - Discount))
orderdetails_silver = orderdetails_silver \
    .withColumn("line_total", (col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).cast(DecimalType(12, 2)))

# 5. Flag de qualidade: VALID se Quantity, UnitPrice, Discount não nulos e dentro das regras
orderdetails_silver = add_quality_flag(orderdetails_silver, ["OrderDetailsID", "Quantity", "UnitPrice", "Discount"])

# 6. Adicionar timestamp de processamento
orderdetails_silver = orderdetails_silver.withColumn("processing_timestamp", current_timestamp())

# 7. Salvar como Delta Silver
orderdetails_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.silver.orderdetails")

# 8. Validação
print("✅ Silver OrderDetails processado com precisão DECIMAL")
print("Contagem Silver:", orderdetails_silver.count())
orderdetails_silver.printSchema()
print("\nPrimeiras 5 linhas:")
display(orderdetails_silver.limit(5))