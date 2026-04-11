# Databricks notebook source
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# DBTITLE 1,Transformação Silver - Orders
# Silver - Orders | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Limpar, validar e enriquecer dados do Bronze 'orders' e salvar Delta na camada Silver.

from pyspark.sql.functions import current_timestamp, col, expr, datediff, when, to_date

# 1. Leitura Bronze
orders_bronze = spark.read.format("delta").table("main.bronze.orders")

# 2. Remover duplicatas pela chave primária
orders_silver = orders_bronze.dropDuplicates(["OrderID"])

# 3. Corrigir tipo das datas (OrderDate, ShippedDate) para garantir comparações/cálculos corretos
orders_silver = orders_silver.withColumn(
    "OrderDate",
    to_date(col("OrderDate"), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS")
)
orders_silver = orders_silver.withColumn(
    "ShippedDate",
    to_date(col("ShippedDate"), "yyyy/MM/dd HH:mm:ss.SSSSSSSSS")
)

# 4. Validar datas (OrderDate não pode ser futura)
from datetime import datetime
hoje = datetime.today().strftime('%Y-%m-%d')
orders_silver = orders_silver.withColumn(
    "OrderDate",
    when(col("OrderDate") <= hoje, col("OrderDate")).otherwise(None)
)

# 5. Criar flag is_shipped (TRUE se ShippedDate não nulo)
orders_silver = orders_silver.withColumn("is_shipped", when(col("ShippedDate").isNotNull(), True).otherwise(False))

# 6. Calcular days_to_ship (diferença entre OrderDate e ShippedDate)
orders_silver = orders_silver.withColumn("days_to_ship", datediff(col("ShippedDate"), col("OrderDate")))

# 7. Flag de qualidade: VALID se OrderDate não nulo e OrderID válido
orders_silver = add_quality_flag(orders_silver, ["OrderID", "OrderDate"])

# 8. Adicionar timestamp de processamento
orders_silver = orders_silver.withColumn("processing_timestamp", current_timestamp())

# 9. Salvar como Delta Silver
orders_silver.write.format("delta").mode("overwrite").saveAsTable("main.silver.orders")

# 10. Validação
print("Contagem Silver:", orders_silver.count())
orders_silver.printSchema()
display(orders_silver.limit(5))
