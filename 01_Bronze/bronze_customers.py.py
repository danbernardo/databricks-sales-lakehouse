# Databricks notebook source
# DBTITLE 1,Ingestão Bronze - Customers
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# 1. Leitura do CSV bruto do volume
caminho_arquivo = "/Volumes/main/bronze/sales_data/customers.csv"

# ✅ CORRIGIDO: Adicionado multiLine=True para CSV com quebras de linha dentro de campos
df = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .option("multiLine", "true") \
    .option("includeMetadata", True) \
    .csv(caminho_arquivo, inferSchema=True)

# 2. Adição de metadados de controle
bronze_df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("SQL_Server_OnPremise")) \
    .withColumn("file_name", lit("customers.csv"))  # ✅ CORRIGIDO: customers.csv

# 3. Salvamento como tabela Delta na camada Bronze
bronze_df.write.format("delta").mode("overwrite").saveAsTable("main.bronze.customers")

# 4. Validação dos dados
print("✅ Bronze Customers processado com multiLine=True")
print(f"Contagem de registros: {bronze_df.count()}")
print("\nSchema:")
bronze_df.printSchema()
print("\n5 primeiros registros:")
display(bronze_df.limit(5))