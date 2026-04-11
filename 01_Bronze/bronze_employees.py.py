# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# 1. Leitura do CSV bruto do volume
caminho_arquivo = "/Volumes/main/bronze/sales_data/employees.csv"
df = spark.read.option("header", True).option("sep", ";").option("includeMetadata", True).csv(caminho_arquivo, inferSchema=True)

# 2. Adição de metadados de controle
bronze_df = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("SQL_Server_OnPremise")) \
    .withColumn("file_name", lit("employees.csv"))

# 3. Salvamento como tabela Delta na camada Bronze
bronze_df.write.format("delta").mode("overwrite").saveAsTable("main.bronze.employees")

# 4. Validação dos dados
print("Contagem de registros:", bronze_df.count())
print("Schema:")
bronze_df.printSchema()
display(bronze_df.limit(5))
