# Databricks notebook source
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions.py"

# COMMAND ----------

# DBTITLE 1,Transformação Silver - Categories
# Silver - Categories | Sales Data Lakehouse
# Objetivo: Limpar e padronizar dados da tabela Bronze 'categories', aplicar regras de negócio e qualidade, e salvar como Delta Silver.
# Camada Silver = dados limpos, enriquecidos, com metadados e flag de qualidade.

from pyspark.sql.functions import current_timestamp, lit, col

# 1. Leitura dos dados bronze
categories_bronze = spark.read.format("delta").table("main.bronze.categories")

# 2. Limpeza e padronização da coluna CategoryName
categories_silver = clean_string_column(categories_bronze, "CategoryName", case_type="title")

# 3. Preencher nulos em PortugueseDescription e EnglishDescription
categories_silver = categories_silver.fillna({"PortugueseDescription": "N/A", "EnglishDescription": "N/A"})

# 4. Remover duplicatas pela chave primária
categories_silver = categories_silver.dropDuplicates(["CategoryID"])

# 5. Flag de qualidade: INVALID se CategoryID ou CategoryName forem nulos
categories_silver = add_quality_flag(categories_silver, ["CategoryID", "CategoryName"])

# 6. Adicionar processing_timestamp
categories_silver = categories_silver.withColumn("processing_timestamp", current_timestamp())

# 7. Salvar como tabela Delta na camada Silver
categories_silver.write.format("delta").mode("overwrite").saveAsTable("main.silver.categories")

# 8. Validação
print("Contagem Silver:", categories_silver.count())
categories_silver.printSchema()
display(categories_silver.limit(5))