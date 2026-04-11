# Databricks notebook source
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions"

# COMMAND ----------

# DBTITLE 1,Validação Global Silver
# Validação e Qualidade das Tabelas Silver | Sales Data Lakehouse
# Autor: Daniel Bernardo | Data: 2026-03-17
# Objetivo: Consolidar métricas de qualidade das tabelas Silver, comparar com Bronze, identificar problemas de qualidade e preparar relatório para auditoria.


from pyspark.sql.functions import col

# Lista de tabelas para validação
silver_tables = [
    "categories", "customers", "employees", "orderdetails", "orders", "products", "shippers", "suppliers"
]

# Validação por tabela: comparação contagem Bronze vs Silver e proporção VALID vs INVALID
for table in silver_tables:
    bronze_table = f"main.bronze.{table}"
    silver_table = f"main.silver.{table}"
    print(f"\nTabela: {table}")
    df_bronze = spark.read.format("delta").table(bronze_table)
    df_silver = spark.read.format("delta").table(silver_table)
    print(f"  Contagem Bronze : {df_bronze.count()}")
    print(f"  Contagem Silver : {df_silver.count()}")
    if "data_quality_status" in df_silver.columns:
        df_silver.groupBy("data_quality_status").count().show()
    else:
        print("  Coluna data_quality_status ausente: tabela não tem flag de qualidade.")
    df_silver.show(5)

# Encerramento: análise de principais problemas e sumário de qualidade para relatório final
