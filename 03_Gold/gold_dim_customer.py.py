# Databricks notebook source
# DBTITLE 1,Header - Dimensão Cliente
"""
Notebook: gold_dim_customer.py
Camada Gold | Dimensão Cliente

Origem: main.silver.customers (apenas registros VALID)
Destino: main.gold.dim_customer

Transformações:
    - Filtrar apenas registros VALID
    - Selecionar colunas relevantes para dimensão
    - Adicionar surrogate key (customer_key)
    - Manter natural key (customer_id)
    - Adicionar effective_date

Colunas finais:
    - customer_key (PK - surrogate)
    - customer_id (natural key)
    - company_name
    - contact_name
    - country
    - city
    - region
    - phone
    - effective_date

Autor: Daniel Bernardo
Data: Março 2026
"""

# COMMAND ----------

# DBTITLE 1,Importar Funções Utilitárias
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions"

# COMMAND ----------

# DBTITLE 1,Leitura e Transformação
from pyspark.sql.functions import current_timestamp, col

# Leitura da Silver (apenas VALID)
customers_silver = spark.table('main.silver.customers')
customers_valid = customers_silver.filter(col('data_quality_status') == 'VALID')

# Seleção de colunas relevantes para dimensão
dim_customer_base = customers_valid.select(
    col('CustomerID').alias('customer_id'),
    col('CompanyName').alias('company_name'),
    col('ContactName').alias('contact_name'),
    col('Country').alias('country'),
    col('City').alias('city'),
    col('Region').alias('region'),
    col('Phone').alias('phone')
)

# Adicionar effective_date
dim_customer_base = dim_customer_base.withColumn('effective_date', current_timestamp())

# Adicionar surrogate key (usa função do common_functions com row_number)
dim_customer_with_key = add_surrogate_key(dim_customer_base, 'customer')

# ✅ CORRIGIDO: Reordenar colunas - PK primeiro!
dim_customer = dim_customer_with_key.select(
    'customer_key',      # PK primeiro
    'customer_id',       # Natural key
    'company_name',
    'contact_name',
    'country',
    'city',
    'region',
    'phone',
    'effective_date'
)

print(f"✅ Dimensão Cliente preparada: {dim_customer.count()} registros")
print("✅ Surrogate key sequencial (1, 2, 3...) aplicada")
print("✅ Colunas reordenadas: customer_key como PK primeiro")

# COMMAND ----------

# DBTITLE 1,Salvar Delta Table
# Salvar como Delta Table em Gold
dim_customer.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('main.gold.dim_customer')

print("✅ Tabela main.gold.dim_customer salva com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validação
# Validação da dimensão
customer_gold = spark.table('main.gold.dim_customer')

print(f"\n📊 VALIDAÇÃO - DIMENSÃO CLIENTE")
print(f"Total de registros: {customer_gold.count()}")
print("\nSchema:")
customer_gold.printSchema()
print("\nPrimeiros 5 registros:")
customer_gold.orderBy('customer_key').show(5, truncate=False)

# COMMAND ----------

