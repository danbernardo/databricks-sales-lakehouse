# Databricks notebook source
# DBTITLE 1,Header - Dimensão Produto
"""
Notebook: gold_dim_product.py
Camada Gold | Dimensão Produto

Origem: main.silver.products + main.silver.categories (JOIN)
Destino: main.gold.dim_product

Transformações:
    - Filtrar apenas registros VALID
    - JOIN com categories para obter category_name
    - Selecionar colunas relevantes
    - Adicionar surrogate key (product_key)
    - Manter natural key (product_id)
    - Adicionar effective_date

Colunas finais:
    - product_key (PK - surrogate)
    - product_id (natural key)
    - product_name
    - category_name
    - unit_price
    - price_category
    - is_available
    - effective_date

"""

# COMMAND ----------

# DBTITLE 1,Importar Funções
# MAGIC %run "/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions"

# COMMAND ----------

# DBTITLE 1,Leitura e JOIN
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, TimestampType

# Leitura da Silver
products_silver = spark.table('main.silver.products').filter(col('data_quality_status') == 'VALID')
categories_silver = spark.table('main.silver.categories').filter(col('data_quality_status') == 'VALID')

# JOIN products com categories
joined_df = products_silver.join(
    categories_silver.select('CategoryID', col('CategoryName').alias('category_name')),
    'CategoryID',
    'left'
)

# Coletar dados como lista de Rows
rows_data = joined_df.select(
    col('ProductID'),
    col('ProductName'),
    'category_name',
    col('UnitPrice'),
    'price_category',
    'is_available'
).collect()

# ✅ SCHEMA EXPLÍCITO com DECIMAL(10,2) para unit_price
schema = StructType([
    StructField('product_id', IntegerType(), True),
    StructField('product_name', StringType(), True),
    StructField('category_name', StringType(), True),
    StructField('unit_price', DecimalType(10, 2), True),  # DECIMAL(10,2) explícito
    StructField('price_category', StringType(), True),
    StructField('is_available', BooleanType(), True)
])

# Criar DataFrame com schema explícito
dim_product_base = spark.createDataFrame(rows_data, schema)

# Adicionar effective_date
dim_product_base = dim_product_base.withColumn('effective_date', current_timestamp())

# Adicionar surrogate key (usa função do common_functions com row_number)
dim_product_with_key = add_surrogate_key(dim_product_base, 'product')

# ✅ CORRIGIDO: Reordenar colunas - PK primeiro!
dim_product = dim_product_with_key.select(
    'product_key',      # PK primeiro
    'product_id',       # Natural key
    'product_name',
    'category_name',
    'unit_price',
    'price_category',
    'is_available',
    'effective_date'
)

print(f"✅ Dimensão Produto preparada: {dim_product.count()} registros")
print("✅ Schema explícito aplicado: unit_price DECIMAL(10,2)")
print("✅ Surrogate key sequencial (1, 2, 3...) aplicada")
print("✅ Colunas reordenadas: product_key como PK primeiro")

# COMMAND ----------

# DBTITLE 1,Salvar Delta Table
dim_product.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('main.gold.dim_product')

print("✅ Tabela main.gold.dim_product salva com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validação
product_gold = spark.table('main.gold.dim_product')

print(f"\n📊 VALIDAÇÃO - DIMENSÃO PRODUTO")
print(f"Total de registros: {product_gold.count()}")
print("\nSchema:")
product_gold.printSchema()
print("\nPrimeiros 5 registros:")
product_gold.orderBy('product_key').show(5, truncate=False)