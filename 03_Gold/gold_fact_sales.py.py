# Databricks notebook source
# DBTITLE 1,Header - Fato Vendas
"""
Notebook: gold_fact_sales.py
Camada Gold | Fato Vendas

Origem: main.silver.orderdetails + main.silver.orders + dimensões Gold
Destino: main.gold.fact_sales

Transformações:
    - JOIN orderdetails com orders
    - JOIN com dim_customer, dim_product, dim_employee, dim_date
    - Calcular line_total
    - Adicionar surrogate key (sales_key)
    - Manter order_id como dimensão degenerada

Colunas finais:
    - sales_key (PK)
    - date_key (FK)
    - customer_key (FK)
    - product_key (FK)
    - employee_key (FK)
    - order_id (degenerado)
    - quantity
    - unit_price
    - discount
    - line_total
    - load_timestamp

"""

# COMMAND ----------

# DBTITLE 1,Importar Funções
"/Users/danielbernardo18@hotmail.com/PROJETO PRÁTICO DATABRICKS - SALES DATA LAKEHOUSE/04_Utils/common_functions"

# COMMAND ----------

# DBTITLE 1,Função Surrogate Key
from pyspark.sql.functions import monotonically_increasing_id

def add_surrogate_key(df, key_name):
    """
    Adiciona surrogate key sequencial única na coluna {key_name}_key.
    Usa monotonically_increasing_id para garantir unicidade.
    """
    return df.withColumn(f"{key_name}_key", monotonically_increasing_id())

# COMMAND ----------

# DBTITLE 1,Leitura das Dimensões
# Leitura de todas as dimensões Gold
dim_customer = spark.table('main.gold.dim_customer')
dim_product = spark.table('main.gold.dim_product')
dim_employee = spark.table('main.gold.dim_employee')
dim_date = spark.table('main.gold.dim_date')

print("✅ Dimensões Gold carregadas:")
print(f"  - dim_customer: {dim_customer.count()} registros")
print(f"  - dim_product: {dim_product.count()} registros")
print(f"  - dim_employee: {dim_employee.count()} registros")
print(f"  - dim_date: {dim_date.count()} registros")

# COMMAND ----------

# DBTITLE 1,Leitura e JOIN Base
from pyspark.sql.functions import col, current_timestamp, to_date, date_format

# Leitura Silver com filtros de qualidade
orders_silver = spark.table('main.silver.orders') \
    .filter(col('data_quality_status') == 'VALID') \
    .filter(col('OrderID').rlike('^[0-9]+$'))  # Filtrar apenas OrderID numéricos

orderdetails_silver = spark.table('main.silver.orderdetails').filter(col('data_quality_status') == 'VALID')

# JOIN orderdetails com orders
fact_base = orderdetails_silver.join(
    orders_silver.select('OrderID', 'CustomerID', 'EmployeeID', 'OrderDate'),
    'OrderID',
    'inner'
)

print(f"✅ JOIN base criado: {fact_base.count()} registros")

# COMMAND ----------

# DBTITLE 1,JOIN com Dimensões
# JOIN com dim_customer (INNER para garantir integridade)
fact_sales = fact_base.join(
    dim_customer.select('customer_id', 'customer_key'),
    fact_base.CustomerID == dim_customer.customer_id,
    'inner'  # ✅ CORRIGIDO: INNER JOIN elimina customer_key NULL
)

# JOIN com dim_product
fact_sales = fact_sales.join(
    dim_product.select('product_id', 'product_key'),
    fact_sales.ProductID == dim_product.product_id,
    'inner'  # ✅ CORRIGIDO: INNER JOIN
)

# JOIN com dim_employee
fact_sales = fact_sales.join(
    dim_employee.select('employee_id', 'employee_key'),
    fact_sales.EmployeeID == dim_employee.employee_id,
    'inner'  # ✅ CORRIGIDO: INNER JOIN
)

# Criar date_key para JOIN com dim_date (formato YYYYMMDD como INT)
fact_sales = fact_sales.withColumn(
    'order_date_key',
    date_format(col('OrderDate'), 'yyyyMMdd').cast('int')  # ✅ CORRIGIDO: INT ao invés de STRING
)

# JOIN com dim_date
dim_date_subset = dim_date.select('date_key')
fact_sales = fact_sales.join(
    dim_date_subset,
    fact_sales.order_date_key == dim_date_subset.date_key,
    'inner'  # ✅ CORRIGIDO: INNER JOIN
)

print("✅ JOIN com dimensões completo (INNER JOIN + date_key INT)")

# COMMAND ----------

# DBTITLE 1,Seleção e Métricas
from pyspark.sql.types import DecimalType

# ✅ CORRIGIDO: Adicionar surrogate key PRIMEIRO
fact_sales_with_key = add_surrogate_key(fact_sales, 'sales')

# Selecionar colunas finais com ordem correta (PK primeiro)
fact_sales_final = fact_sales_with_key.select(
    'sales_key',  # ✅ CORRIGIDO: PK primeiro!
    col('date_key'),
    'customer_key',
    'product_key',
    'employee_key',
    col('OrderID').alias('order_id'),
    col('Quantity').alias('quantity'),
    col('UnitPrice').cast(DecimalType(10, 2)).alias('unit_price'),
    col('Discount').cast(DecimalType(5, 4)).alias('discount'),
    col('line_total').cast(DecimalType(12, 2))
)

# Adicionar load_timestamp
fact_sales_final = fact_sales_final.withColumn('load_timestamp', current_timestamp())

print(f"✅ Fato Vendas preparado: {fact_sales_final.count()} registros")
print("✅ Correções aplicadas: INNER JOINs + date_key INT + PK primeiro")

# COMMAND ----------

# DBTITLE 1,Salvar Delta Table
fact_sales_final.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('main.gold.fact_sales')

print("✅ Tabela main.gold.fact_sales salva com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validação
sales_gold = spark.table('main.gold.fact_sales')

print(f"\n📊 VALIDAÇÃO - FATO VENDAS")
print(f"Total de registros: {sales_gold.count()}")
print("\nSchema:")
sales_gold.printSchema()
print("\nPrimeiros 5 registros:")
sales_gold.orderBy('sales_key').show(5, truncate=False)
print("\nValidação de integridade referencial:")
print(f"  - Registros com customer_key nulo: {sales_gold.filter(col('customer_key').isNull()).count()}")
print(f"  - Registros com product_key nulo: {sales_gold.filter(col('product_key').isNull()).count()}")
print(f"  - Registros com employee_key nulo: {sales_gold.filter(col('employee_key').isNull()).count()}")
print(f"  - Registros com date_key nulo: {sales_gold.filter(col('date_key').isNull()).count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.gold.fact_sales limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.gold.vw_sales_by_category LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.gold.vw_sales_by_category LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.gold.vw_sales_by_category LIMIT 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from main.gold.fact_sales limit 10

# COMMAND ----------

