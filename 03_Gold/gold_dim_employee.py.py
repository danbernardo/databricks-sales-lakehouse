# Databricks notebook source
# DBTITLE 1,Header - Dimensão Funcionário
"""
Notebook: gold_dim_employee.py
Camada Gold | Dimensão Funcionário

Origem: main.silver.employees (apenas registros VALID)
Destino: main.gold.dim_employee

Transformações:
    - Filtrar apenas registros VALID
    - Concatenar FirstName + LastName para full_name
    - Selecionar colunas relevantes
    - Adicionar surrogate key (employee_key)
    - Manter natural key (employee_id)
    - Adicionar effective_date

Colunas finais:
    - employee_key (PK - surrogate)
    - employee_id (natural key)
    - full_name
    - city
    - province
    - phone
    - effective_date

"""

# COMMAND ----------

# DBTITLE 1,Importar Funções
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

def add_surrogate_key(df: DataFrame, dimension_name: str) -> DataFrame:
    """
    Adiciona uma surrogate key (chave substituta) para dimensões Gold.
    Gera ID sequencial único.
    
    Args:
        df: DataFrame PySpark
        dimension_name: Nome da dimensão (ex: "customer", "product")
    
    Returns:
        DataFrame com coluna {dimension_name}_key
    """
    key_column = f"{dimension_name}_key"
    
    # Gera ID sequencial usando row_number
    window_spec = Window.orderBy(monotonically_increasing_id())
    df = df.withColumn(key_column, row_number().over(window_spec))
    
    return df

print("✅ Função add_surrogate_key carregada!")

# COMMAND ----------

# DBTITLE 1,Leitura e Transformação
from pyspark.sql.functions import current_timestamp, col, concat, lit

# Leitura da Silver (apenas VALID)
employees_silver = spark.table('main.silver.employees').filter(col('data_quality_status') == 'VALID')

# Seleção e transformação
dim_employee_base = employees_silver.select(
    col('EmployeeID').alias('employee_id'),
    concat(col('FirstName'), lit(' '), col('LastName')).alias('full_name'),
    col('City').alias('city'),
    col('Province').alias('province'),
    col('Phone').alias('phone')
)

# Adicionar effective_date
dim_employee_base = dim_employee_base.withColumn('effective_date', current_timestamp())

# Adicionar surrogate key (usa função do common_functions com row_number)
dim_employee_with_key = add_surrogate_key(dim_employee_base, 'employee')

# ✅ CORRIGIDO: Reordenar colunas - PK primeiro!
dim_employee = dim_employee_with_key.select(
    'employee_key',     # PK primeiro
    'employee_id',      # Natural key
    'full_name',
    'city',
    'province',
    'phone',
    'effective_date'
)

print(f"✅ Dimensão Funcionário preparada: {dim_employee.count()} registros")
print("✅ Surrogate key sequencial (1, 2, 3...) aplicada")
print("✅ Colunas reordenadas: employee_key como PK primeiro")

# COMMAND ----------

# DBTITLE 1,Salvar Delta Table
dim_employee.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('main.gold.dim_employee')

print("✅ Tabela main.gold.dim_employee salva com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validação
employee_gold = spark.table('main.gold.dim_employee')

print(f"\n📊 VALIDAÇÃO - DIMENSÃO FUNCIONÁRIO")
print(f"Total de registros: {employee_gold.count()}")
print("\nSchema:")
employee_gold.printSchema()
print("\nPrimeiros 5 registros:")
employee_gold.orderBy('employee_key').show(5, truncate=False)