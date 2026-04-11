# Databricks notebook source
# DBTITLE 1,Header - Funções Utilitárias
"""
Notebook: common_functions.py
Camada Utilitária | Funções Reutilizáveis

Este notebook contém funções utilitárias reutilizáveis para todas as camadas do projeto Sales Data Lakehouse.

Funções disponíveis:
    - clean_string_column(): Limpeza e padronização de colunas de texto
    - standardize_phone(): Padronização de números de telefone
    - add_quality_flag(): Adiciona flag de qualidade de dados (VALID/INVALID)
    - add_surrogate_key(): Gera surrogate key para dimensões Gold

Autor: Daniel Bernardo
Data: Março 2026
"""

# COMMAND ----------

# DBTITLE 1,Função: clean_string_column
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, initcap, when

def clean_string_column(df: DataFrame, column_name: str, case_type: str = "title") -> DataFrame:
    """
    Limpa e padroniza uma coluna de texto removendo espaços e aplicando case.
    
    Args:
        df: DataFrame PySpark
        column_name: Nome da coluna a limpar
        case_type: Tipo de capitalização ("upper", "title", "lower")
    
    Returns:
        DataFrame com coluna limpa
    """
    if column_name not in df.columns:
        return df
    
    # Remove espaços no início e fim
    df = df.withColumn(column_name, trim(col(column_name)))
    
    # Aplica case conforme solicitado
    if case_type == "upper":
        df = df.withColumn(column_name, upper(col(column_name)))
    elif case_type == "title":
        df = df.withColumn(column_name, initcap(col(column_name)))
    
    return df

# COMMAND ----------

# DBTITLE 1,Função: standardize_phone
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col

def standardize_phone(df: DataFrame, column_name: str) -> DataFrame:
    """
    Padroniza números de telefone removendo caracteres especiais.
    Mantém apenas números e o símbolo +.
    
    Args:
        df: DataFrame PySpark
        column_name: Nome da coluna com números de telefone
    
    Returns:
        DataFrame com coluna de telefones padronizados
    """
    if column_name not in df.columns:
        return df
    
    # Remove parênteses, hífens e espaços
    cleaned = regexp_replace(col(column_name), r"[()\-\s]", "")
    
    return df.withColumn(column_name, cleaned)

# COMMAND ----------

# DBTITLE 1,Função: add_quality_flag
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

def add_quality_flag(df: DataFrame, required_columns: list) -> DataFrame:
    """
    Adiciona coluna data_quality_status com flag VALID ou INVALID.
    Marca INVALID se alguma coluna obrigatória for nula.
    
    Args:
        df: DataFrame PySpark
        required_columns: Lista de colunas obrigatórias para validação
    
    Returns:
        DataFrame com coluna data_quality_status
    """
    # Cria condição: todas as colunas obrigatórias devem ser não nulas
    condition = None
    for column in required_columns:
        if column in df.columns:
            if condition is None:
                condition = col(column).isNotNull()
            else:
                condition = condition & col(column).isNotNull()
    
    if condition is None:
        # Se não houver colunas obrigatórias, marca tudo como VALID
        return df.withColumn("data_quality_status", lit("VALID"))
    
    # Adiciona flag
    df = df.withColumn(
        "data_quality_status",
        when(condition, "VALID").otherwise("INVALID")
    )
    
    return df

# COMMAND ----------

# DBTITLE 1,Função: add_surrogate_key
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

# COMMAND ----------

# DBTITLE 1,Testes das Funções
# Testes básicos das funções utilitárias
print("✅ Funções utilitárias carregadas com sucesso!")
print("\nFunções disponíveis:")
print("  - clean_string_column(df, column_name, case_type)")
print("  - standardize_phone(phone_col)")
print("  - add_quality_flag(df, required_columns)")
print("  - add_surrogate_key(df, dimension_name)")
print("\n✅ Pronto para uso nos notebooks Silver e Gold!")