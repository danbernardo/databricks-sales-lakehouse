# Databricks notebook source
# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.categories")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.customers")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.employees")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.orderdetails")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.orders")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.products")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.shippers")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))

# COMMAND ----------

# Validação Bronze – Categories
df = spark.read.format("delta").table("main.bronze.suppliers")
print("Contagem de registros:", df.count())
df.printSchema()
display(df.limit(5))