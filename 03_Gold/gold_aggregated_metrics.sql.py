# Databricks notebook source
# DBTITLE 1,Header - Métricas Agregadas
# MAGIC %md
# MAGIC # Gold - Métricas Agregadas
# MAGIC
# MAGIC **Notebook:** gold_aggregated_metrics.sql  
# MAGIC **Camada:** Gold | Views Agregadas  
# MAGIC **Origem:** main.gold.fact_sales + dimensões
# MAGIC
# MAGIC **Views Criadas:**
# MAGIC 1. `vw_sales_by_category` - Vendas por categoria (mensal)
# MAGIC 2. `vw_employee_performance` - Performance de vendedores
# MAGIC 3. `vw_top_customers` - Top clientes por receita
# MAGIC 4. `vw_sales_by_period` - Análise temporal (ano/mês/trimestre)
# MAGIC 5. `vw_discount_impact` - Impacto dos descontos
# MAGIC 6. `vw_top_products` - Top produtos mais vendidos
# MAGIC

# COMMAND ----------

# DBTITLE 1,View 1: Vendas por Categoria
# MAGIC %sql
# MAGIC -- View: Vendas por Categoria (Mensal)
# MAGIC CREATE OR REPLACE VIEW main.gold.vw_sales_by_category AS
# MAGIC SELECT 
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     p.category_name,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     SUM(f.quantity) AS total_quantity,
# MAGIC     CAST(SUM(f.line_total) AS DECIMAL(15,2)) AS total_revenue,
# MAGIC     CAST(AVG(f.line_total) AS DECIMAL(12,2)) AS avg_line_value,
# MAGIC     CAST(SUM(f.discount * f.unit_price * f.quantity) AS DECIMAL(15,2)) AS total_discount_amount
# MAGIC FROM main.gold.fact_sales f
# MAGIC INNER JOIN main.gold.dim_date d ON f.date_key = d.date_key
# MAGIC INNER JOIN main.gold.dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY d.year, d.month, d.month_name, p.category_name
# MAGIC ORDER BY d.year DESC, d.month DESC, total_revenue DESC;
# MAGIC
# MAGIC SELECT '✅ View vw_sales_by_category criada com precisão DECIMAL!' AS status;

# COMMAND ----------

# DBTITLE 1,View 2: Performance Vendedores
# MAGIC %sql
# MAGIC -- View: Performance de Vendedores
# MAGIC CREATE OR REPLACE VIEW main.gold.vw_employee_performance AS
# MAGIC SELECT 
# MAGIC     e.employee_id,
# MAGIC     e.full_name,
# MAGIC     e.city,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     SUM(f.quantity) AS total_units_sold,
# MAGIC     CAST(SUM(f.line_total) AS DECIMAL(15,2)) AS total_revenue,
# MAGIC     CAST(AVG(f.line_total) AS DECIMAL(12,2)) AS avg_transaction_value,
# MAGIC     CAST(SUM(f.line_total) / COUNT(DISTINCT f.order_id) AS DECIMAL(12,2)) AS avg_order_value,
# MAGIC     CAST(MAX(f.line_total) AS DECIMAL(12,2)) AS max_transaction
# MAGIC FROM main.gold.fact_sales f
# MAGIC INNER JOIN main.gold.dim_employee e ON f.employee_key = e.employee_key
# MAGIC GROUP BY e.employee_id, e.full_name, e.city
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC SELECT '✅ View vw_employee_performance criada com precisão DECIMAL!' AS status;

# COMMAND ----------

# DBTITLE 1,View 3: Top Clientes
# MAGIC %sql
# MAGIC -- View: Top Clientes por Receita
# MAGIC CREATE OR REPLACE VIEW main.gold.vw_top_customers AS
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.company_name,
# MAGIC     c.country,
# MAGIC     c.city,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     SUM(f.quantity) AS total_units_purchased,
# MAGIC     CAST(SUM(f.line_total) AS DECIMAL(15,2)) AS total_spent,
# MAGIC     CAST(AVG(f.line_total) AS DECIMAL(12,2)) AS avg_transaction_value,
# MAGIC     MIN(d.full_date) AS first_purchase_date,
# MAGIC     MAX(d.full_date) AS last_purchase_date,
# MAGIC     DATEDIFF(MAX(d.full_date), MIN(d.full_date)) AS customer_lifetime_days
# MAGIC FROM main.gold.fact_sales f
# MAGIC INNER JOIN main.gold.dim_customer c ON f.customer_key = c.customer_key
# MAGIC INNER JOIN main.gold.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY c.customer_id, c.company_name, c.country, c.city
# MAGIC ORDER BY total_spent DESC;
# MAGIC
# MAGIC SELECT '✅ View vw_top_customers criada com precisão DECIMAL!' AS status;

# COMMAND ----------

# DBTITLE 1,View 4: Vendas por Período
# MAGIC %sql
# MAGIC -- View: Análise Temporal de Vendas
# MAGIC CREATE OR REPLACE VIEW main.gold.vw_sales_by_period AS
# MAGIC SELECT 
# MAGIC     d.year,
# MAGIC     d.quarter,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     d.is_weekend,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     SUM(f.quantity) AS total_quantity,
# MAGIC     CAST(SUM(f.line_total) AS DECIMAL(15,2)) AS total_revenue,
# MAGIC     CAST(AVG(f.line_total) AS DECIMAL(12,2)) AS avg_transaction_value,
# MAGIC     COUNT(DISTINCT f.customer_key) AS unique_customers,
# MAGIC     COUNT(DISTINCT f.product_key) AS unique_products
# MAGIC FROM main.gold.fact_sales f
# MAGIC INNER JOIN main.gold.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.year, d.quarter, d.month, d.month_name, d.is_weekend
# MAGIC ORDER BY d.year DESC, d.month DESC;
# MAGIC
# MAGIC SELECT '✅ View vw_sales_by_period criada com precisão DECIMAL!' AS status;

# COMMAND ----------

# DBTITLE 1,View 5: Impacto Descontos
# MAGIC %sql
# MAGIC -- View: Análise de Impacto de Descontos
# MAGIC CREATE OR REPLACE VIEW main.gold.vw_discount_impact AS
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN f.discount = 0 THEN 'Sem Desconto'
# MAGIC         WHEN f.discount <= 0.05 THEN '1-5%'
# MAGIC         WHEN f.discount <= 0.10 THEN '6-10%'
# MAGIC         WHEN f.discount <= 0.15 THEN '11-15%'
# MAGIC         WHEN f.discount <= 0.20 THEN '16-20%'
# MAGIC         ELSE '20%+'
# MAGIC     END AS discount_range,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     SUM(f.quantity) AS total_quantity,
# MAGIC     CAST(SUM(f.line_total) AS DECIMAL(15,2)) AS total_revenue,
# MAGIC     CAST(AVG(f.line_total) AS DECIMAL(12,2)) AS avg_transaction_value,
# MAGIC     CAST(SUM(f.discount * f.unit_price * f.quantity) AS DECIMAL(15,2)) AS total_discount_given
# MAGIC FROM main.gold.fact_sales f
# MAGIC GROUP BY discount_range
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC SELECT '✅ View vw_discount_impact criada com precisão DECIMAL!' AS status;

# COMMAND ----------

# DBTITLE 1,View 6: Top Produtos
# MAGIC %sql
# MAGIC -- View: Top Produtos Mais Vendidos
# MAGIC CREATE OR REPLACE VIEW main.gold.vw_top_products AS
# MAGIC SELECT 
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     p.category_name,
# MAGIC     p.price_category,
# MAGIC     p.unit_price AS catalog_price,
# MAGIC     COUNT(DISTINCT f.order_id) AS times_ordered,
# MAGIC     SUM(f.quantity) AS total_units_sold,
# MAGIC     CAST(SUM(f.line_total) AS DECIMAL(15,2)) AS total_revenue,
# MAGIC     CAST(AVG(f.unit_price) AS DECIMAL(10,2)) AS avg_selling_price,
# MAGIC     CAST(AVG(f.discount) AS DECIMAL(5,4)) AS avg_discount_rate
# MAGIC FROM main.gold.fact_sales f
# MAGIC INNER JOIN main.gold.dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY p.product_id, p.product_name, p.category_name, p.price_category, p.unit_price
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC SELECT '✅ View vw_top_products criada com precisão DECIMAL!' AS status;

# COMMAND ----------

# DBTITLE 1,Validação Final
# MAGIC %sql
# MAGIC -- Validação: Listar todas as views criadas
# MAGIC SELECT 
# MAGIC     '1. vw_sales_by_category' AS view_name,
# MAGIC     'Vendas por categoria (mensal)' AS description
# MAGIC UNION ALL
# MAGIC SELECT '2. vw_employee_performance', 'Performance de vendedores'
# MAGIC UNION ALL
# MAGIC SELECT '3. vw_top_customers', 'Top clientes por receita'
# MAGIC UNION ALL
# MAGIC SELECT '4. vw_sales_by_period', 'Análise temporal (ano/mês/trimestre)'
# MAGIC UNION ALL
# MAGIC SELECT '5. vw_discount_impact', 'Impacto dos descontos'
# MAGIC UNION ALL
# MAGIC SELECT '6. vw_top_products', 'Top produtos mais vendidos';
# MAGIC
# MAGIC -- Testar view mais importante
# MAGIC SELECT * FROM main.gold.vw_sales_by_category LIMIT 10;

# COMMAND ----------

