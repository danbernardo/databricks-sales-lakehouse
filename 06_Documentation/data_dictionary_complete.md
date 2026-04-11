# 📖 Dicionário de Dados - Sales Data Lakehouse

**Projeto**: Sales Data Lakehouse com Arquitetura Medalhão  
**Total de Tabelas/Views**: 27 (8 Bronze + 8 Silver + 5 Gold + 6 Views)  
**Última Atualização**: Abril 2026  

---

## 📑 Índice

* [Camada BRONZE (8 tabelas)](#camada-bronze)
* [Camada SILVER (8 tabelas)](#camada-silver)
* [Camada GOLD - Dimensões e Fato (5 tabelas)](#camada-gold)
* [Camada GOLD - Views Agregadas (6 views)](#camada-gold-views)
* [Convenções e Boas Práticas](#convenções)

---

## 🥉 Camada BRONZE

**Objetivo**: Armazenar dados brutos exatamente como vieram da origem (CSV), com metadados de controle.

**Características**:
- Formato: Delta Lake
- Schema: `main.bronze`
- Metadados: `ingestion_timestamp`, `source_system`, `file_name`
- Mode: Overwrite (carga full)


### `main.bronze.categories` ✅

**Origem**: `/Volumes/main/bronze/sales_data/categories.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `CategoryID` | `int` |
| `CategoryName` | `string` |
| `EnglishDescription` | `string` |
| `PortugueseDescription` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

### `main.bronze.customers` ✅

**Origem**: `/Volumes/main/bronze/sales_data/customers.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `CustomerID` | `string` |
| `CompanyName` | `string` |
| `ContactName` | `string` |
| `ContactTitle` | `string` |
| `Address` | `string` |
| `City` | `string` |
| `Region` | `string` |
| `PostalCode` | `string` |
| `Country` | `string` |
| `Phone` | `string` |
| `Fax` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

### `main.bronze.employees` ✅

**Origem**: `/Volumes/main/bronze/sales_data/employees.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `EmployeeID` | `int` |
| `LastName` | `string` |
| `FirstName` | `string` |
| `Address` | `string` |
| `City` | `string` |
| `Province` | `string` |
| `PostalCode` | `string` |
| `Phone` | `bigint` |
| `BirthDate` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

### `main.bronze.products` ✅

**Origem**: `/Volumes/main/bronze/sales_data/products.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `ProductID` | `int` |
| `SupplierID` | `int` |
| `CategoryID` | `int` |
| `ProductName` | `string` |
| `EnglishName` | `string` |
| `QuantityPerUnit` | `string` |
| `UnitPrice` | `double` |
| `UnitsInStock` | `int` |
| `UnitsOnOrder` | `int` |
| `ReorderLevel` | `int` |
| `Discontinued` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

### `main.bronze.suppliers` ✅

**Origem**: `/Volumes/main/bronze/sales_data/suppliers.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `SupplierID` | `int` |
| `Name` | `string` |
| `Address` | `string` |
| `City` | `string` |
| `Province` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

### `main.bronze.shippers` ✅

**Origem**: `/Volumes/main/bronze/sales_data/shippers.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `ShipperID` | `int` |
| `CompanyName` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

### `main.bronze.orders` ✅

**Origem**: `/Volumes/main/bronze/sales_data/orders.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `OrderID` | `string` |
| `CustomerID` | `string` |
| `EmployeeID` | `string` |
| `ShipName` | `string` |
| `ShipAddress` | `string` |
| `ShipCity` | `string` |
| `ShipRegion` | `string` |
| `ShipPostalCode` | `string` |
| `ShipCountry` | `string` |
| `ShipperID` | `double` |
| `OrderDate` | `string` |
| `RequiredDate` | `string` |
| `ShippedDate` | `string` |
| `Freight` | `double` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

### `main.bronze.orderdetails` ✅

**Origem**: `/Volumes/main/bronze/sales_data/orderdetails.csv`  
**Formato**: Delta Lake  

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `OrderDetailsID` | `int` |
| `OrderID` | `int` |
| `ProductID` | `int` |
| `UnitPrice` | `double` |
| `Quantity` | `int` |
| `Discount` | `double` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |

---

## 🥈 Camada SILVER

**Objetivo**: Dados limpos, validados e enriquecidos com regras de negócio.

**Características**:
- Origem: Tabelas Bronze
- Transformações: Limpeza, validação, cálculos
- Flag de qualidade: `data_quality_status` (VALID/INVALID)
- Formato: Delta Lake
- Schema: `main.silver`


### `main.silver.categories` ✅

**Origem**: `main.bronze.categories`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `CategoryID` | `int` |
| `CategoryName` | `string` |
| `EnglishDescription` | `string` |
| `PortugueseDescription` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |
| `data_quality_status` | `string` |
| `processing_timestamp` | `timestamp` |

---

### `main.silver.customers` ✅

**Origem**: `main.bronze.customers`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `CustomerID` | `string` |
| `CompanyName` | `string` |
| `ContactName` | `string` |
| `ContactTitle` | `string` |
| `Address` | `string` |
| `City` | `string` |
| `Region` | `string` |
| `PostalCode` | `string` |
| `Country` | `string` |
| `Phone` | `string` |
| `Fax` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| ... | *(+ 3 colunas)* |

---

### `main.silver.employees` ✅

**Origem**: `main.bronze.employees`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `EmployeeID` | `int` |
| `LastName` | `string` |
| `FirstName` | `string` |
| `Address` | `string` |
| `City` | `string` |
| `Province` | `string` |
| `PostalCode` | `string` |
| `Phone` | `bigint` |
| `BirthDate` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |
| `data_quality_status` | `string` |
| ... | *(+ 1 colunas)* |

---

### `main.silver.products` ✅

**Origem**: `main.bronze.products`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `ProductID` | `int` |
| `SupplierID` | `int` |
| `CategoryID` | `int` |
| `ProductName` | `string` |
| `EnglishName` | `string` |
| `QuantityPerUnit` | `string` |
| `UnitPrice` | `decimal(10,2)` |
| `UnitsInStock` | `int` |
| `UnitsOnOrder` | `int` |
| `ReorderLevel` | `int` |
| `Discontinued` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| ... | *(+ 5 colunas)* |

---

### `main.silver.suppliers` ✅

**Origem**: `main.bronze.suppliers`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `SupplierID` | `int` |
| `Name` | `string` |
| `Address` | `string` |
| `City` | `string` |
| `Province` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |
| `data_quality_status` | `string` |
| `processing_timestamp` | `timestamp` |

---

### `main.silver.shippers` ✅

**Origem**: `main.bronze.shippers`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `ShipperID` | `int` |
| `CompanyName` | `string` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |
| `data_quality_status` | `string` |
| `processing_timestamp` | `timestamp` |

---

### `main.silver.orders` ✅

**Origem**: `main.bronze.orders`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `OrderID` | `string` |
| `CustomerID` | `string` |
| `EmployeeID` | `string` |
| `ShipName` | `string` |
| `ShipAddress` | `string` |
| `ShipCity` | `string` |
| `ShipRegion` | `string` |
| `ShipPostalCode` | `string` |
| `ShipCountry` | `string` |
| `ShipperID` | `double` |
| `OrderDate` | `date` |
| `RequiredDate` | `string` |
| `ShippedDate` | `date` |
| `Freight` | `double` |
| `CreateDate` | `string` |
| ... | *(+ 8 colunas)* |

---

### `main.silver.orderdetails` ✅

**Origem**: `main.bronze.orderdetails`  
**Transformações**: Limpeza, validação, enriquecimento  

**Colunas Principais**:

| Coluna | Tipo |
|--------|------|
| `OrderDetailsID` | `int` |
| `OrderID` | `int` |
| `ProductID` | `int` |
| `UnitPrice` | `decimal(10,2)` |
| `Quantity` | `int` |
| `Discount` | `decimal(5,4)` |
| `CreateDate` | `string` |
| `UpdateDate` | `string` |
| `ingestion_timestamp` | `timestamp` |
| `source_system` | `string` |
| `file_name` | `string` |
| `line_total` | `decimal(12,2)` |
| `data_quality_status` | `string` |
| `processing_timestamp` | `timestamp` |

---

## 🥇 Camada GOLD - Dimensões e Fato

**Objetivo**: Modelo dimensional Star Schema otimizado para analytics e BI.

**Características**:
- Modelo: Star Schema
- Surrogate Keys: Chaves substitutas geradas (`_key`)
- Natural Keys: IDs originais preservados (`_id`)
- Formato: Delta Lake
- Schema: `main.gold`


### `main.gold.dim_customer` 🟡

**Tipo**: Dimensão  

**Colunas**:

| Coluna | Tipo | Chave |
|--------|------|-------|
| `customer_id` | `string` | NK |
| `company_name` | `string` |  |
| `contact_name` | `string` |  |
| `country` | `string` |  |
| `city` | `string` |  |
| `region` | `string` |  |
| `phone` | `string` |  |
| `effective_date` | `timestamp` |  |
| `customer_key` | `bigint` | **PK** |

---

### `main.gold.dim_product` 🟡

**Tipo**: Dimensão  

**Colunas**:

| Coluna | Tipo | Chave |
|--------|------|-------|
| `product_id` | `int` | NK |
| `product_name` | `string` |  |
| `category_name` | `string` |  |
| `unit_price` | `decimal(10,2)` |  |
| `price_category` | `string` |  |
| `is_available` | `boolean` |  |
| `effective_date` | `timestamp` |  |
| `product_key` | `bigint` | **PK** |

---

### `main.gold.dim_employee` 🟡

**Tipo**: Dimensão  

**Colunas**:

| Coluna | Tipo | Chave |
|--------|------|-------|
| `employee_id` | `int` | NK |
| `full_name` | `string` |  |
| `city` | `string` |  |
| `province` | `string` |  |
| `phone` | `bigint` |  |
| `effective_date` | `timestamp` |  |
| `employee_key` | `bigint` | **PK** |

---

### `main.gold.dim_date` 🟡

**Tipo**: Dimensão  

**Colunas**:

| Coluna | Tipo | Chave |
|--------|------|-------|
| `full_date` | `timestamp` |  |
| `date_key` | `string` | **PK** |
| `year` | `string` |  |
| `quarter` | `string` |  |
| `month` | `string` |  |
| `month_name` | `string` |  |
| `day` | `string` |  |
| `day_of_week` | `string` |  |
| `day_name` | `string` |  |
| `week_of_year` | `string` |  |
| `is_weekend` | `boolean` |  |
| `effective_date` | `timestamp` |  |

---

### `main.gold.fact_sales` 🔴

**Tipo**: Tabela Fato  
**Granularidade**: 1 linha = 1 item de pedido (OrderID + ProductID)  

**Colunas**:

| Coluna | Tipo | Chave |
|--------|------|-------|
| `date_key` | `string` | FK |
| `customer_key` | `bigint` | FK |
| `product_key` | `bigint` | FK |
| `employee_key` | `bigint` | FK |
| `order_id` | `int` | NK |
| `quantity` | `int` |  |
| `unit_price` | `decimal(10,2)` |  |
| `discount` | `decimal(5,4)` |  |
| `line_total` | `decimal(12,2)` |  |
| `load_timestamp` | `timestamp` |  |
| `sales_key` | `bigint` | **PK** |

---

## 🥇 Camada GOLD - Views Agregadas

**Objetivo**: Consultas pré-agregadas otimizadas para dashboards e relatórios.

**Características**:
- Origem: `fact_sales` + Dimensões
- Métricas com DECIMAL para precisão monetária
- Otimizadas para BI (Power BI, Tableau)


### `main.gold.vw_sales_by_category` 📊

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `year` | `string` |
| `month` | `string` |
| `month_name` | `string` |
| `category_name` | `string` |
| `total_orders` | `bigint` |
| `total_quantity` | `bigint` |
| `total_revenue` | `decimal(15,2)` |
| `avg_line_value` | `decimal(12,2)` |
| `total_discount_amount` | `decimal(15,2)` |

---

### `main.gold.vw_employee_performance` 📊

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `employee_id` | `int` |
| `full_name` | `string` |
| `city` | `string` |
| `total_orders` | `bigint` |
| `total_units_sold` | `bigint` |
| `total_revenue` | `decimal(15,2)` |
| `avg_transaction_value` | `decimal(12,2)` |
| `avg_order_value` | `decimal(12,2)` |
| `max_transaction` | `decimal(12,2)` |

---

### `main.gold.vw_top_customers` 📊

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `customer_id` | `string` |
| `company_name` | `string` |
| `country` | `string` |
| `city` | `string` |
| `total_orders` | `bigint` |
| `total_units_purchased` | `bigint` |
| `total_spent` | `decimal(15,2)` |
| `avg_transaction_value` | `decimal(12,2)` |
| `first_purchase_date` | `timestamp` |
| `last_purchase_date` | `timestamp` |
| ... | *(+ 1 colunas)* |

---

### `main.gold.vw_sales_by_period` 📊

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `year` | `string` |
| `quarter` | `string` |
| `month` | `string` |
| `month_name` | `string` |
| `is_weekend` | `boolean` |
| `total_orders` | `bigint` |
| `total_quantity` | `bigint` |
| `total_revenue` | `decimal(15,2)` |
| `avg_transaction_value` | `decimal(12,2)` |
| `unique_customers` | `bigint` |
| ... | *(+ 1 colunas)* |

---

### `main.gold.vw_discount_impact` 📊

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `discount_range` | `string` |
| `total_orders` | `bigint` |
| `total_quantity` | `bigint` |
| `total_revenue` | `decimal(15,2)` |
| `avg_transaction_value` | `decimal(12,2)` |
| `total_discount_given` | `decimal(15,2)` |

---

### `main.gold.vw_top_products` 📊

**Colunas**:

| Coluna | Tipo |
|--------|------|
| `product_id` | `int` |
| `product_name` | `string` |
| `category_name` | `string` |
| `price_category` | `string` |
| `catalog_price` | `decimal(10,2)` |
| `times_ordered` | `bigint` |
| `total_units_sold` | `bigint` |
| `total_revenue` | `decimal(15,2)` |
| `avg_selling_price` | `decimal(10,2)` |
| `avg_discount_rate` | `decimal(5,4)` |

---

## 📊 Resumo Estatístico

| Camada | Quantidade | Descrição |
|--------|-----------|-----------|
| **Bronze** | 8 tabelas | Dados brutos com metadados |
| **Silver** | 8 tabelas | Dados limpos com qualidade |
| **Gold Dim** | 4 dimensões | Star Schema - Dimensões |
| **Gold Fact** | 1 fato | Star Schema - Fato Vendas |
| **Gold Views** | 6 views | Views agregadas para BI |
| **TOTAL** | **27 objetos** | Pipeline completo |

---

## 🔑 Convenções de Nomenclatura

### Chaves
* **PK (Primary Key)**: Chave primária (ex: `customer_key`)
* **FK (Foreign Key)**: Chave estrangeira (ex: `product_key` na fact)
* **NK (Natural Key)**: Chave original do sistema (ex: `customer_id`)

### Sufixos
* `_key`: Surrogate key (chave substituta gerada)
* `_id`: Natural key (ID original)
* `_timestamp`: Data/hora de controle
* `_status`: Flag de status ou qualidade

### Tipos de Dados
* **DECIMAL(p,s)**: Valores monetários com precisão fixa
* **STRING**: Texto variável
* **INT/BIGINT**: Números inteiros
* **TIMESTAMP**: Data e hora
* **BOOLEAN**: TRUE/FALSE

---

## 💡 Notas Importantes

### ✅ Precisão Numérica (DECIMAL)
**Todas as colunas monetárias usam tipo DECIMAL:**
* `unit_price` → DECIMAL(10,2)
* `discount` → DECIMAL(5,4)
* `line_total` → DECIMAL(12,2)
* Views agregadas → DECIMAL(15,2) ou DECIMAL(12,2)

❌ **EVITE DOUBLE** para valores monetários (causa erros como 0.150000006)

### 🔍 Flags de Qualidade
Tabelas Silver possuem `data_quality_status`:
* **VALID**: Passou todas validações
* **INVALID**: Possui problemas de qualidade

💡 **Recomendação**: Filtrar por `WHERE data_quality_status = 'VALID'`

---

## 🔗 Relacionamentos (Star Schema)

```
       dim_date (4.018)
            ↓ date_key
            │
dim_customer (91) → fact_sales (2.691) ← dim_employee (9)
            ↑ customer_key  ↑ employee_key
            │               │
       product_key          │
            │               │
       dim_product (77) ────┘
```

**Granularidade Fato**: 1 linha = 1 item de pedido (OrderID + ProductID)

---

## 👤 Informações do Documento

**Projeto**: Sales Data Lakehouse  
**Arquitetura**: Medallion (Bronze → Silver → Gold)  
**Plataforma**: Databricks Community Edition  
**Formato**: Delta Lake  
**Governança**: Unity Catalog (`main` catalog)  
**Versão**: 1.0  
**Última Atualização**: Abril 2026  

---

**📖 Fim do Dicionário de Dados**
