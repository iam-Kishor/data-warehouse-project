# 🚀 End-to-End Data Warehouse Project (SQL Server | Medallion Architecture)

## 📌 Project Summary

Designed and implemented a **production-style data warehouse** using **SQL Server**, transforming raw CRM & ERP data into **analytics-ready datasets** through a structured **Medallion Architecture (Bronze → Silver → Gold)**.

This project simulates a real-world data engineering pipeline, focusing on **data ingestion, transformation, modeling, and performance optimization**.

---

## 🧠 Business Objective

Enable data-driven decision-making by building a centralized warehouse to answer:

* 📊 What are the top-performing products?
* 👥 Which customers generate the highest revenue?
* 📈 How do sales trends vary over time?
* 🌍 Which regions contribute most to business growth?

---

## 🏗️ Architecture Overview

### 🥉 Bronze Layer (Raw Ingestion)

* Loaded raw CSV data from **CRM & ERP systems**
* Used **BULK INSERT with batch processing**
* Preserved source data for auditability
* Designed schema with flexible datatypes for ingestion reliability

---

### 🥈 Silver Layer (Data Transformation)

* Applied **data cleaning & standardization**
* Removed duplicates and handled null values
* Fixed datatype inconsistencies
* Implemented business rules for data consistency
* Prepared conformed datasets for modeling

---

### 🥇 Gold Layer (Data Modeling)

* Designed **Star Schema**
* Created:

  * `fact_sales`
  * `dim_customers`
  * `dim_products`
* Optimized for analytical queries and BI tools

---

## ⚙️ Tech Stack

| Category     | Tools                          |
| ------------ | ------------------------------ |
| Database     | SQL Server                     |
| Language     | T-SQL                          |
| ETL          | Stored Procedures, BULK INSERT |
| Data Sources | CSV (CRM & ERP systems)        |
| Tools        | SSMS, GitHub                   |

---

## 📊 Project Highlights

| Metric | Value |
|------|------|
| Architecture | Medallion (Bronze–Silver–Gold) |
| Data Sources | 2+ |
| ETL Pipelines | End-to-End |
| Data Model | Star Schema |
| SQL Queries | 50+ |
| Layers | 3 |
| Use Case | Analytics & Reporting |
| Level | Intermediate → Advanced |

## 🔄 Data Pipeline Workflow

```id="q7l1z4"
Raw CSV Files → Bronze (Ingestion) → Silver (Cleaning & Transformation) → Gold (Star Schema) → Analytics / BI
```

---

## 📂 Project Structure

```id="b3glhf"
data-warehouse-project/
│
├── datasets/                  # Source files (CRM & ERP)
│
├── scripts/
│   ├── bronze/
│   │   ├── ddl_bronze.sql
│   │   ├── data_load_procedure.sql
│   │   └── only_data_load.sql
│   │
│   ├── silver/
│   └── gold/
│
└── README.md
```

---

## 🔥 Key Engineering Highlights

* 🚀 Implemented **batch data ingestion** using stored procedures
* ⚡ Optimized loading using `TABLOCK` for performance
* 🧹 Built robust **data cleaning pipelines** in Silver layer
* 🧱 Designed scalable **star schema for analytics**
* 🐞 Debugged real-world issues:

  * Data type mismatches
  * Encoding & bulk load errors
  * File path & permission issues
* 🛠 Applied **error handling using TRY...CATCH**

---

## 📊 Example Analytical Queries

```sql id="3o2h8r"
-- Total Revenue
SELECT SUM(sales_amount) AS total_revenue
FROM gold.fact_sales;

-- Top 5 Products
SELECT TOP 5 product_name, SUM(sales_amount) AS revenue
FROM gold.fact_sales
GROUP BY product_name
ORDER BY revenue DESC;

-- Customer Lifetime Value
SELECT customer_id, SUM(sales_amount) AS CLV
FROM gold.fact_sales
GROUP BY customer_id;
```

---

## 🧠 Skills Demonstrated

* Data Warehousing Concepts (Medallion Architecture)
* SQL Development (Advanced Joins, Aggregations, ETL Logic)
* Data Modeling (Star Schema Design)
* Data Cleaning & Transformation
* Performance Optimization (Bulk Operations)
* Debugging & Error Handling in SQL

---

## ⚠️ Challenges & Solutions

| Challenge          | Solution                          |
| ------------------ | --------------------------------- |
| Bulk insert errors | Adjusted datatypes & encoding     |
| File access issues | Validated paths & permissions     |
| Dirty source data  | Implemented Silver layer cleaning |
| Schema mismatches  | Used flexible Bronze schema       |

---

## 🚀 How to Execute

1. Clone the repository:

```bash id="9r6yxa"
git clone https://github.com/iam-Kishor/data-warehouse-project.git
```

2. Run Bronze scripts:

```sql id="g1c0j1"
EXEC bronze.batchload;
```

3. Execute Silver & Gold scripts sequentially

---

---

## 🙋‍♂️ About Me

Data Analyst transitioning into Data Engineering, building end-to-end data warehouse projects using SQL Server and Medallion Architecture.

**Focused on ETL pipelines, data modeling, and transforming raw data into meaningful business insights.**

---

## ⭐ Support

If you found this project useful, consider giving it a ⭐ on GitHub!

---
