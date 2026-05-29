# Databricks notebook source
# MAGIC %pip install kaggle

# COMMAND ----------

import os

os.environ["KAGGLE_USERNAME"] = "rutujaavinashmuthe"
os.environ["KAGGLE_KEY"] = "KGAT_70c55e92f3c00b76ceb98187faf5836c"

# COMMAND ----------

#Download both Datasets
import subprocess
import os

save_path = "/Workspace/Users/kavya@cloudaeon.net/kaggle_data"
os.makedirs(save_path, exist_ok=True)

# Cosmetics Dataset
subprocess.run([
    "kaggle", "datasets", "download",
    "-d", "arbaaztamboli/retail-markdown-optimization-discounts-and-sales",
    "-p", save_path,
    "--unzip"
], check=True)

# Fashion Dataset
subprocess.run([
    "kaggle", "datasets", "download",
    "-d", "pratyushpuri/retail-fashion-boutique-data-sales-analytics-2025",
    "-p", save_path,
    "--unzip"
], check=True)

# COMMAND ----------

#Read both Datasets
import pandas as pd

cosmetics_df = pd.read_csv(
    save_path + "/SYNTHETIC Markdown Dataset.csv"
)

fashion_df = pd.read_csv(
    save_path + "/fashion_boutique_dataset.csv"
)

# COMMAND ----------

#verify exact files name using
os.listdir(save_path)

# COMMAND ----------

#convert to spark dataframes
spark_cosmetics = spark.createDataFrame(cosmetics_df)
spark_fashion = spark.createDataFrame(fashion_df)

# COMMAND ----------

#Standardize column names
spark_cosmetics = spark_cosmetics.toDF(
    *[c.replace(" ", "_").lower() for c in spark_cosmetics.columns]
)

spark_fashion = spark_fashion.toDF(
    *[c.replace(" ", "_").lower() for c in spark_fashion.columns]
)

# COMMAND ----------

# %sql
# -- create catalog
# CREATE CATALOG IF NOT EXISTS retail_catalog;

# COMMAND ----------

# %sql
# -- List all catalogs
# SHOW CATALOGS;

# COMMAND ----------

# %sql
# -- create schema
# CREATE SCHEMA IF NOT EXISTS retail_catalog.dynamic_markdown;

# COMMAND ----------

# %sql
# -- List all schema
# SHOW SCHEMAS;

# COMMAND ----------

# %sql
# USE retail_catalog.dynamic_markdown;

# COMMAND ----------

#create raw tables
spark_cosmetics.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("markdown.hackathon.cosmetics_raw")

spark_fashion.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("markdown.hackathon.fashion_raw")

# COMMAND ----------

# DBTITLE 1,Cell 14
#verify dataframes
display(spark_cosmetics)

display(spark_fashion)

# COMMAND ----------

#create raw tables
spark_cosmetics.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("markdown.hackathon.cosmetics_raw")

spark_fashion.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("markdown.hackathon.fashion_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- verify raw tables
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.cosmetics_raw
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- verify raw tables
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.fashion_raw
# MAGIC LIMIT 10;

# COMMAND ----------

print(spark_cosmetics.columns)

print(spark_fashion.columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- verify raw tables
# MAGIC SELECT COUNT(*) 
# MAGIC FROM markdown.hackathon.cosmetics_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- verify raw tables
# MAGIC SELECT COUNT(*) 
# MAGIC FROM markdown.hackathon.fashion_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check sample data
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.cosmetics_raw
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check sample data
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.fashion_raw
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 1 — dim_category

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DDL for category dim table
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_category (
# MAGIC     category_key BIGINT,
# MAGIC     category_name STRING,
# MAGIC     parent_category STRING,
# MAGIC     sub_category STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_category
# MAGIC SELECT DISTINCT
# MAGIC     monotonically_increasing_id() AS category_key,
# MAGIC     category AS category_name,
# MAGIC     NULL AS parent_category,
# MAGIC     NULL AS sub_category
# MAGIC FROM (
# MAGIC     SELECT category
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT category
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_category;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 2 — dim_brand

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_brand (
# MAGIC     brand_key BIGINT,
# MAGIC     brand_name STRING,
# MAGIC     brand_type STRING,
# MAGIC     country STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_brand
# MAGIC SELECT DISTINCT
# MAGIC     monotonically_increasing_id() AS brand_key,
# MAGIC     brand AS brand_name,
# MAGIC     NULL AS brand_type,
# MAGIC     NULL AS country
# MAGIC FROM (
# MAGIC     SELECT brand
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT brand
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_brand;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_brand;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 3 — dim_season

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_season (
# MAGIC     season_key BIGINT,
# MAGIC     season_name STRING,
# MAGIC     start_month INT,
# MAGIC     end_month INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_season
# MAGIC SELECT DISTINCT
# MAGIC     monotonically_increasing_id() AS season_key,
# MAGIC     season AS season_name,
# MAGIC     CASE
# MAGIC         WHEN lower(season) = 'summer' THEN 3
# MAGIC         WHEN lower(season) = 'monsoon' THEN 7
# MAGIC         WHEN lower(season) = 'winter' THEN 10
# MAGIC         WHEN lower(season) = 'festive' THEN 10
# MAGIC         ELSE NULL
# MAGIC     END AS start_month,
# MAGIC     CASE
# MAGIC         WHEN lower(season) = 'summer' THEN 6
# MAGIC         WHEN lower(season) = 'monsoon' THEN 9
# MAGIC         WHEN lower(season) = 'winter' THEN 2
# MAGIC         WHEN lower(season) = 'festive' THEN 12
# MAGIC         ELSE NULL
# MAGIC     END AS end_month
# MAGIC FROM (
# MAGIC     SELECT season
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT season
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_season;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_season;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 4 — dim_markdown

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_markdown (
# MAGIC     markdown_key BIGINT,
# MAGIC     markdown_percentage DECIMAL(10,2),
# MAGIC     markdown_type STRING,
# MAGIC     markdown_reason STRING,
# MAGIC     markdown_strategy STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_markdown
# MAGIC SELECT DISTINCT
# MAGIC     monotonically_increasing_id() AS markdown_key,
# MAGIC     markdown_percentage,
# MAGIC     CASE
# MAGIC         WHEN markdown_percentage <= 10 THEN 'Low Discount'
# MAGIC         WHEN markdown_percentage <= 30 THEN 'Medium Discount'
# MAGIC         ELSE 'High Discount'
# MAGIC     END AS markdown_type,
# MAGIC     'Dynamic Pricing Strategy' AS markdown_reason,
# MAGIC     CASE
# MAGIC         WHEN markdown_percentage <= 10 THEN 'Profit Optimization'
# MAGIC         WHEN markdown_percentage <= 30 THEN 'Sales Boost'
# MAGIC         ELSE 'Inventory Clearance'
# MAGIC     END AS markdown_strategy
# MAGIC FROM (
# MAGIC     SELECT markdown_1 AS markdown_percentage
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT markdown_2
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT markdown_3
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT markdown_4
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT markdown_percentage
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_markdown;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_markdown;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 5 — dim_price_range

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_price_range (
# MAGIC     price_range_key BIGINT,
# MAGIC     min_price DECIMAL(10,2),
# MAGIC     max_price DECIMAL(10,2),
# MAGIC     price_segment STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_price_range
# MAGIC VALUES
# MAGIC (1, 0.00, 500.00, 'Budget'),
# MAGIC (2, 501.00, 2000.00, 'Mid Range'),
# MAGIC (3, 2001.00, 10000.00, 'Premium');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_price_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_price_range;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 6 — dim_stock_level

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_stock_level (
# MAGIC     stock_level_key BIGINT,
# MAGIC     stock_status STRING,
# MAGIC     stock_bucket STRING,
# MAGIC     reorder_flag STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_stock_level
# MAGIC VALUES
# MAGIC (1, 'Low Stock', '0-50', 'Yes'),
# MAGIC (2, 'Medium Stock', '51-200', 'No'),
# MAGIC (3, 'Overstock', '201+', 'No');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_stock_level;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_stock_level;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 7 — dim_customer_rating

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_customer_rating (
# MAGIC     customer_rating_key BIGINT,
# MAGIC     customer_rating DECIMAL(3,2),
# MAGIC     rating_category STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_customer_rating
# MAGIC SELECT DISTINCT
# MAGIC     monotonically_increasing_id() AS customer_rating_key,
# MAGIC     customer_rating,
# MAGIC     CASE
# MAGIC         WHEN customer_rating <= 2 THEN 'Poor'
# MAGIC         WHEN customer_rating <= 3 THEN 'Average'
# MAGIC         WHEN customer_rating <= 4 THEN 'Good'
# MAGIC         ELSE 'Excellent'
# MAGIC     END AS rating_category
# MAGIC FROM (
# MAGIC     SELECT customer_ratings AS customer_rating
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     UNION
# MAGIC     SELECT customer_rating
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC )
# MAGIC WHERE customer_rating IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_customer_rating;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_customer_rating;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 8 — dim_promotion

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_promotion (
# MAGIC     promotion_key BIGINT,
# MAGIC     promotion_name STRING,
# MAGIC     promotion_type STRING,
# MAGIC     discount_type STRING,
# MAGIC     start_date DATE,
# MAGIC     end_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_promotion
# MAGIC SELECT DISTINCT
# MAGIC     monotonically_increasing_id() AS promotion_key,
# MAGIC     promotion_type AS promotion_name,
# MAGIC     promotion_type,
# MAGIC     CASE
# MAGIC         WHEN lower(promotion_type) LIKE '%discount%' THEN 'Percentage'
# MAGIC         WHEN lower(promotion_type) LIKE '%clearance%' THEN 'Inventory Clearance'
# MAGIC         WHEN lower(promotion_type) LIKE '%combo%' THEN 'Combo Offer'
# MAGIC         ELSE 'General Promotion'
# MAGIC     END AS discount_type,
# MAGIC     NULL AS start_date,
# MAGIC     NULL AS end_date
# MAGIC FROM markdown.hackathon.cosmetics_raw
# MAGIC WHERE promotion_type IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_promotion;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_promotion;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 9 — dim_store

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_store (
# MAGIC     store_key BIGINT,
# MAGIC     store_name STRING,
# MAGIC     store_type STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_store
# MAGIC VALUES
# MAGIC (1, 'Online Store', 'E-Commerce', 'Pune', 'Maharashtra', 'India'),
# MAGIC (2, 'Fashion Retail Store', 'Retail', 'Mumbai', 'Maharashtra', 'India'),
# MAGIC (3, 'Cosmetics Outlet', 'Retail', 'Bangalore', 'Karnataka', 'India');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_store;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_store;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 10 — dim_channel

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_channel (
# MAGIC     channel_key BIGINT,
# MAGIC     channel_name STRING,
# MAGIC     channel_type STRING,
# MAGIC     platform STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_channel
# MAGIC VALUES
# MAGIC (1, 'Website', 'Online', 'Web'),
# MAGIC (2, 'Mobile App', 'Digital', 'Android/iOS'),
# MAGIC (3, 'Retail Store', 'Offline', 'Physical Store');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_channel;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_channel;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 11 — dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_date (
# MAGIC     date_key BIGINT,
# MAGIC     full_date DATE,
# MAGIC     day INT,
# MAGIC     month INT,
# MAGIC     month_name STRING,
# MAGIC     quarter INT,
# MAGIC     year INT,
# MAGIC     week_of_year INT,
# MAGIC     day_of_week STRING,
# MAGIC     is_weekend STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_date
# MAGIC SELECT
# MAGIC     CAST(date_format(dt, 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC     dt AS full_date,
# MAGIC     day(dt) AS day,
# MAGIC     month(dt) AS month,
# MAGIC     date_format(dt, 'MMMM') AS month_name,
# MAGIC     quarter(dt) AS quarter,
# MAGIC     year(dt) AS year,
# MAGIC     weekofyear(dt) AS week_of_year,
# MAGIC     date_format(dt, 'EEEE') AS day_of_week,
# MAGIC     CASE
# MAGIC         WHEN dayofweek(dt) IN (1,7) THEN 'Yes'
# MAGIC         ELSE 'No'
# MAGIC     END AS is_weekend
# MAGIC FROM (
# MAGIC     SELECT explode(
# MAGIC         sequence(
# MAGIC             to_date('2024-01-01'),
# MAGIC             to_date('2025-12-31'),
# MAGIC             interval 1 day
# MAGIC         )
# MAGIC     ) AS dt
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_date
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_date;

# COMMAND ----------

# MAGIC %md
# MAGIC TABLE 12 — dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_customer (
# MAGIC     customer_key BIGINT,
# MAGIC     customer_id STRING,
# MAGIC     customer_name STRING,
# MAGIC     gender STRING,
# MAGIC     age_group STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.dim_customer
# MAGIC VALUES
# MAGIC (1, 'CUST001', 'Aarav Sharma', 'Male', '18-25', 'Pune', 'Maharashtra', 'India'),
# MAGIC (2, 'CUST002', 'Priya Verma', 'Female', '26-35', 'Mumbai', 'Maharashtra', 'India'),
# MAGIC (3, 'CUST003', 'Rohan Mehta', 'Male', '26-35', 'Bangalore', 'Karnataka', 'India'),
# MAGIC (4, 'CUST004', 'Sneha Iyer', 'Female', '18-25', 'Hyderabad', 'Telangana', 'India'),
# MAGIC (5, 'CUST005', 'Karan Patel', 'Male', '36-45', 'Delhi', 'Delhi', 'India');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.dim_customer;

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC FACT TABLE 1 — fact_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.fact_sales (
# MAGIC     sales_key BIGINT,
# MAGIC     product_key BIGINT,
# MAGIC     category_key BIGINT,
# MAGIC     brand_key BIGINT,
# MAGIC     season_key BIGINT,
# MAGIC     markdown_key BIGINT,
# MAGIC     customer_rating_key BIGINT,
# MAGIC     price_range_key BIGINT,
# MAGIC     stock_level_key BIGINT,
# MAGIC     promotion_key BIGINT,
# MAGIC     store_key BIGINT,
# MAGIC     channel_key BIGINT,
# MAGIC     date_key BIGINT,
# MAGIC     original_price DECIMAL(10,2),
# MAGIC     current_price DECIMAL(10,2),
# MAGIC     markdown_percentage DECIMAL(10,2),
# MAGIC     historical_sales INT,
# MAGIC     sales_after_markdown INT,
# MAGIC     revenue DECIMAL(12,2),
# MAGIC     optimal_discount DECIMAL(10,2),
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 87
# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.fact_sales
# MAGIC
# MAGIC SELECT
# MAGIC     monotonically_increasing_id() AS sales_key,
# MAGIC     NULL AS product_key,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     ds.season_key,
# MAGIC     dm.markdown_key,
# MAGIC     dcr.customer_rating_key,
# MAGIC     dpr.price_range_key,
# MAGIC     dsl.stock_level_key,
# MAGIC     1 AS promotion_key,
# MAGIC     1 AS store_key,
# MAGIC     1 AS channel_key,
# MAGIC
# MAGIC     CAST(date_format(current_date(), 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC     src.original_price,
# MAGIC     COALESCE(src.current_price, src.original_price) AS current_price,
# MAGIC     src.markdown_percentage,
# MAGIC     src.historical_sales,
# MAGIC     src.sales_after_markdown,
# MAGIC     (
# MAGIC         COALESCE(src.current_price, src.original_price)
# MAGIC         * src.sales_after_markdown
# MAGIC     ) AS revenue,
# MAGIC     src.optimal_discount,
# MAGIC     current_timestamp()
# MAGIC FROM (
# MAGIC
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         category,
# MAGIC         brand,
# MAGIC         season,
# MAGIC         original_price,
# MAGIC         NULL AS current_price,
# MAGIC         markdown_1 AS markdown_percentage,
# MAGIC         historical_sales,
# MAGIC         sales_after_m1 AS sales_after_markdown,
# MAGIC         optimal_discount,
# MAGIC         stock_level,
# MAGIC         customer_ratings
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         category,
# MAGIC         brand,
# MAGIC         season,
# MAGIC         original_price,
# MAGIC         current_price,
# MAGIC         markdown_percentage,
# MAGIC         0 AS historical_sales,
# MAGIC         1 AS sales_after_markdown,
# MAGIC         markdown_percentage AS optimal_discount,
# MAGIC         stock_quantity AS stock_level,
# MAGIC         customer_rating AS customer_ratings
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC ) src
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_category dc
# MAGIC ON src.category = dc.category_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_brand db
# MAGIC ON src.brand = db.brand_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_season ds
# MAGIC ON src.season = ds.season_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_customer_rating dcr
# MAGIC ON src.customer_ratings = dcr.customer_rating
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_markdown dm
# MAGIC ON src.markdown_percentage = dm.markdown_percentage
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_price_range dpr
# MAGIC ON src.original_price BETWEEN dpr.min_price AND dpr.max_price
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_stock_level dsl
# MAGIC ON (
# MAGIC     CASE
# MAGIC         WHEN src.stock_level <= 50 THEN 'Low Stock'
# MAGIC         WHEN src.stock_level <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END
# MAGIC ) = dsl.stock_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.fact_sales
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC FACT TABLE 2 — fact_inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.fact_inventory (
# MAGIC     inventory_key BIGINT,
# MAGIC     product_key BIGINT,
# MAGIC     category_key BIGINT,
# MAGIC     brand_key BIGINT,
# MAGIC     season_key BIGINT,
# MAGIC     stock_level_key BIGINT,
# MAGIC     store_key BIGINT,
# MAGIC     date_key BIGINT,
# MAGIC     stock_quantity INT,
# MAGIC     stock_status STRING,
# MAGIC     reorder_required STRING,
# MAGIC     inventory_value DECIMAL(12,2),
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 92
# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.fact_inventory
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     monotonically_increasing_id() AS inventory_key,
# MAGIC
# MAGIC     NULL AS product_key,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     ds.season_key,
# MAGIC     dsl.stock_level_key,
# MAGIC
# MAGIC     1 AS store_key,
# MAGIC
# MAGIC     CAST(date_format(current_date(), 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC
# MAGIC     src.stock_quantity,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN src.stock_quantity <= 50 THEN 'Low Stock'
# MAGIC         WHEN src.stock_quantity <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END AS stock_status,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN src.stock_quantity <= 50 THEN 'Yes'
# MAGIC         ELSE 'No'
# MAGIC     END AS reorder_required,
# MAGIC
# MAGIC     (
# MAGIC         src.stock_quantity * src.original_price
# MAGIC     ) AS inventory_value,
# MAGIC
# MAGIC     current_timestamp()
# MAGIC
# MAGIC FROM (
# MAGIC
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         category,
# MAGIC         brand,
# MAGIC         season,
# MAGIC         stock_level AS stock_quantity,
# MAGIC         original_price
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         category,
# MAGIC         brand,
# MAGIC         season,
# MAGIC         stock_quantity,
# MAGIC         original_price
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC ) src
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_category dc
# MAGIC ON src.category = dc.category_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_brand db
# MAGIC ON src.brand = db.brand_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_season ds
# MAGIC ON src.season = ds.season_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_stock_level dsl
# MAGIC ON (
# MAGIC     CASE
# MAGIC         WHEN src.stock_quantity <= 50 THEN 'Low Stock'
# MAGIC         WHEN src.stock_quantity <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END
# MAGIC ) = dsl.stock_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.fact_inventory
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.fact_inventory;

# COMMAND ----------

# MAGIC %md
# MAGIC FACT TABLE 3 — fact_markdown_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.fact_markdown_analytics (
# MAGIC     markdown_fact_key BIGINT,
# MAGIC     product_key BIGINT,
# MAGIC     category_key BIGINT,
# MAGIC     brand_key BIGINT,
# MAGIC     season_key BIGINT,
# MAGIC     markdown_key BIGINT,
# MAGIC     promotion_key BIGINT,
# MAGIC     stock_level_key BIGINT,
# MAGIC     customer_rating_key BIGINT,
# MAGIC     date_key BIGINT,
# MAGIC     markdown_1 DECIMAL(10,2),
# MAGIC     markdown_2 DECIMAL(10,2),
# MAGIC     markdown_3 DECIMAL(10,2),
# MAGIC     markdown_4 DECIMAL(10,2),
# MAGIC     optimal_discount DECIMAL(10,2),
# MAGIC     historical_sales INT,
# MAGIC     sales_after_m1 INT,
# MAGIC     sales_after_m2 INT,
# MAGIC     sales_after_m3 INT,
# MAGIC     sales_after_m4 INT,
# MAGIC     seasonality_factor DECIMAL(10,2),
# MAGIC     return_rate DECIMAL(10,2),
# MAGIC     markdown_effectiveness STRING,
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 97
# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.fact_markdown_analytics
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC     ROW_NUMBER() OVER (ORDER BY src.category) AS markdown_fact_key,
# MAGIC     
# MAGIC     NULL AS product_key,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     ds.season_key,
# MAGIC     dm.markdown_key,
# MAGIC     dpr.promotion_key,
# MAGIC     dsl.stock_level_key,
# MAGIC     dcr.customer_rating_key,
# MAGIC
# MAGIC     CAST(date_format(current_date(), 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC
# MAGIC     src.markdown_1,
# MAGIC     src.markdown_2,
# MAGIC     src.markdown_3,
# MAGIC     src.markdown_4,
# MAGIC
# MAGIC     src.optimal_discount,
# MAGIC
# MAGIC     src.historical_sales,
# MAGIC
# MAGIC     src.sales_after_m1,
# MAGIC     src.sales_after_m2,
# MAGIC     src.sales_after_m3,
# MAGIC     src.sales_after_m4,
# MAGIC
# MAGIC     src.seasonality_factor,
# MAGIC
# MAGIC     src.return_rate,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN src.sales_after_m4 > src.historical_sales
# MAGIC         THEN 'High Impact'
# MAGIC
# MAGIC         WHEN src.sales_after_m2 > src.historical_sales
# MAGIC         THEN 'Moderate Impact'
# MAGIC
# MAGIC         ELSE 'Low Impact'
# MAGIC     END AS markdown_effectiveness,
# MAGIC
# MAGIC     current_timestamp()
# MAGIC FROM markdown.hackathon.cosmetics_raw src
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_category dc
# MAGIC ON src.category = dc.category_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_brand db
# MAGIC ON src.brand = db.brand_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_season ds
# MAGIC ON src.season = ds.season_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_markdown dm
# MAGIC ON src.markdown_1 = dm.markdown_percentage
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_promotion dpr
# MAGIC ON src.promotion_type = dpr.promotion_type
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_stock_level dsl
# MAGIC ON (
# MAGIC     CASE
# MAGIC         WHEN src.stock_level <= 50 THEN 'Low Stock'
# MAGIC         WHEN src.stock_level <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END
# MAGIC ) = dsl.stock_status
# MAGIC LEFT JOIN markdown.hackathon.dim_customer_rating dcr
# MAGIC ON src.customer_ratings = dcr.customer_rating;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.fact_markdown_analytics
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.fact_markdown_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC FACT TABLE 4 — fact_customer_feedback

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.fact_customer_feedback (
# MAGIC     feedback_key BIGINT,
# MAGIC     product_key BIGINT,
# MAGIC     category_key BIGINT,
# MAGIC     brand_key BIGINT,
# MAGIC     customer_rating_key BIGINT,
# MAGIC     markdown_key BIGINT,
# MAGIC     date_key BIGINT,
# MAGIC     customer_rating DECIMAL(3,2),
# MAGIC     is_returned STRING,
# MAGIC     return_reason STRING,
# MAGIC     feedback_category STRING,
# MAGIC     created_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 102
# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.fact_customer_feedback
# MAGIC SELECT
# MAGIC     monotonically_increasing_id() AS feedback_key,
# MAGIC     NULL AS product_key,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     dcr.customer_rating_key,
# MAGIC     dm.markdown_key,
# MAGIC     CAST(date_format(current_date(), 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC     src.customer_rating,
# MAGIC     src.is_returned,
# MAGIC     src.return_reason,
# MAGIC     CASE
# MAGIC         WHEN src.customer_rating <= 2 THEN 'Negative Feedback'
# MAGIC         WHEN src.customer_rating <= 4 THEN 'Neutral Feedback'
# MAGIC         ELSE 'Positive Feedback'
# MAGIC     END AS feedback_category,
# MAGIC     current_timestamp()
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         product_id,
# MAGIC         category,
# MAGIC         brand,
# MAGIC         customer_rating,
# MAGIC         is_returned,
# MAGIC         return_reason,
# MAGIC         markdown_percentage
# MAGIC     FROM markdown.hackathon.fashion_raw
# MAGIC ) src
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_category dc
# MAGIC ON src.category = dc.category_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_brand db
# MAGIC ON src.brand = db.brand_name
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_customer_rating dcr
# MAGIC ON src.customer_rating = dcr.customer_rating
# MAGIC
# MAGIC LEFT JOIN markdown.hackathon.dim_markdown dm
# MAGIC ON src.markdown_percentage = dm.markdown_percentage;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM markdown.hackathon.fact_customer_feedback
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM markdown.hackathon.fact_customer_feedback;

# COMMAND ----------

# %sql
# -- DIMENSION TABLES:

# -- DIM_CATEGORY
# CREATE TABLE IF NOT EXISTS dim_category (
#     category_key BIGINT,
#     category_name STRING,
#     parent_category STRING,
#     sub_category STRING
# )
# USING DELTA;

# -- DIM_BRAND
# CREATE TABLE IF NOT EXISTS dim_brand (
#     brand_key BIGINT,
#     brand_name STRING,
#     brand_type STRING,
#     country STRING
# )
# USING DELTA;

# -- DIM_SEASON
# CREATE TABLE IF NOT EXISTS dim_season (
#     season_key BIGINT,
#     season_name STRING,
#     start_month INT,
#     end_month INT
# )
# USING DELTA;

# -- DIM_PRICE_RANGE
# CREATE TABLE IF NOT EXISTS dim_price_range (
#     price_range_key BIGINT,
#     min_price DECIMAL(10,2),
#     max_price DECIMAL(10,2),
#     price_segment STRING
# )
# USING DELTA;

# -- DIM_STOCK_LEVEL
# CREATE TABLE IF NOT EXISTS dim_stock_level (
#     stock_level_key BIGINT,
#     stock_status STRING,
#     stock_bucket STRING,
#     reorder_flag STRING
# )
# USING DELTA;

# -- DIM_CUSTOMER_RATING
# CREATE TABLE IF NOT EXISTS dim_customer_rating (
#     customer_rating_key BIGINT,
#     rating DECIMAL(2,1),
#     rating_category STRING
# )
# USING DELTA;

# -- DIM_MARKDOWN
# CREATE TABLE IF NOT EXISTS dim_markdown (
#     markdown_key BIGINT,
#     markdown_percentage DECIMAL(5,2),
#     markdown_type STRING,
#     markdown_reason STRING,
#     markdown_strategy STRING
# )
# USING DELTA;

# -- DIM_STORE
# CREATE TABLE IF NOT EXISTS dim_store (
#     store_key BIGINT,
#     store_id STRING,
#     store_name STRING,
#     city STRING,
#     state STRING,
#     country STRING,
#     region STRING,
#     store_type STRING
# )
# USING DELTA;

# -- DIM_CUSTOMER
# CREATE TABLE IF NOT EXISTS dim_customer (
#     customer_key BIGINT,
#     customer_id STRING,
#     customer_name STRING,
#     gender STRING,
#     age INT,
#     city STRING,
#     state STRING,
#     country STRING,
#     loyalty_segment STRING,
#     signup_date DATE
# )
# USING DELTA;

# -- DIM_DATE
# CREATE TABLE IF NOT EXISTS dim_date (
#     date_key INT,
#     full_date DATE,
#     day INT,
#     month INT,
#     month_name STRING,
#     quarter STRING,
#     year INT,
#     week_number INT,
#     weekend_flag STRING,
#     festival_flag STRING
# )
# USING DELTA;

# -- DIM_CHANNEL
# CREATE TABLE IF NOT EXISTS dim_channel (
#     channel_key BIGINT,
#     channel_name STRING,
#     device_type STRING
# )
# USING DELTA;

# -- DIM_PRODUCT
# CREATE TABLE IF NOT EXISTS dim_product (
#     product_key BIGINT,
#     product_id STRING,
#     product_name STRING,
#     category_key BIGINT,
#     brand_key BIGINT,
#     season_key BIGINT,
#     price_range_key BIGINT,
#     supplier_name STRING,
#     gender STRING,
#     color STRING,
#     size STRING,
#     fabric_material STRING,
#     cosmetic_type STRING,
#     skin_type STRING,
#     expiry_date DATE,
#     shelf_life_days INT,
#     base_price DECIMAL(10,2),
#     cost_price DECIMAL(10,2),
#     created_date TIMESTAMP
# )
# USING DELTA;

# -- DIM_PROMOTION
# CREATE TABLE IF NOT EXISTS dim_promotion (
#     promotion_key BIGINT,
#     promotion_name STRING,
#     promotion_type STRING,
#     discount_type STRING,
#     start_date DATE,
#     end_date DATE
# )
# USING DELTA;

# -- =========================================================
# -- FACT TABLES:

# -- FACT_SALES
# CREATE TABLE IF NOT EXISTS fact_sales (
#     sales_key BIGINT,
#     date_key INT,
#     customer_key BIGINT,
#     product_key BIGINT,
#     store_key BIGINT,
#     promotion_key BIGINT,
#     markdown_key BIGINT,
#     stock_level_key BIGINT,
#     quantity_sold INT,
#     original_price DECIMAL(10,2),
#     discount_amount DECIMAL(10,2),
#     selling_price DECIMAL(10,2),
#     revenue DECIMAL(12,2),
#     cost_price DECIMAL(10,2),
#     profit DECIMAL(12,2),
#     payment_mode STRING,
#     created_timestamp TIMESTAMP
# )
# USING DELTA;

# -- FACT_INVENTORY
# CREATE TABLE IF NOT EXISTS fact_inventory (
#     inventory_fact_key BIGINT,
#     date_key INT,
#     product_key BIGINT,
#     store_key BIGINT,
#     stock_level_key BIGINT,
#     opening_stock INT,
#     closing_stock INT,
#     stock_received INT,
#     stock_sold INT,
#     damaged_stock INT
# )
# USING DELTA;

# -- FACT_MARKDOWN_ANALYTICS
# CREATE TABLE IF NOT EXISTS fact_markdown_analytics (
#     markdown_fact_key BIGINT,
#     date_key INT,
#     product_key BIGINT,
#     markdown_key BIGINT,
#     promotion_key BIGINT,
#     before_markdown_sales DECIMAL(12,2),
#     after_markdown_sales DECIMAL(12,2),
#     uplift_percentage DECIMAL(5,2),
#     inventory_clearance_rate DECIMAL(5,2)
# )
# USING DELTA;

# -- FACT_CUSTOMER_FEEDBACK
# CREATE TABLE IF NOT EXISTS fact_customer_feedback (
#     feedback_fact_key BIGINT,
#     customer_key BIGINT,
#     product_key BIGINT,
#     customer_rating_key BIGINT,
#     review_text STRING,
#     sentiment_score DECIMAL(5,2),
#     review_date DATE
# )
# USING DELTA;

# -- =========================================================
# -- INSERT STATIC DIMENSION DATA

# -- INSERT INTO DIM_SEASON
# INSERT INTO dim_season VALUES
# (1, 'Summer', 3, 6),
# (2, 'Monsoon', 7, 9),
# (3, 'Winter', 10, 2),
# (4, 'Festive', 10, 12);

# -- INSERT INTO DIM_MARKDOWN
# INSERT INTO dim_markdown VALUES
# (1, 10.00, 'Percentage', 'Seasonal Sale', 'Seasonal'),
# (2, 20.00, 'Percentage', 'Festival Offer', 'Festival'),
# (3, 40.00, 'Percentage', 'Inventory Clearance', 'Clearance');

# -- INSERT INTO DIM_PRICE_RANGE
# INSERT INTO dim_price_range VALUES
# (1, 0.00, 500.00, 'Budget'),
# (2, 501.00, 2000.00, 'Mid Range'),
# (3, 2001.00, 10000.00, 'Premium');

# -- INSERT INTO DIM_STOCK_LEVEL
# INSERT INTO dim_stock_level VALUES
# (1, 'Low Stock', '0-50', 'Yes'),
# (2, 'Medium Stock', '51-200', 'No'),
# (3, 'Over Stock', '201+', 'No');

# -- INSERT INTO DIM_CUSTOMER_RATING
# INSERT INTO dim_customer_rating VALUES
# (1, 1.0, 'Poor'),
# (2, 2.0, 'Average'),
# (3, 3.0, 'Good'),
# (4, 4.0, 'Very Good'),
# (5, 5.0, 'Excellent');

# -- INSERT INTO DIM_CHANNEL
# INSERT INTO dim_channel VALUES
# (1, 'Online', 'Mobile'),
# (2, 'Online', 'Desktop'),
# (3, 'Offline Store', 'NA');

# -- INSERT INTO DIM_STORE
# INSERT INTO dim_store VALUES
# (1, 'S001', 'Mumbai Fashion Store', 'Mumbai', 'Maharashtra', 'India', 'West', 'Offline'),
# (2, 'S002', 'Delhi Beauty Store', 'Delhi', 'Delhi', 'India', 'North', 'Offline'),
# (3, 'S003', 'Online Retail Hub', 'Bangalore', 'Karnataka', 'India', 'South', 'Online');

# -- INSERT INTO DIM_DATE
# INSERT INTO dim_date VALUES
# (20250101, '2025-01-01', 1, 1, 'January', 'Q1', 2025, 1, 'No', 'Yes'),
# (20250102, '2025-01-02', 2, 1, 'January', 'Q1', 2025, 1, 'No', 'No'),
# (20250103, '2025-01-03', 3, 1, 'January', 'Q1', 2025, 1, 'No', 'No');

# -- 6. INSERT DATA FROM DATASETS

# -- IMPORTANT:
# -- Before running below inserts,
# -- create these temp/raw tables:
# --
# -- cosmetics_raw
# -- fashion_raw
# --
# -- using your Spark DataFrames.

# -- =========================================================
# -- INSERT INTO DIM_CATEGORY
# -- =========================================================

# INSERT INTO dim_category
# SELECT DISTINCT
#     monotonically_increasing_id() AS category_key,
#     category AS category_name,
#     NULL AS parent_category,
#     NULL AS sub_category
# FROM (
#     SELECT category FROM cosmetics_raw
#     UNION
#     SELECT category FROM fashion_raw
# );

# -- =========================================================
# -- INSERT INTO DIM_BRAND
# -- =========================================================

# INSERT INTO dim_brand
# SELECT DISTINCT
#     monotonically_increasing_id() AS brand_key,
#     brand AS brand_name,
#     NULL AS brand_type,
#     NULL AS country
# FROM (
#     SELECT brand FROM cosmetics_raw
#     UNION
#     SELECT brand FROM fashion_raw
# );

# -- =========================================================
# -- INSERT INTO DIM_PRODUCT
# -- =========================================================

# INSERT INTO dim_product
# SELECT
#     monotonically_increasing_id() AS product_key,
#     p.product_id,
#     p.product_name,
#     c.category_key,
#     b.brand_key,
#     1 AS season_key,
#     CASE
#         WHEN p.price <= 500 THEN 1
#         WHEN p.price <= 2000 THEN 2
#         ELSE 3
#     END AS price_range_key,
#     NULL AS supplier_name,
#     NULL AS gender,
#     NULL AS color,
#     NULL AS size,
#     NULL AS fabric_material,
#     NULL AS cosmetic_type,
#     NULL AS skin_type,
#     NULL AS expiry_date,
#     NULL AS shelf_life_days,
#     p.price AS base_price,
#     p.price * 0.60 AS cost_price,
#     current_timestamp() AS created_date
# FROM (

#     SELECT
#         product_id,
#         product_name,
#         category,
#         brand,
#         price
#     FROM cosmetics_raw

#     UNION

#     SELECT
#         product_id,
#         product_name,
#         category,
#         brand,
#         price
#     FROM fashion_raw

# ) p
# LEFT JOIN dim_category c
# ON p.category = c.category_name

# LEFT JOIN dim_brand b
# ON p.brand = b.brand_name;

# -- =========================================================
# -- INSERT INTO FACT_SALES
# -- =========================================================

# INSERT INTO fact_sales
# SELECT
#     monotonically_increasing_id() AS sales_key,
#     20250101 AS date_key,
#     NULL AS customer_key,
#     dp.product_key,
#     1 AS store_key,
#     NULL AS promotion_key,
#     1 AS markdown_key,
#     2 AS stock_level_key,
#     1 AS quantity_sold,
#     dp.base_price AS original_price,
#     dp.base_price * 0.10 AS discount_amount,
#     dp.base_price * 0.90 AS selling_price,
#     dp.base_price * 0.90 AS revenue,
#     dp.cost_price AS cost_price,
#     (dp.base_price * 0.90) - dp.cost_price AS profit,
#     'Online' AS payment_mode,
#     current_timestamp() AS created_timestamp
# FROM dim_product dp;

# -- =========================================================
# -- INSERT INTO FACT_MARKDOWN_ANALYTICS
# -- =========================================================

# INSERT INTO fact_markdown_analytics
# SELECT
#     monotonically_increasing_id() AS markdown_fact_key,
#     20250101 AS date_key,
#     product_key,
#     1 AS markdown_key,
#     NULL AS promotion_key,
#     base_price AS before_markdown_sales,
#     base_price * 0.90 AS after_markdown_sales,
#     10.00 AS uplift_percentage,
#     70.00 AS inventory_clearance_rate
# FROM dim_product;

# -- =========================================================
# -- VALIDATION QUERIES
# -- =========================================================

# SELECT * FROM dim_product;
# SELECT * FROM dim_category;
# SELECT * FROM dim_brand;
# SELECT * FROM fact_sales;
# SELECT * FROM fact_markdown_analytics;
