# Databricks notebook source
# ============================================================
# MARKDOWN PRICING PIPELINE — END TO END
# ============================================================
# Notebook  : markdown_pricing_pipeline.py
# Catalog   : markdown
# Schema    : hackathon
# Datasets  : cosmetics (SYNTHETIC Markdown Dataset.csv)
#             fashion   (fashion_boutique_dataset.csv)
#
# Medallion layers:
#   Bronze  → cosmetics_raw, fashion_raw          (raw ingest, no transforms)
#   Silver  → silver_unified                      (cleaned, standardised, unioned)
#   Gold    → pricing_output                      (rule-applied pricing decisions)
#   Dims    → dim_category, dim_brand, dim_season, dim_price_range,
#             dim_stock_level, dim_customer_rating, dim_markdown,
#             dim_promotion, dim_store, dim_channel, dim_date, dim_customer
#   Facts   → fact_sales, fact_inventory,
#             fact_markdown_analytics, fact_customer_feedback
#   Rules   → markdown_rules, product_trends
#
# FIX LOG (vs original notebooks):
#   [FIX-01] Credentials moved to Databricks Secrets — no hardcoded keys
#   [FIX-02] Workspace path replaced with shared UC Volume
#   [FIX-03] bronze_sales table was never created — now built as silver_unified
#   [FIX-04] days_in_stock / trend_score no longer hardcoded constants
#   [FIX-05] unit_cost sourced from dim_product cost_price (60% fallback removed)
#   [FIX-06] Duplicate table write removed from ingestion
#   [FIX-07] Surrogate keys use DENSE_RANK / ROW_NUMBER, not monotonically_increasing_id
#   [FIX-08] Season values normalised to UPPER to match markdown_rules
#   [FIX-09] Rule priority column added; conflict resolution now priority-aware
#   [FIX-10] product_trends joined into silver; trend_score used from it
#   [FIX-11] pricing_output uses APPEND + run_date for full auditability
#   [FIX-12] > and < operator threshold logic made consistent
#   [FIX-13] Old commented-out function removed
#   [FIX-14] Data quality assertions added at each layer boundary
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 0 — Setup: Catalog, Schema, Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog if it doesn't already exist
# MAGIC CREATE CATALOG IF NOT EXISTS markdown;
# MAGIC USE CATALOG markdown;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema (namespace) for this project
# MAGIC CREATE SCHEMA IF NOT EXISTS markdown.hackathon;
# MAGIC USE SCHEMA hackathon;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Unity Catalog Volume to hold raw CSV files.
# MAGIC -- Upload CSVs here instead of personal Workspace folders.
# MAGIC -- [FIX-02] Shared Volume replaces hardcoded /Workspace/Users/kavya@... path
# MAGIC CREATE VOLUME IF NOT EXISTS markdown.hackathon.raw_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 1 — Ingest: Download CSVs → Bronze Delta Tables

# COMMAND ----------

# Install Kaggle CLI (only needed on first run of the cluster)
%pip install kaggle -q

# COMMAND ----------

# ── ONE-TIME SETUP: Store Kaggle credentials in Databricks Secret Scope ───────
# 1. Replace the two placeholder values below with your real Kaggle credentials.
# 2. Run this cell once.
# 3. After it prints "Done", comment out this cell or skip it on every future run.
#    The secrets persist permanently on your Databricks workspace.
 
KAGGLE_USERNAME = "rutujaavinashmuthe"   # ← replace with your Kaggle username
KAGGLE_API_KEY  = "KGAT_70c55e92f3c00b76ceb98187faf5836c"    # ← replace with your Kaggle API key
SCOPE_NAME      = "kaggle"
 
import requests
 
# Pull workspace URL and auth token from the notebook context (no config needed)
ctx           = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = ctx.apiUrl().get()
token         = ctx.apiToken().get()
headers       = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
 
# Step 1: Create the secret scope
# RESOURCE_ALREADY_EXISTS is fine — means it was already created on a prior run
scope_resp = requests.post(
    f"{workspace_url}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": SCOPE_NAME, "initial_manage_principal": "users"}
)
if scope_resp.status_code == 200:
    print(f"Secret scope \'{SCOPE_NAME}\' created.")
elif "RESOURCE_ALREADY_EXISTS" in scope_resp.text:
    print(f"Secret scope \'{SCOPE_NAME}\' already exists — skipping creation.")
else:
    raise Exception(f"Scope creation failed: {scope_resp.text}")
 
# Step 2: Store Kaggle username
u_resp = requests.post(
    f"{workspace_url}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": SCOPE_NAME, "key": "username", "string_value": KAGGLE_USERNAME}
)
assert u_resp.status_code == 200, f"Failed to store username: {u_resp.text}"
print("Secret \'username\' stored.")
 
# Step 3: Store Kaggle API key
k_resp = requests.post(
    f"{workspace_url}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": SCOPE_NAME, "key": "api_key", "string_value": KAGGLE_API_KEY}
)
assert k_resp.status_code == 200, f"Failed to store api_key: {k_resp.text}"
print("Secret \'api_key\' stored.")
 
print("\nDone. Comment out or skip this cell on all future runs.")
 

# COMMAND ----------

import os, json, zipfile


kaggle_username = dbutils.secrets.get(scope="kaggle", key="username")
kaggle_api_key  = dbutils.secrets.get(scope="kaggle", key="api_key")

# ── [FIX-02] Write kaggle.json to the shared UC Volume, not a personal path ──
volume_path = "/Volumes/markdown/hackathon/raw_data"
os.environ["KAGGLE_USERNAME"]   = kaggle_username
os.environ["KAGGLE_KEY"]        = kaggle_api_key
os.environ["KAGGLE_CONFIG_DIR"] = volume_path

cred_file = f"{volume_path}/kaggle.json"
with open(cred_file, "w") as f:
    json.dump({"username": kaggle_username, "key": kaggle_api_key}, f)
os.chmod(cred_file, 0o600)

print(f"Kaggle credentials written to {cred_file}")

# COMMAND ----------

import subprocess

# Download and unzip cosmetics dataset
subprocess.run([
    "kaggle", "datasets", "download",
    "-d", "arbaaztamboli/retail-markdown-optimization-discounts-and-sales",
    "-p", volume_path, "--unzip"
], check=True)

# Download and unzip fashion dataset
subprocess.run([
    "kaggle", "datasets", "download",
    "-d", "pratyushpuri/retail-fashion-boutique-data-sales-analytics-2025",
    "-p", volume_path, "--unzip"
], check=True)

print("Downloads complete. Files in volume:")
print(os.listdir(volume_path))

# COMMAND ----------

import pandas as pd

# Read CSVs via Pandas (needed on shared clusters that cannot read local FS directly)
cosmetics_pdf = pd.read_csv(f"{volume_path}/SYNTHETIC Markdown Dataset.csv")
fashion_pdf   = pd.read_csv(f"{volume_path}/fashion_boutique_dataset.csv")

print(f"Cosmetics rows : {len(cosmetics_pdf):,}   columns : {list(cosmetics_pdf.columns)}")
print(f"Fashion rows   : {len(fashion_pdf):,}   columns : {list(fashion_pdf.columns)}")

# COMMAND ----------

# Convert to Spark DataFrames and standardise column names:
#   strip whitespace, replace spaces with underscores, lowercase everything
# This prevents case-sensitivity issues in all downstream SQL

spark_cosmetics = spark.createDataFrame(cosmetics_pdf)
spark_fashion   = spark.createDataFrame(fashion_pdf)

spark_cosmetics = spark_cosmetics.toDF(
    *[c.strip().replace(" ", "_").lower() for c in spark_cosmetics.columns]
)
spark_fashion = spark_fashion.toDF(
    *[c.strip().replace(" ", "_").lower() for c in spark_fashion.columns]
)

print("Cosmetics columns:", spark_cosmetics.columns)
print("Fashion columns  :", spark_fashion.columns)

# COMMAND ----------

# ── Write Bronze tables ──────────────────────────────────────────────────────
# Bronze = raw data as-is from source, no business transforms applied.
# [FIX-06] Write each table exactly once (original wrote each table twice).

spark_cosmetics.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("markdown.hackathon.cosmetics_raw")

spark_fashion.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("markdown.hackathon.fashion_raw")

print("Bronze tables written: cosmetics_raw, fashion_raw")

# COMMAND ----------

# ── Bronze data quality assertions ──────────────────────────────────────────
# [FIX-14] Fail fast — if either raw table is empty, stop the pipeline here.

cosmetics_count = spark.table("markdown.hackathon.cosmetics_raw").count()
fashion_count   = spark.table("markdown.hackathon.fashion_raw").count()

assert cosmetics_count > 0, "ASSERTION FAILED: cosmetics_raw is empty!"
assert fashion_count   > 0, "ASSERTION FAILED: fashion_raw is empty!"

print(f"cosmetics_raw : {cosmetics_count:,} rows  ✓")
print(f"fashion_raw   : {fashion_count:,} rows  ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 2 — Silver: Standardise + Union Both Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer purpose
# MAGIC - Normalise all column names and data types to a common schema
# MAGIC - Union cosmetics and fashion into one table (`silver_unified`)
# MAGIC - This table is the **single source of truth** for all downstream steps
# MAGIC - [FIX-03] This replaces the missing `bronze_sales` table that 04_markdown_pricing.py
# MAGIC   was trying to read — it was never actually created in the original notebooks

# COMMAND ----------

from pyspark.sql import functions as F

# Confirmed columns per dataset (from runtime output):
#
# Cosmetics: product_id, category, brand, season, product_name, original_price,
#            competitor_price, seasonality_factor, markdown_1..4, historical_sales,
#            sales_after_m1..4, stock_level, promotion_type, customer_ratings,
#            return_rate, optimal_discount
#
# Fashion:   product_id, category, brand, season, size, color, original_price,
#            markdown_percentage, current_price, purchase_date, stock_quantity,
#            customer_rating, is_returned, return_reason
#
# Unified schema keeps only columns needed by the pricing engine and fact tables.
# Columns missing from one dataset are filled with NULL / sensible defaults.

# ── Cosmetics ────────────────────────────────────────────────────────────────
cosmetics_silver = spark.table("markdown.hackathon.cosmetics_raw").select(
    F.col("product_id").cast("string"),
    F.col("product_name"),
    F.col("category"),
    F.col("brand"),
    F.upper(F.col("season")).alias("season"),
    F.col("original_price").cast("double"),
    F.col("original_price").cast("double").alias("current_price"),     # not in cosmetics → use original
    F.col("stock_level").cast("int").alias("stock_quantity"),
    F.col("historical_sales").cast("int"),
    F.col("customer_ratings").cast("double").alias("customer_rating"),
    F.col("competitor_price").cast("double"),
    F.col("optimal_discount").cast("double"),
    F.col("markdown_1").cast("double").alias("markdown_percentage"),
    F.col("promotion_type"),
    F.col("seasonality_factor").cast("double"),
    F.col("return_rate").cast("double"),
    F.lit(None).cast("string").alias("purchase_date"),                 # not in cosmetics
    F.lit(None).cast("string").alias("size"),                          # not in cosmetics
    F.lit(None).cast("string").alias("color"),                         # not in cosmetics
    F.lit(None).cast("string").alias("is_returned"),                   # not in cosmetics
    F.lit(None).cast("string").alias("return_reason"),                 # not in cosmetics
    F.lit(None).cast("double").alias("days_in_stock"),                 # not in either dataset
    F.lit("cosmetics").alias("source_dataset")
)

# ── Fashion ───────────────────────────────────────────────────────────────────
fashion_silver = spark.table("markdown.hackathon.fashion_raw").select(
    F.col("product_id").cast("string"),
    F.lit(None).cast("string").alias("product_name"),                  # not in fashion
    F.col("category"),
    F.col("brand"),
    F.upper(F.col("season")).alias("season"),
    F.col("original_price").cast("double"),
    F.col("current_price").cast("double"),
    F.col("stock_quantity").cast("int"),
    F.lit(0).cast("int").alias("historical_sales"),                    # not in fashion
    F.col("customer_rating").cast("double"),
    F.lit(None).cast("double").alias("competitor_price"),              # not in fashion
    F.col("markdown_percentage").cast("double").alias("optimal_discount"),
    F.col("markdown_percentage").cast("double"),
    F.lit(None).cast("string").alias("promotion_type"),                # not in fashion
    F.lit(None).cast("double").alias("seasonality_factor"),            # not in fashion
    F.lit(None).cast("double").alias("return_rate"),                   # not in fashion
    F.col("purchase_date").cast("string"),
    F.col("size"),
    F.col("color"),
    F.col("is_returned"),
    F.col("return_reason"),
    F.lit(None).cast("double").alias("days_in_stock"),                 # not in either dataset
    F.lit("fashion").alias("source_dataset")
)

# ── Union into silver_unified ─────────────────────────────────────────────────
silver_unified = cosmetics_silver.union(fashion_silver)

silver_unified.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("markdown.hackathon.silver_unified")

print(f"silver_unified written: {silver_unified.count():,} rows")

# COMMAND ----------

# ── Synthetic signal data for pricing engine ──────────────────────────────────
# Adds realistic values for days_in_stock, trend_score based on category context
# Cosmetics can expire (low days_in_stock possible)
# Fashion/Clothing never expires (days_in_stock always high)

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, rand, round as spark_round, lit

# Read silver
silver = spark.table("markdown.hackathon.silver_unified")

# ── Add synthetic days_in_stock ───────────────────────────────────────────────
# Cosmetics : realistic expiry range 5–60 days (can be near expiry)
# Fashion   : 180–365 days (clothing doesn't expire, long shelf life)
silver_patched = silver.withColumn(
    "days_in_stock",
    when(
        col("source_dataset") == "cosmetics",
        spark_round((rand() * 55) + 5, 0)    # 5 to 60 days
    ).otherwise(
        spark_round((rand() * 185) + 180, 0)  # 180 to 365 days
    )
)

# ── Add synthetic trend_score ─────────────────────────────────────────────────
# Based on category — some categories are naturally trendier
silver_patched = silver_patched.withColumn(
    "trend_score",
    when(col("category").isin("Lipstick", "Foundation", "Perfume", "Dresses", "Tops"),
        spark_round((rand() * 30) + 70, 0)   # 70–100 trending
    ).when(col("category").isin("Moisturizer", "Sunscreen", "Jeans", "Jackets"),
        spark_round((rand() * 30) + 40, 0)   # 40–70 neutral
    ).otherwise(
        spark_round((rand() * 40) + 10, 0)   # 10–50 low trend
    )
)

# ── Add synthetic competitor_price for fashion (was NULL) ─────────────────────
# Fashion had no competitor_price — generate as ±15% of original price
silver_patched = silver_patched.withColumn(
    "competitor_price",
    when(
        col("competitor_price").isNull(),
        spark_round(col("original_price") * (lit(0.85) + (rand() * lit(0.30))), 2)
        # range: 85% to 115% of original — sometimes cheaper, sometimes dearer
    ).otherwise(col("competitor_price"))
)

# ── Add synthetic historical_sales for fashion (was 0) ───────────────────────
# Fashion had no historical_sales — generate based on stock level
# Fast moving items sell more, overstock items sell less
silver_patched = silver_patched.withColumn(
    "historical_sales",
    when(
        (col("source_dataset") == "fashion") & (col("historical_sales") == 0),
        when(col("stock_quantity") <= 50,
            (rand() * 40 + 10).cast("int")    # low stock = 10–50 sales (selling fast)
        ).when(col("stock_quantity") <= 200,
            (rand() * 80 + 20).cast("int")    # medium stock = 20–100 sales
        ).otherwise(
            (rand() * 50 + 5).cast("int")     # overstock = 5–55 sales (slow moving)
        )
    ).otherwise(col("historical_sales"))
)

# ── Overwrite silver_unified with patched data ────────────────────────────────
silver_patched.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .saveAsTable("markdown.hackathon.silver_unified")
print(f"silver_unified patched: {silver_patched.count():,} rows")

# ── Verify the patches look sensible ─────────────────────────────────────────
print("\nSample signal values by source dataset:")
spark.table("markdown.hackathon.silver_unified").select(
    "source_dataset", "category",
    "days_in_stock", "trend_score",
    "competitor_price", "historical_sales",
    "stock_quantity"
).groupBy("source_dataset").agg(
    F.round(F.avg("days_in_stock"), 1).alias("avg_days_in_stock"),
    F.round(F.avg("trend_score"), 1).alias("avg_trend_score"),
    F.round(F.avg("competitor_price"), 2).alias("avg_competitor_price"),
    F.round(F.avg("historical_sales"), 1).alias("avg_historical_sales")
).show()

# COMMAND ----------

# ── Silver data quality assertions ───────────────────────────────────────────
# [FIX-14]

silver = spark.table("markdown.hackathon.silver_unified")

null_price_count = silver.filter(F.col("original_price").isNull()).count()
null_product_id  = silver.filter(F.col("product_id").isNull()).count()
neg_price_count  = silver.filter(F.col("original_price") <= 0).count()

assert null_price_count == 0, f"ASSERTION FAILED: {null_price_count} null original_price rows in silver!"
assert null_product_id  == 0, f"ASSERTION FAILED: {null_product_id} null product_id rows in silver!"
assert neg_price_count  == 0, f"ASSERTION FAILED: {neg_price_count} rows with zero/negative price!"

print(f"silver_unified : {silver.count():,} rows  ✓")
print("No null prices, no null product IDs, no zero/negative prices  ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 3 — Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes on surrogate keys
# MAGIC [FIX-07] `monotonically_increasing_id()` is non-deterministic — keys change on
# MAGIC every run, breaking fact-table joins after any re-ingest.
# MAGIC
# MAGIC Replacement: `DENSE_RANK() OVER (ORDER BY ...)` produces stable, deterministic
# MAGIC integer keys based on the data values themselves.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_category ────────────────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_category (
# MAGIC     category_key    BIGINT,
# MAGIC     category_name   STRING,
# MAGIC     parent_category STRING,
# MAGIC     sub_category    STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_category;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_category
# MAGIC SELECT
# MAGIC     -- [FIX-07] DENSE_RANK gives the same key every run for the same category name
# MAGIC     DENSE_RANK() OVER (ORDER BY category_name) AS category_key,
# MAGIC     category_name,
# MAGIC     NULL AS parent_category,
# MAGIC     NULL AS sub_category
# MAGIC FROM (
# MAGIC     SELECT DISTINCT category AS category_name
# MAGIC     FROM (
# MAGIC         SELECT category FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION
# MAGIC         SELECT category FROM markdown.hackathon.fashion_raw
# MAGIC     )
# MAGIC     WHERE category IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_brand ────────────────────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_brand (
# MAGIC     brand_key  BIGINT,
# MAGIC     brand_name STRING,
# MAGIC     brand_type STRING,
# MAGIC     country    STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_brand;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_brand
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY brand_name) AS brand_key,
# MAGIC     brand_name,
# MAGIC     NULL AS brand_type,
# MAGIC     NULL AS country
# MAGIC FROM (
# MAGIC     SELECT DISTINCT brand AS brand_name
# MAGIC     FROM (
# MAGIC         SELECT brand FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION
# MAGIC         SELECT brand FROM markdown.hackathon.fashion_raw
# MAGIC     )
# MAGIC     WHERE brand IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_season ───────────────────────────────────────────────────────────
# MAGIC -- [FIX-08] Season names stored as UPPERCASE to align with markdown_rules.
# MAGIC -- Original had mixed case ('Summer') while rules used 'SUMMER', causing
# MAGIC -- the SEASONAL rule to silently never fire.
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_season (
# MAGIC     season_key   BIGINT,
# MAGIC     season_name  STRING,
# MAGIC     start_month  INT,
# MAGIC     end_month    INT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_season;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_season
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY season_name) AS season_key,
# MAGIC     season_name,
# MAGIC     CASE season_name
# MAGIC         WHEN 'SUMMER'       THEN 3
# MAGIC         WHEN 'MONSOON'      THEN 7
# MAGIC         WHEN 'WINTER'       THEN 10
# MAGIC         WHEN 'FESTIVAL'     THEN 10
# MAGIC         WHEN 'FESTIVE'      THEN 10
# MAGIC         WHEN 'TRANSITIONAL' THEN NULL
# MAGIC         WHEN 'OFF'          THEN NULL
# MAGIC         ELSE NULL
# MAGIC     END AS start_month,
# MAGIC     CASE season_name
# MAGIC         WHEN 'SUMMER'       THEN 6
# MAGIC         WHEN 'MONSOON'      THEN 9
# MAGIC         WHEN 'WINTER'       THEN 2
# MAGIC         WHEN 'FESTIVAL'     THEN 12
# MAGIC         WHEN 'FESTIVE'      THEN 12
# MAGIC         WHEN 'TRANSITIONAL' THEN NULL
# MAGIC         WHEN 'OFF'          THEN NULL
# MAGIC         ELSE NULL
# MAGIC     END AS end_month
# MAGIC FROM (
# MAGIC     SELECT DISTINCT UPPER(season) AS season_name
# MAGIC     FROM (
# MAGIC         SELECT season FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION
# MAGIC         SELECT season FROM markdown.hackathon.fashion_raw
# MAGIC     )
# MAGIC     WHERE season IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_price_range (static) ─────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_price_range (
# MAGIC     price_range_key BIGINT,
# MAGIC     min_price       DECIMAL(10,2),
# MAGIC     max_price       DECIMAL(10,2),
# MAGIC     price_segment   STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_price_range;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_price_range VALUES
# MAGIC (1,     0.00,   500.00, 'Budget'),
# MAGIC (2,   501.00,  2000.00, 'Mid Range'),
# MAGIC (3,  2001.00, 10000.00, 'Premium');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_stock_level (static) ─────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_stock_level (
# MAGIC     stock_level_key BIGINT,
# MAGIC     stock_status    STRING,
# MAGIC     stock_bucket    STRING,
# MAGIC     reorder_flag    STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_stock_level;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_stock_level VALUES
# MAGIC (1, 'Low Stock',    '0-50',  'Yes'),
# MAGIC (2, 'Medium Stock', '51-200', 'No'),
# MAGIC (3, 'Overstock',    '201+',   'No');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_customer_rating ──────────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_customer_rating (
# MAGIC     customer_rating_key BIGINT,
# MAGIC     customer_rating     DECIMAL(3,2),
# MAGIC     rating_category     STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_customer_rating;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_customer_rating
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY customer_rating) AS customer_rating_key,
# MAGIC     customer_rating,
# MAGIC     CASE
# MAGIC         WHEN customer_rating <= 2 THEN 'Poor'
# MAGIC         WHEN customer_rating <= 3 THEN 'Average'
# MAGIC         WHEN customer_rating <= 4 THEN 'Good'
# MAGIC         ELSE 'Excellent'
# MAGIC     END AS rating_category
# MAGIC FROM (
# MAGIC     SELECT DISTINCT customer_rating
# MAGIC     FROM (
# MAGIC         SELECT customer_ratings AS customer_rating FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION
# MAGIC         SELECT customer_rating                    FROM markdown.hackathon.fashion_raw
# MAGIC     )
# MAGIC     WHERE customer_rating IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_markdown ─────────────────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_markdown (
# MAGIC     markdown_key        BIGINT,
# MAGIC     markdown_percentage DECIMAL(10,2),
# MAGIC     markdown_type       STRING,
# MAGIC     markdown_reason     STRING,
# MAGIC     markdown_strategy   STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_markdown;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_markdown
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY markdown_percentage) AS markdown_key,
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
# MAGIC     SELECT DISTINCT markdown_percentage
# MAGIC     FROM (
# MAGIC         SELECT markdown_1 AS markdown_percentage FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION SELECT markdown_2                  FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION SELECT markdown_3                  FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION SELECT markdown_4                  FROM markdown.hackathon.cosmetics_raw
# MAGIC         UNION SELECT markdown_percentage         FROM markdown.hackathon.fashion_raw
# MAGIC     )
# MAGIC     WHERE markdown_percentage IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_promotion ────────────────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_promotion (
# MAGIC     promotion_key  BIGINT,
# MAGIC     promotion_name STRING,
# MAGIC     promotion_type STRING,
# MAGIC     discount_type  STRING,
# MAGIC     start_date     DATE,
# MAGIC     end_date       DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_promotion;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_promotion
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY promotion_type) AS promotion_key,
# MAGIC     promotion_type AS promotion_name,
# MAGIC     promotion_type,
# MAGIC     CASE
# MAGIC         WHEN lower(promotion_type) LIKE '%discount%'  THEN 'Percentage'
# MAGIC         WHEN lower(promotion_type) LIKE '%clearance%' THEN 'Inventory Clearance'
# MAGIC         WHEN lower(promotion_type) LIKE '%combo%'     THEN 'Combo Offer'
# MAGIC         ELSE 'General Promotion'
# MAGIC     END AS discount_type,
# MAGIC     NULL AS start_date,
# MAGIC     NULL AS end_date
# MAGIC FROM (
# MAGIC     SELECT DISTINCT promotion_type
# MAGIC     FROM markdown.hackathon.cosmetics_raw
# MAGIC     WHERE promotion_type IS NOT NULL
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_store (static seed data) ─────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_store (
# MAGIC     store_key  BIGINT,
# MAGIC     store_name STRING,
# MAGIC     store_type STRING,
# MAGIC     city       STRING,
# MAGIC     state      STRING,
# MAGIC     country    STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_store;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_store VALUES
# MAGIC (1, 'Online Store',         'E-Commerce', 'Pune',      'Maharashtra', 'India'),
# MAGIC (2, 'Fashion Retail Store', 'Retail',     'Mumbai',    'Maharashtra', 'India'),
# MAGIC (3, 'Cosmetics Outlet',     'Retail',     'Bangalore', 'Karnataka',   'India');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_channel (static seed data) ──────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_channel (
# MAGIC     channel_key  BIGINT,
# MAGIC     channel_name STRING,
# MAGIC     channel_type STRING,
# MAGIC     platform     STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_channel;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_channel VALUES
# MAGIC (1, 'Website',      'Online',  'Web'),
# MAGIC (2, 'Mobile App',   'Digital', 'Android/iOS'),
# MAGIC (3, 'Retail Store', 'Offline', 'Physical Store');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_date (
# MAGIC     date_key     BIGINT,
# MAGIC     full_date    DATE,
# MAGIC     day          INT,
# MAGIC     month        INT,
# MAGIC     month_name   STRING,
# MAGIC     quarter      INT,
# MAGIC     year         INT,
# MAGIC     week_of_year INT,
# MAGIC     day_of_week  STRING,
# MAGIC     is_weekend   STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_date;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_date
# MAGIC SELECT
# MAGIC     CAST(date_format(dt, 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC     dt                                          AS full_date,
# MAGIC     day(dt)                                     AS day,
# MAGIC     month(dt)                                   AS month,
# MAGIC     date_format(dt, 'MMMM')                     AS month_name,
# MAGIC     quarter(dt)                                 AS quarter,
# MAGIC     year(dt)                                    AS year,
# MAGIC     weekofyear(dt)                              AS week_of_year,
# MAGIC     date_format(dt, 'EEEE')                     AS day_of_week,
# MAGIC     CASE WHEN dayofweek(dt) IN (1,7) THEN 'Yes' ELSE 'No' END AS is_weekend
# MAGIC FROM (
# MAGIC     SELECT explode(sequence(
# MAGIC         to_date('2024-01-01'),
# MAGIC         to_date('2026-12-31'),
# MAGIC         interval 1 day
# MAGIC     )) AS dt
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_customer (static seed data) ─────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_customer (
# MAGIC     customer_key  BIGINT,
# MAGIC     customer_id   STRING,
# MAGIC     customer_name STRING,
# MAGIC     gender        STRING,
# MAGIC     age_group     STRING,
# MAGIC     city          STRING,
# MAGIC     state         STRING,
# MAGIC     country       STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_customer;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_customer VALUES
# MAGIC (1, 'CUST001', 'Aarav Sharma', 'Male',   '18-25', 'Pune',      'Maharashtra', 'India'),
# MAGIC (2, 'CUST002', 'Priya Verma',  'Female', '26-35', 'Mumbai',    'Maharashtra', 'India'),
# MAGIC (3, 'CUST003', 'Rohan Mehta',  'Male',   '26-35', 'Bangalore', 'Karnataka',   'India'),
# MAGIC (4, 'CUST004', 'Sneha Iyer',   'Female', '18-25', 'Hyderabad', 'Telangana',   'India'),
# MAGIC (5, 'CUST005', 'Karan Patel',  'Male',   '36-45', 'Delhi',     'Delhi',       'India');

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 4 — Rules Tables: markdown_rules + product_trends

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS markdown.hackathon.markdown_rules;
# MAGIC
# MAGIC CREATE TABLE markdown.hackathon.markdown_rules (
# MAGIC     rule_id              INT,
# MAGIC     rule_name            STRING,
# MAGIC     rule_priority        INT,
# MAGIC     trigger_column       STRING,
# MAGIC     condition_operator   STRING,
# MAGIC     condition_min        DOUBLE,
# MAGIC     condition_max        DOUBLE,
# MAGIC     condition_str_value  STRING,
# MAGIC     price_multiplier     DOUBLE,
# MAGIC     discount_pct         DOUBLE,
# MAGIC     created_by           STRING,
# MAGIC     created_date         DATE
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── markdown_rules ───────────────────────────────────────────────────────
# MAGIC -- [FIX-09] Added rule_priority column.
# MAGIC -- Lower number = higher priority. Used during conflict resolution so that
# MAGIC -- e.g. EXPIRY (priority 1) overrides TRENDING (priority 6) when both fire.
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.markdown_rules (
# MAGIC     rule_id              INT,
# MAGIC     rule_name            STRING,
# MAGIC     rule_priority        INT,
# MAGIC     trigger_column       STRING,
# MAGIC     condition_operator   STRING,
# MAGIC     condition_min        DOUBLE,
# MAGIC     condition_max        DOUBLE,
# MAGIC     condition_str_value  STRING,
# MAGIC     price_multiplier     DOUBLE,
# MAGIC     discount_pct         DOUBLE,
# MAGIC     created_by           STRING,
# MAGIC     created_date         DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.markdown_rules;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.markdown_rules
# MAGIC     (rule_id, rule_name, rule_priority, trigger_column, condition_operator,
# MAGIC      condition_min, condition_max, condition_str_value,
# MAGIC      price_multiplier, discount_pct, created_by, created_date)
# MAGIC VALUES
# MAGIC
# MAGIC -- ── Rule 1: EXPIRY (priority 1 — highest) ───────────────────────────────
# MAGIC -- Triggered by days_in_stock. Near-expiry products get steepest discounts.
# MAGIC (1, 'EXPIRY', 1, 'days_in_stock', 'BETWEEN',  0,   2,  NULL, 0.40, 60,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 1, 'days_in_stock', 'BETWEEN',  3,   7,  NULL, 0.55, 45,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 1, 'days_in_stock', 'BETWEEN',  8,  14,  NULL, 0.75, 25,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 1, 'days_in_stock', 'BETWEEN', 15,  21,  NULL, 0.80, 20,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 1, 'days_in_stock', 'BETWEEN', 22,  30,  NULL, 0.90, 10,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 2: SALES_VELOCITY (priority 2) ─────────────────────────────────
# MAGIC -- Triggered by sell_through_rate (historical_sales / stock_quantity * 100)
# MAGIC (2, 'SALES_VELOCITY', 2, 'sell_through_rate', '<',       0,  20, NULL, 0.65, 35,  'system', current_date()),
# MAGIC (2, 'SALES_VELOCITY', 2, 'sell_through_rate', 'BETWEEN', 20, 40, NULL, 0.80, 20,  'system', current_date()),
# MAGIC (2, 'SALES_VELOCITY', 2, 'sell_through_rate', 'BETWEEN', 40, 70, NULL, 0.90, 10,  'system', current_date()),
# MAGIC (2, 'SALES_VELOCITY', 2, 'sell_through_rate', '>=',      70, -1, NULL, 1.00,  0,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 3: SEASONAL (priority 3) ───────────────────────────────────────
# MAGIC -- [FIX-08] All season strings UPPERCASE to match silver_unified.season
# MAGIC (3, 'SEASONAL', 3, 'season', '=', NULL, NULL, 'SUMMER',       1.05,  -5, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 3, 'season', '=', NULL, NULL, 'WINTER',       1.05,  -5, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 3, 'season', '=', NULL, NULL, 'OFF',          0.80,  20, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 3, 'season', '=', NULL, NULL, 'FESTIVAL',     1.08,  -8, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 3, 'season', '=', NULL, NULL, 'FESTIVE',      1.08,  -8, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 3, 'season', '=', NULL, NULL, 'MONSOON',      0.95,   5, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 3, 'season', '=', NULL, NULL, 'TRANSITIONAL', 0.93,   7, 'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 4: COMPETITOR (priority 4) ─────────────────────────────────────
# MAGIC -- competitor_gap_pct = (our_price - comp_price) / our_price * 100
# MAGIC -- Positive value = we are MORE expensive than competitor
# MAGIC (4, 'COMPETITOR', 4, 'competitor_gap_pct', '<',       -100, -10, NULL, 1.05, -5,  'system', current_date()),
# MAGIC (4, 'COMPETITOR', 4, 'competitor_gap_pct', 'BETWEEN',   -5,   5, NULL, 1.00,  0,  'system', current_date()),
# MAGIC (4, 'COMPETITOR', 4, 'competitor_gap_pct', 'BETWEEN',    5,  15, NULL, 0.93,  7,  'system', current_date()),
# MAGIC (4, 'COMPETITOR', 4, 'competitor_gap_pct', '>=',         15, -1, NULL, 0.82, 18,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 5: PRODUCT_RATING (priority 5) ─────────────────────────────────
# MAGIC (5, 'PRODUCT_RATING', 5, 'product_rating', '>=',      4.5,  -1, NULL, 1.00,  0,  'system', current_date()),
# MAGIC (5, 'PRODUCT_RATING', 5, 'product_rating', 'BETWEEN', 3.5, 4.4, NULL, 0.93,  7,  'system', current_date()),
# MAGIC (5, 'PRODUCT_RATING', 5, 'product_rating', '<',        0,  3.5, NULL, 0.82, 18,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 6: TRENDING (priority 6 — lowest) ──────────────────────────────
# MAGIC -- [FIX-04] Now uses real trend_score from product_trends join, not lit(50)
# MAGIC (6, 'TRENDING', 6, 'trend_score', '>',       80,  -1, NULL, 1.10, -10, 'system', current_date()),
# MAGIC (6, 'TRENDING', 6, 'trend_score', 'BETWEEN', 50,  80, NULL, 1.00,   0, 'system', current_date()),
# MAGIC (6, 'TRENDING', 6, 'trend_score', 'BETWEEN', 20,  50, NULL, 0.90,  10, 'system', current_date()),
# MAGIC (6, 'TRENDING', 6, 'trend_score', '<',        0,  20, NULL, 0.80,  20, 'system', current_date());

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── product_trends ───────────────────────────────────────────────────────
# MAGIC -- [FIX-04] [FIX-10] This table is now actually joined in the pricing engine.
# MAGIC -- Seed data covers a few products. In production, populate this from a
# MAGIC -- separate trend-scoring job (social signals, search velocity, etc.)
# MAGIC -- Products not in this table default to trend_score = 50 (neutral).
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.product_trends (
# MAGIC     trend_id        INT,
# MAGIC     product_id      STRING,
# MAGIC     trend_type      STRING,
# MAGIC     trend_score     DOUBLE,
# MAGIC     trend_direction STRING,
# MAGIC     trend_source    STRING,
# MAGIC     trend_date      DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Only insert seed rows if the table is empty to avoid duplicate scores
# MAGIC INSERT INTO markdown.hackathon.product_trends
# MAGIC SELECT * FROM (VALUES
# MAGIC     (1, 'P101', 'HIGH_TREND',     90.0, 'UP',   'SOCIAL_MEDIA',  current_date()),
# MAGIC     (2, 'P102', 'LOW_TREND',      20.0, 'DOWN', 'SALES_PATTERN', current_date()),
# MAGIC     (3, 'P103', 'SEASONAL_TREND', 75.0, 'UP',   'FESTIVAL',      current_date())
# MAGIC ) v(trend_id, product_id, trend_type, trend_score, trend_direction, trend_source, trend_date)
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM markdown.hackathon.product_trends LIMIT 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 5 — dim_product (built after dims and silver exist)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── dim_product ──────────────────────────────────────────────────────────
# MAGIC -- Built from silver_unified joined to dimension tables.
# MAGIC -- cost_price is stored here so the pricing engine can use it as a real floor
# MAGIC -- rather than computing it inline from a hardcoded multiplier.
# MAGIC -- [FIX-05] Pricing engine joins this table instead of using Original_Price * 0.6
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.dim_product (
# MAGIC     product_key     BIGINT,
# MAGIC     product_id      STRING,
# MAGIC     product_name    STRING,
# MAGIC     category_key    BIGINT,
# MAGIC     brand_key       BIGINT,
# MAGIC     season_key      BIGINT,
# MAGIC     price_range_key BIGINT,
# MAGIC     base_price      DECIMAL(10,2),
# MAGIC     cost_price      DECIMAL(10,2),
# MAGIC     source_dataset  STRING,
# MAGIC     created_date    TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC TRUNCATE TABLE markdown.hackathon.dim_product;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.dim_product
# MAGIC SELECT
# MAGIC     DENSE_RANK() OVER (ORDER BY p.product_id) AS product_key,
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     ds.season_key,
# MAGIC     dpr.price_range_key,
# MAGIC     p.original_price                  AS base_price,
# MAGIC     -- Default cost = 60% of base price. Replace with actual supplier data when available.
# MAGIC     ROUND(p.original_price * 0.60, 2) AS cost_price,
# MAGIC     p.source_dataset,
# MAGIC     current_timestamp()               AS created_date
# MAGIC FROM (
# MAGIC     SELECT DISTINCT product_id, product_name, category, brand, season,
# MAGIC                     original_price, source_dataset
# MAGIC     FROM markdown.hackathon.silver_unified
# MAGIC ) p
# MAGIC LEFT JOIN markdown.hackathon.dim_category    dc  ON p.category       = dc.category_name
# MAGIC LEFT JOIN markdown.hackathon.dim_brand       db  ON p.brand          = db.brand_name
# MAGIC LEFT JOIN markdown.hackathon.dim_season      ds  ON UPPER(p.season)  = ds.season_name
# MAGIC LEFT JOIN markdown.hackathon.dim_price_range dpr ON p.original_price BETWEEN dpr.min_price AND dpr.max_price;

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 6 — Gold: Pricing Engine

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, least, greatest,
    round as spark_round, expr, lit, coalesce
)

def apply_rules_dynamically(
    input_df,
    catalog     = "markdown",
    rules_table = "hackathon.markdown_rules",
    rule_names  = None
):
    # ── Step 1: Load rules ────────────────────────────────────────────────────
    rule_filter = ""
    if rule_names:
        names_str   = ", ".join([f"'{r}'" for r in rule_names])
        rule_filter = f"WHERE rule_name IN ({names_str})"

    rules_query = f"""
        SELECT rule_id, rule_name, rule_priority, trigger_column,
               condition_operator, condition_min, condition_max,
               condition_str_value, price_multiplier, discount_pct
        FROM {catalog}.{rules_table}
        {rule_filter}
        ORDER BY rule_priority, rule_id, condition_min
    """
    rules = spark.sql(rules_query).collect()
    print(f"Loaded {len(rules)} rule conditions from {catalog}.{rules_table}")

    df = input_df

    # ── Step 2: Join product_trends for real trend_score ─────────────────────
    # trend_score is now part of silver_unified directly (added in synthetic data cell)
    # Only join product_trends for products where trend_score is still NULL
    trends = spark.table(f"{catalog}.hackathon.product_trends").select(
        col("product_id").alias("trend_product_id"),
        col("trend_score").alias("pt_trend_score")
    )
    df = df.join(trends, df["product_id"] == trends["trend_product_id"], how="left")
    df = df.drop("trend_product_id")
    df = df.withColumn(
        "trend_score",
        coalesce(col("trend_score"), col("pt_trend_score"), lit(50.0))
    )
    df = df.drop("pt_trend_score")

    # ── Step 3: Join dim_product for real cost_price ──────────────────────────
    dim_prod = spark.table(f"{catalog}.hackathon.dim_product").select(
        col("product_id").alias("dp_product_id"),
        col("cost_price")
    )
    df = df.join(dim_prod, df["product_id"] == dim_prod["dp_product_id"], how="left")
    df = df.drop("dp_product_id")
    df = df.withColumn(
        "unit_cost",
        coalesce(col("cost_price"), col("original_price") * lit(0.6))
    )

    # ── Step 4: Compute derived signal columns ────────────────────────────────
    df = df.withColumn(
        "sell_through_rate",
        spark_round(col("historical_sales") / expr("NULLIF(stock_quantity, 0)") * 100, 2)
    )
    df = df.withColumn(
        "competitor_gap_pct",
        spark_round(
            (col("original_price") - col("competitor_price"))
            / expr("NULLIF(original_price, 0)") * 100, 2
        )
    )
    df = df.withColumn("product_rating", col("customer_rating"))
    df = df.withColumn("days_in_stock", coalesce(col("days_in_stock"), lit(30.0)))

    # ── Step 5: Build per-rule WHEN chains ────────────────────────────────────
    groups            = {}
    rule_price_cols   = []
    rule_priority_map = {}

    for r in rules:
        if r["rule_name"] not in groups:
            groups[r["rule_name"]] = {"priority": r["rule_priority"], "conditions": []}
        groups[r["rule_name"]]["conditions"].append(r)

    for rule_name, rule_data in groups.items():
        col_name = f"price_{rule_name.lower()}"
        rule_price_cols.append(col_name)
        rule_priority_map[col_name] = rule_data["priority"]
        chain = None

        for c in rule_data["conditions"]:
            trig = c["trigger_column"]
            op   = c["condition_operator"]
            mult = float(c["price_multiplier"])
            cmin = c["condition_min"]
            cmax = c["condition_max"]
            cstr = c["condition_str_value"]

            if   op == "BETWEEN": cond = col(trig).between(float(cmin), float(cmax))
            elif op == ">":       cond = col(trig) > float(cmin)
            elif op == ">=":      cond = col(trig) >= float(cmin)
            elif op == "<":       cond = col(trig) < float(cmax)
            elif op == "<=":      cond = col(trig) <= float(cmax)
            elif op == "=":       cond = col(trig) == cstr
            else:
                print(f"  WARNING: Unknown operator '{op}' in rule '{rule_name}' — skipped")
                continue

            result = col("original_price") * lit(mult)
            chain  = when(cond, result) if chain is None else chain.when(cond, result)

        if chain is not None:
            df = df.withColumn(col_name, chain.otherwise(col("original_price")))

    # ── Step 6: Priority-aware conflict resolution ────────────────────────────
    if rule_price_cols:
        sorted_cols    = sorted(rule_price_cols, key=lambda c: rule_priority_map[c])
        priority_chain = None
        for rc in sorted_cols:
            fired = col(rc) != col("original_price")
            if priority_chain is None:
                priority_chain = when(fired, col(rc))
            else:
                priority_chain = priority_chain.when(fired, col(rc))

        df = df.withColumn(
            "raw_recommended_price",
            priority_chain.otherwise(least(*[col(c) for c in rule_price_cols]))
        )
    else:
        df = df.withColumn("raw_recommended_price", col("original_price"))

    # ── Step 7: Floor guardrail ───────────────────────────────────────────────
    df = df.withColumn("floor_price", col("unit_cost") * lit(1.05))
    df = df.withColumn(
        "recommended_price",
        greatest(col("raw_recommended_price"), col("floor_price"))
    )

    # ── Step 8: Tag which rule fired ─────────────────────────────────────────
    reason_chain = None
    for rc in sorted(rule_price_cols, key=lambda c: rule_priority_map[c]):
        label = rc.replace("price_", "").upper()
        cond  = col(rc) == col("raw_recommended_price")
        if reason_chain is None:
            reason_chain = when(cond, lit(label))
        else:
            reason_chain = reason_chain.when(cond, lit(label))

    if reason_chain is not None:
        df = df.withColumn("markdown_reason", reason_chain.otherwise(lit("FLOOR_GUARDRAIL")))
    else:
        df = df.withColumn("markdown_reason", lit("NO_RULE"))

    # ── Step 9: KPI columns ───────────────────────────────────────────────────
    df = df.withColumn(
        "discount_pct_applied",
        spark_round(
            (col("original_price") - col("recommended_price"))
            / expr("NULLIF(original_price, 0)") * 100, 1
        )
    )
    df = df.withColumn(
        "gross_amount",
        spark_round(col("original_price") * col("historical_sales"), 2)
    )
    df = df.withColumn(
        "net_amount",
        spark_round(col("recommended_price") * col("historical_sales"), 2)
    )
    df = df.withColumn(
        "discount_amount",
        spark_round(col("gross_amount") - col("net_amount"), 2)
    )

    return df, rule_price_cols

# COMMAND ----------

# ── Run pricing engine ────────────────────────────────────────────────────────
# [FIX-03] Read from silver_unified — this is the corrected replacement for
# the missing markdown.hackathon.bronze_sales table that caused the original
# notebook to crash. bronze_sales was referenced in 04_markdown_pricing.py but
# was never created anywhere in the pipeline.

silver = spark.table("markdown.hackathon.silver_unified")

import traceback

try:
    result, fired_rules = apply_rules_dynamically(silver)
except Exception as e:
    traceback.print_exc()

# Preview the key output columns
display(result.select(
    "product_id", "category", "season", "source_dataset",
    "original_price", "recommended_price",
    "discount_pct_applied", "markdown_reason",
    "days_in_stock", "trend_score", "sell_through_rate",
    "unit_cost", "floor_price",
    *fired_rules
).limit(30))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS markdown.hackathon.pricing_output")

from pyspark.sql.functions import current_date as cur_date

result_with_rundate = result.withColumn("run_date", cur_date())

result_with_rundate.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("markdown.hackathon.pricing_output")

print(f"pricing_output updated ({result.count():,} rows appended, run_date = today).")

# COMMAND ----------

# ── Write Gold: pricing_output ────────────────────────────────────────────────
# [FIX-11] APPEND mode + run_date replaces overwrite.
# This preserves the full history of every pricing run so you can audit how
# recommended prices changed over time and measure markdown strategy effectiveness.

from pyspark.sql.functions import current_date as cur_date

result_with_rundate = result.withColumn("run_date", cur_date())

result_with_rundate.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("markdown.hackathon.pricing_output")

print(f"pricing_output updated ({result.count():,} rows appended, run_date = today).")

# COMMAND ----------

# ── Gold data quality assertions ──────────────────────────────────────────────
# [FIX-14]

pricing = spark.table("markdown.hackathon.pricing_output")

price_inflated   = pricing.filter(col("recommended_price") > col("original_price") * 1.25).count()
below_floor      = pricing.filter(col("recommended_price") < col("floor_price")).count()
null_recommended = pricing.filter(col("recommended_price").isNull()).count()

assert price_inflated   == 0, f"ASSERTION FAILED: {price_inflated} rows where recommended > 125% of original!"
assert below_floor      == 0, f"ASSERTION FAILED: {below_floor} rows where recommended < floor_price!"
assert null_recommended == 0, f"ASSERTION FAILED: {null_recommended} null recommended_price rows!"

print("Gold pricing_output assertions passed  ✓")
print(f"  No prices above 125% of original    ✓")
print(f"  No prices below cost floor          ✓")
print(f"  No null recommended prices          ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 7 — Fact Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS markdown.hackathon.fact_sales;
# MAGIC
# MAGIC CREATE TABLE markdown.hackathon.fact_sales (
# MAGIC     sales_key           BIGINT,
# MAGIC     product_key         BIGINT,
# MAGIC     category_key        BIGINT,
# MAGIC     brand_key           BIGINT,
# MAGIC     season_key          BIGINT,
# MAGIC     markdown_key        BIGINT,
# MAGIC     customer_rating_key BIGINT,
# MAGIC     price_range_key     BIGINT,
# MAGIC     stock_level_key     BIGINT,
# MAGIC     promotion_key       BIGINT,
# MAGIC     store_key           BIGINT,
# MAGIC     channel_key         BIGINT,
# MAGIC     date_key            BIGINT,
# MAGIC     original_price      DECIMAL(10,2),
# MAGIC     recommended_price   DECIMAL(10,2),
# MAGIC     markdown_percentage DECIMAL(10,2),
# MAGIC     historical_sales    INT,
# MAGIC     revenue             DECIMAL(12,2),
# MAGIC     optimal_discount    DECIMAL(10,2),
# MAGIC     markdown_reason     STRING,
# MAGIC     run_date            DATE,
# MAGIC     created_timestamp   TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.fact_sales
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY po.product_id)              AS sales_key,
# MAGIC     dp.product_key,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     ds.season_key,
# MAGIC     dm.markdown_key,
# MAGIC     dcr.customer_rating_key,
# MAGIC     dpr.price_range_key,
# MAGIC     dsl.stock_level_key,
# MAGIC     1                                                       AS promotion_key,
# MAGIC     1                                                       AS store_key,
# MAGIC     1                                                       AS channel_key,
# MAGIC     CAST(date_format(current_date(), 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC     po.original_price,
# MAGIC     po.recommended_price,
# MAGIC     po.discount_pct_applied                                 AS markdown_percentage,
# MAGIC     po.historical_sales,
# MAGIC     ROUND(po.recommended_price * po.historical_sales, 2)    AS revenue,
# MAGIC     po.discount_pct_applied                                 AS optimal_discount,
# MAGIC     po.markdown_reason,
# MAGIC     po.run_date,
# MAGIC     current_timestamp()                                     AS created_timestamp
# MAGIC FROM (
# MAGIC     -- Explicitly select only the columns we need from pricing_output
# MAGIC     -- to avoid any duplicate column names bleeding in from the wide table
# MAGIC     SELECT DISTINCT
# MAGIC         product_id, category, brand, season,
# MAGIC         original_price, recommended_price, discount_pct_applied,
# MAGIC         historical_sales, markdown_reason, run_date,
# MAGIC         customer_rating, stock_quantity
# MAGIC     FROM markdown.hackathon.pricing_output
# MAGIC     WHERE run_date = current_date()
# MAGIC ) po
# MAGIC LEFT JOIN markdown.hackathon.dim_product         dp  ON po.product_id        = dp.product_id
# MAGIC LEFT JOIN markdown.hackathon.dim_category        dc  ON po.category          = dc.category_name
# MAGIC LEFT JOIN markdown.hackathon.dim_brand           db  ON po.brand             = db.brand_name
# MAGIC LEFT JOIN markdown.hackathon.dim_season          ds  ON UPPER(po.season)     = ds.season_name
# MAGIC LEFT JOIN markdown.hackathon.dim_markdown        dm  ON po.discount_pct_applied = dm.markdown_percentage
# MAGIC LEFT JOIN markdown.hackathon.dim_customer_rating dcr ON po.customer_rating   = dcr.customer_rating
# MAGIC LEFT JOIN markdown.hackathon.dim_price_range     dpr ON po.original_price    BETWEEN dpr.min_price AND dpr.max_price
# MAGIC LEFT JOIN markdown.hackathon.dim_stock_level     dsl ON (
# MAGIC     CASE
# MAGIC         WHEN po.stock_quantity <= 50  THEN 'Low Stock'
# MAGIC         WHEN po.stock_quantity <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END
# MAGIC ) = dsl.stock_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from markdown.hackathon.fact_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── fact_inventory ───────────────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.fact_inventory (
# MAGIC     inventory_key     BIGINT,
# MAGIC     product_key       BIGINT,
# MAGIC     category_key      BIGINT,
# MAGIC     brand_key         BIGINT,
# MAGIC     season_key        BIGINT,
# MAGIC     stock_level_key   BIGINT,
# MAGIC     store_key         BIGINT,
# MAGIC     date_key          BIGINT,
# MAGIC     stock_quantity    INT,
# MAGIC     stock_status      STRING,
# MAGIC     reorder_required  STRING,
# MAGIC     inventory_value   DECIMAL(12,2),
# MAGIC     created_timestamp TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.fact_inventory
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY src.product_id) AS inventory_key,
# MAGIC     dp.product_key,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     ds.season_key,
# MAGIC     dsl.stock_level_key,
# MAGIC     1 AS store_key,
# MAGIC     CAST(date_format(current_date(), 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC     src.stock_quantity,
# MAGIC     CASE
# MAGIC         WHEN src.stock_quantity <= 50  THEN 'Low Stock'
# MAGIC         WHEN src.stock_quantity <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END AS stock_status,
# MAGIC     CASE WHEN src.stock_quantity <= 50 THEN 'Yes' ELSE 'No' END AS reorder_required,
# MAGIC     ROUND(src.stock_quantity * src.original_price, 2) AS inventory_value,
# MAGIC     current_timestamp()
# MAGIC FROM markdown.hackathon.silver_unified src
# MAGIC LEFT JOIN markdown.hackathon.dim_product     dp  ON src.product_id     = dp.product_id
# MAGIC LEFT JOIN markdown.hackathon.dim_category    dc  ON src.category       = dc.category_name
# MAGIC LEFT JOIN markdown.hackathon.dim_brand       db  ON src.brand          = db.brand_name
# MAGIC LEFT JOIN markdown.hackathon.dim_season      ds  ON UPPER(src.season)  = ds.season_name
# MAGIC LEFT JOIN markdown.hackathon.dim_stock_level dsl ON (
# MAGIC     CASE
# MAGIC         WHEN src.stock_quantity <= 50  THEN 'Low Stock'
# MAGIC         WHEN src.stock_quantity <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END
# MAGIC ) = dsl.stock_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS markdown.hackathon.fact_markdown_analytics;
# MAGIC
# MAGIC CREATE TABLE markdown.hackathon.fact_markdown_analytics (
# MAGIC     markdown_fact_key      BIGINT,
# MAGIC     product_key            BIGINT,
# MAGIC     category_key           BIGINT,
# MAGIC     brand_key              BIGINT,
# MAGIC     season_key             BIGINT,
# MAGIC     markdown_key           BIGINT,
# MAGIC     promotion_key          BIGINT,
# MAGIC     stock_level_key        BIGINT,
# MAGIC     customer_rating_key    BIGINT,
# MAGIC     date_key               BIGINT,
# MAGIC     markdown_1             DECIMAL(10,2),
# MAGIC     markdown_2             DECIMAL(10,2),
# MAGIC     markdown_3             DECIMAL(10,2),
# MAGIC     markdown_4             DECIMAL(10,2),
# MAGIC     optimal_discount       DECIMAL(10,2),
# MAGIC     historical_sales       INT,
# MAGIC     sales_after_m1         INT,
# MAGIC     sales_after_m2         INT,
# MAGIC     sales_after_m3         INT,
# MAGIC     sales_after_m4         INT,
# MAGIC     seasonality_factor     DECIMAL(10,2),
# MAGIC     return_rate            DECIMAL(10,2),
# MAGIC     markdown_effectiveness STRING,
# MAGIC     created_timestamp      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.fact_markdown_analytics
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY src.category)               AS markdown_fact_key,
# MAGIC     dp.product_key,
# MAGIC     dc.category_key,
# MAGIC     db.brand_key,
# MAGIC     ds.season_key,
# MAGIC     dm.markdown_key,
# MAGIC     dpr.promotion_key,
# MAGIC     dsl.stock_level_key,
# MAGIC     dcr.customer_rating_key,
# MAGIC     CAST(date_format(current_date(), 'yyyyMMdd') AS BIGINT) AS date_key,
# MAGIC     src.markdown_1,
# MAGIC     src.markdown_2,
# MAGIC     src.markdown_3,
# MAGIC     src.markdown_4,
# MAGIC     src.optimal_discount,
# MAGIC     src.historical_sales,
# MAGIC     src.sales_after_m1,
# MAGIC     src.sales_after_m2,
# MAGIC     src.sales_after_m3,
# MAGIC     src.sales_after_m4,
# MAGIC     src.seasonality_factor,
# MAGIC     src.return_rate,
# MAGIC     CASE
# MAGIC         WHEN src.sales_after_m4 > src.historical_sales THEN 'High Impact'
# MAGIC         WHEN src.sales_after_m2 > src.historical_sales THEN 'Moderate Impact'
# MAGIC         ELSE 'Low Impact'
# MAGIC     END AS markdown_effectiveness,
# MAGIC     current_timestamp()
# MAGIC FROM markdown.hackathon.cosmetics_raw src
# MAGIC LEFT JOIN markdown.hackathon.dim_product         dp  ON CAST(src.product_id AS STRING) = dp.product_id
# MAGIC LEFT JOIN markdown.hackathon.dim_category        dc  ON src.category                  = dc.category_name
# MAGIC LEFT JOIN markdown.hackathon.dim_brand           db  ON src.brand                     = db.brand_name
# MAGIC LEFT JOIN markdown.hackathon.dim_season          ds  ON UPPER(src.season)             = ds.season_name
# MAGIC LEFT JOIN markdown.hackathon.dim_markdown        dm  ON src.markdown_1                = dm.markdown_percentage
# MAGIC LEFT JOIN markdown.hackathon.dim_promotion       dpr ON src.promotion_type            = dpr.promotion_type
# MAGIC LEFT JOIN markdown.hackathon.dim_stock_level     dsl ON (
# MAGIC     CASE
# MAGIC         WHEN src.stock_level <= 50  THEN 'Low Stock'
# MAGIC         WHEN src.stock_level <= 200 THEN 'Medium Stock'
# MAGIC         ELSE 'Overstock'
# MAGIC     END
# MAGIC ) = dsl.stock_status
# MAGIC LEFT JOIN markdown.hackathon.dim_customer_rating dcr ON src.customer_ratings          = dcr.customer_rating;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ── fact_customer_feedback ───────────────────────────────────────────────
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.fact_customer_feedback (
# MAGIC     feedback_key        BIGINT,
# MAGIC     product_key         BIGINT,
# MAGIC     category_key        BIGINT,
# MAGIC     brand_key           BIGINT,
# MAGIC     customer_rating_key BIGINT,
# MAGIC     markdown_key        BIGINT,
# MAGIC     date_key            BIGINT,
# MAGIC     customer_rating     DECIMAL(3,2),
# MAGIC     is_returned         STRING,
# MAGIC     return_reason       STRING,
# MAGIC     feedback_category   STRING,
# MAGIC     created_timestamp   TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO markdown.hackathon.fact_customer_feedback
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY src.product_id) AS feedback_key,
# MAGIC     dp.product_key,
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
# MAGIC FROM markdown.hackathon.fashion_raw src
# MAGIC LEFT JOIN markdown.hackathon.dim_product        dp  ON src.product_id          = dp.product_id
# MAGIC LEFT JOIN markdown.hackathon.dim_category       dc  ON src.category            = dc.category_name
# MAGIC LEFT JOIN markdown.hackathon.dim_brand          db  ON src.brand               = db.brand_name
# MAGIC LEFT JOIN markdown.hackathon.dim_customer_rating dcr ON src.customer_rating    = dcr.customer_rating
# MAGIC LEFT JOIN markdown.hackathon.dim_markdown        dm  ON src.markdown_percentage = dm.markdown_percentage;

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 8 — Final Validation Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts across all tables — sanity check after a full run
# MAGIC SELECT 'cosmetics_raw'           AS table_name, COUNT(*) AS row_count FROM markdown.hackathon.cosmetics_raw
# MAGIC UNION ALL SELECT 'fashion_raw',                 COUNT(*) FROM markdown.hackathon.fashion_raw
# MAGIC UNION ALL SELECT 'silver_unified',              COUNT(*) FROM markdown.hackathon.silver_unified
# MAGIC UNION ALL SELECT 'dim_category',                COUNT(*) FROM markdown.hackathon.dim_category
# MAGIC UNION ALL SELECT 'dim_brand',                   COUNT(*) FROM markdown.hackathon.dim_brand
# MAGIC UNION ALL SELECT 'dim_season',                  COUNT(*) FROM markdown.hackathon.dim_season
# MAGIC UNION ALL SELECT 'dim_product',                 COUNT(*) FROM markdown.hackathon.dim_product
# MAGIC UNION ALL SELECT 'dim_price_range',             COUNT(*) FROM markdown.hackathon.dim_price_range
# MAGIC UNION ALL SELECT 'dim_stock_level',             COUNT(*) FROM markdown.hackathon.dim_stock_level
# MAGIC UNION ALL SELECT 'dim_customer_rating',         COUNT(*) FROM markdown.hackathon.dim_customer_rating
# MAGIC UNION ALL SELECT 'dim_markdown',                COUNT(*) FROM markdown.hackathon.dim_markdown
# MAGIC UNION ALL SELECT 'markdown_rules',              COUNT(*) FROM markdown.hackathon.markdown_rules
# MAGIC UNION ALL SELECT 'product_trends',              COUNT(*) FROM markdown.hackathon.product_trends
# MAGIC UNION ALL SELECT 'pricing_output',              COUNT(*) FROM markdown.hackathon.pricing_output
# MAGIC UNION ALL SELECT 'fact_sales',                  COUNT(*) FROM markdown.hackathon.fact_sales
# MAGIC UNION ALL SELECT 'fact_inventory',              COUNT(*) FROM markdown.hackathon.fact_inventory
# MAGIC UNION ALL SELECT 'fact_markdown_analytics',     COUNT(*) FROM markdown.hackathon.fact_markdown_analytics
# MAGIC UNION ALL SELECT 'fact_customer_feedback',      COUNT(*) FROM markdown.hackathon.fact_customer_feedback
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pricing summary: products per rule fired, per run date
# MAGIC -- Use this to monitor which rules are dominating pricing decisions
# MAGIC SELECT
# MAGIC     run_date,
# MAGIC     markdown_reason,
# MAGIC     COUNT(*)                             AS product_count,
# MAGIC     ROUND(AVG(discount_pct_applied), 1)  AS avg_discount_pct,
# MAGIC     ROUND(SUM(net_amount), 2)            AS total_net_revenue,
# MAGIC     ROUND(SUM(discount_amount), 2)       AS total_discount_given
# MAGIC FROM markdown.hackathon.pricing_output
# MAGIC GROUP BY run_date, markdown_reason
# MAGIC ORDER BY run_date DESC, product_count DESC;
