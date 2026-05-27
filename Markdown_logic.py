# Databricks notebook source
# MAGIC %sql
# MAGIC -- 1. Create the persistent configuration metadata table
# MAGIC CREATE TABLE IF NOT EXISTS workspace.default.markdown_rules (
# MAGIC     rule_id INT,
# MAGIC     rule_name STRING,
# MAGIC     condition STRING,
# MAGIC     discount_percentage DOUBLE,
# MAGIC     priority INT
# MAGIC );
# MAGIC
# MAGIC -- 2. Populate the table with your specific business parameters
# MAGIC INSERT INTO workspace.default.markdown_rules VALUES
# MAGIC (1, 'Expiry_Clearance_Critical', 'Stock_Level > 100 AND Customer_Ratings < 3.0', 60.0, 1),
# MAGIC (2, 'Competitor_Match', 'Our_Price > Competitor_Price', 15.0, 2),
# MAGIC (3, 'Seasonality_Off_Season', 'Seasonality_Factor < 1.0', 20.0, 3),
# MAGIC (4, 'Customer_Bulk_Discount', 'Promotion_Type = ''Bulk''', 10.0, 4);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert Rule 5 (Product Performance Rule)
# MAGIC INSERT INTO workspace.default.markdown_rules VALUES
# MAGIC (5, 'Product_Performance_Low', 'Customer_Ratings < 3.0', 25.0, 5),
# MAGIC (6, 'Product_Performance_High', 'Customer_Ratings >= 4.5', 0.0, 6);
# MAGIC
# MAGIC -- Insert Rule 6 (Customer Behaviour Rule)
# MAGIC INSERT INTO workspace.default.markdown_rules VALUES
# MAGIC (7, 'Customer_Loyalty_Pricing', 'Promotion_Type = ''Loyal''', 5.0, 7);
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Ingest Kaggle Retail Dataset from Workspace Catalog
df_retail = spark.table("workspace.default.retail_markdown_final")

# Safety Check: Generate baseline store price if 'Our_Price' column is absent
if "Our_Price" not in df_retail.columns:
    df_retail = df_retail.withColumn("Our_Price", F.col("Competitor_Price") * 1.05)

# 2. Dynamic Rules Ingestion from Delta Metadata Catalog
df_rules = spark.table("workspace.default.markdown_rules")

# Sort rules structurally by business priority and collect them to the driver node
rules_list = df_rules.orderBy("priority").collect()

# 3. Dynamic Rule Application Matrix Logic with Bulletproof Handling
markdown_expression = F.lit(0.0)

for rule in reversed(rules_list):
    condition_str = rule["condition"]          
    discount_val = rule["discount_percentage"]  
    
    # SYSTEM DEFENSE PATCH: Dynamically wraps unquoted string assignments to prevent SQL engine crashes
    if "= Bulk" in condition_str:
        condition_str = condition_str.replace("= Bulk", "= 'Bulk'")
    if "= Regular" in condition_str:
        condition_str = condition_str.replace("= Regular", "= 'Regular'")
    if "= Loyal" in condition_str:
        condition_str = condition_str.replace("= Loyal", "= 'Loyal'")
    
    # Process declarative SQL conditions directly into columnar calculation logic
    markdown_expression = F.when(F.expr(condition_str), F.lit(discount_val)).otherwise(markdown_expression)

# 4. Compute Optimization Metrics and Final Price Matrices
df_final = df_retail \
    .withColumn("Optimal_Discount_Pct", markdown_expression) \
    .withColumn("Markdown_Amount", F.round(F.col("Our_Price") * F.col("Optimal_Discount_Pct") / 100.0, 2)) \
    .withColumn("Final_Marked_Down_Price", F.round(F.col("Our_Price") - F.col("Markdown_Amount"), 2))

# 5. Native Databricks Visual Evaluation Dashboard Display
display(df_final.select(
    "Our_Price",
    "Competitor_Price",
    "Stock_Level",
    "Seasonality_Factor",
    "Customer_Ratings",
    "Optimal_Discount_Pct",
    "Final_Marked_Down_Price"
))

