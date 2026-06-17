# Databricks notebook source
# DBTITLE 1,Product
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `team_data-pirates_catalog`.hackathon.get_product_pricing(
# MAGIC     product STRING
# MAGIC )
# MAGIC RETURNS TABLE
# MAGIC RETURN
# MAGIC
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     original_price,
# MAGIC     recommended_price,
# MAGIC     discount_pct_applied,
# MAGIC     markdown_reason
# MAGIC FROM `team_data-pirates_catalog`.hackathon.pricing_output
# MAGIC WHERE product_id = product;

# COMMAND ----------

# DBTITLE 1,Overstock
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `team_data-pirates_catalog`.hackathon.get_overstock_products(
# MAGIC     inventory_limit INT
# MAGIC )
# MAGIC RETURNS TABLE
# MAGIC RETURN
# MAGIC
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     stock_quantity,
# MAGIC     recommended_price,
# MAGIC     markdown_reason
# MAGIC FROM `team_data-pirates_catalog`.hackathon.pricing_output
# MAGIC WHERE stock_quantity > inventory_limit;

# COMMAND ----------

# DBTITLE 1,Trend
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `team_data-pirates_catalog`.hackathon.get_trending_products(
# MAGIC     min_trend_score DOUBLE
# MAGIC )
# MAGIC RETURNS TABLE
# MAGIC RETURN
# MAGIC
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     category,
# MAGIC     trend_score,
# MAGIC     recommended_price
# MAGIC FROM `team_data-pirates_catalog`.hackathon.pricing_output
# MAGIC WHERE trend_score >= min_trend_score;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `team_data-pirates_catalog`.hackathon.markdown(
# MAGIC     product STRING
# MAGIC )
# MAGIC RETURNS TABLE
# MAGIC RETURN
# MAGIC
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     markdown_reason,
# MAGIC     discount_pct_applied,
# MAGIC     trend_score,
# MAGIC     stock_quantity
# MAGIC FROM `team_data-pirates_catalog`.hackathon.pricing_output
# MAGIC WHERE product_id = product;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION `team_data-pirates_catalog`.hackathon.get_profit()
# MAGIC RETURNS TABLE
# MAGIC RETURN
# MAGIC
# MAGIC SELECT
# MAGIC     category,
# MAGIC     SUM(stock_quantity) inventory,
# MAGIC     AVG(discount_pct_applied) avg_discount,
# MAGIC     SUM(net_amount) revenue
# MAGIC FROM `team_data-pirates_catalog`.hackathon.pricing_output
# MAGIC GROUP BY category;
