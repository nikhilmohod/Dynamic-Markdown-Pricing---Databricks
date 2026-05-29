# Databricks notebook source
# %sql
# CREATE CATALOG IF NOT EXISTS markdown;
# USE CATALOG markdown 

# COMMAND ----------

# %sql
# CREATE SCHEMA IF NOT EXISTS hackathon;
# USE SCHEMA hackathon

# COMMAND ----------

# %sql
# CREATE VOLUME IF NOT EXISTS raw_data;

# COMMAND ----------

# df = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("/Volumes/markdown/hackathon/raw_data/SYNTHETIC Markdown Dataset.csv")

# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.markdown_rules
# MAGIC (
# MAGIC     rule_id              INT,          
# MAGIC     rule_name            STRING ,     
# MAGIC     trigger_column       STRING ,     
# MAGIC     condition_operator   STRING ,    
# MAGIC     condition_min        DOUBLE,
# MAGIC     condition_max        DOUBLE,
# MAGIC     condition_str_value  STRING,
# MAGIC     price_multiplier     DOUBLE ,
# MAGIC     discount_pct         DOUBLE ,
# MAGIC     created_by           STRING,
# MAGIC     created_date         DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 2: Insert all rules — only the columns you kept
# MAGIC INSERT INTO markdown.hackathon.markdown_rules
# MAGIC     (rule_id, rule_name, trigger_column, condition_operator,
# MAGIC      condition_min, condition_max, condition_str_value,
# MAGIC      price_multiplier, discount_pct, created_by, created_date)
# MAGIC VALUES
# MAGIC
# MAGIC -- ── Rule 1: EXPIRY ──────────────────────────────────────────────────────
# MAGIC (1, 'EXPIRY', 'days_in_stock', 'BETWEEN',  0,   2,  NULL, 0.40, 60,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 'days_in_stock', 'BETWEEN',  3,   7,  NULL, 0.55, 45,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 'days_in_stock', 'BETWEEN',  8,  14,  NULL, 0.75, 25,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 'days_in_stock', 'BETWEEN', 15,  21,  NULL, 0.80, 20,  'system', current_date()),
# MAGIC (1, 'EXPIRY', 'days_in_stock', 'BETWEEN', 22,  30,  NULL, 0.90, 10,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 2: SALES VELOCITY ──────────────────────────────────────────────
# MAGIC (2, 'SALES_VELOCITY', 'sell_through_rate', '<',       0,  20, NULL, 0.65, 35,  'system', current_date()),
# MAGIC (2, 'SALES_VELOCITY', 'sell_through_rate', 'BETWEEN', 20, 40, NULL, 0.80, 20,  'system', current_date()),
# MAGIC (2, 'SALES_VELOCITY', 'sell_through_rate', 'BETWEEN', 40, 70, NULL, 0.90, 10,  'system', current_date()),
# MAGIC (2, 'SALES_VELOCITY', 'sell_through_rate', '>',       70, -1, NULL, 1.00,  0,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 3: SEASONAL ────────────────────────────────────────────────────
# MAGIC (3, 'SEASONAL', 'Season', '=', NULL, NULL, 'SUMMER',      1.05,  -5, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 'Season', '=', NULL, NULL, 'WINTER',      1.05,  -5, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 'Season', '=', NULL, NULL, 'OFF',         0.80,  20, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 'Season', '=', NULL, NULL, 'FESTIVAL',    1.08,  -8, 'system', current_date()),
# MAGIC (3, 'SEASONAL', 'Season', '=', NULL, NULL, 'TRANSITIONAL',0.93,   7, 'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 4: COMPETITOR ──────────────────────────────────────────────────
# MAGIC (4, 'COMPETITOR', 'competitor_gap_pct', '<',       -100, -10, NULL, 1.05, -5,  'system', current_date()),
# MAGIC (4, 'COMPETITOR', 'competitor_gap_pct', 'BETWEEN',   -5,   5, NULL, 1.00,  0,  'system', current_date()),
# MAGIC (4, 'COMPETITOR', 'competitor_gap_pct', 'BETWEEN',    5,  15, NULL, 0.93,  7,  'system', current_date()),
# MAGIC (4, 'COMPETITOR', 'competitor_gap_pct', '>',          15, -1, NULL, 0.82, 18,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 5: PRODUCT RATING ──────────────────────────────────────────────
# MAGIC (5, 'PRODUCT_RATING', 'product_rating', '>=',      4.5,  -1, NULL, 1.00,  0,  'system', current_date()),
# MAGIC (5, 'PRODUCT_RATING', 'product_rating', 'BETWEEN', 3.5, 4.4, NULL, 0.93,  7,  'system', current_date()),
# MAGIC (5, 'PRODUCT_RATING', 'product_rating', '<',        0,  3.5, NULL, 0.82, 18,  'system', current_date()),
# MAGIC
# MAGIC -- ── Rule 6: TRENDING ────────────────────────────────────────────────────
# MAGIC (6, 'TRENDING', 'trend_score', '>',       80,  -1, NULL, 1.10, -10, 'system', current_date()),
# MAGIC (6, 'TRENDING', 'trend_score', 'BETWEEN', 50,  80, NULL, 1.00,   0, 'system', current_date()),
# MAGIC (6, 'TRENDING', 'trend_score', 'BETWEEN', 20,  50, NULL, 0.90,  10, 'system', current_date()),
# MAGIC (6, 'TRENDING', 'trend_score', '<',        0,  20, NULL, 0.80,  20, 'system', current_date());

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     rule_id,
# MAGIC     rule_name,
# MAGIC     trigger_column,
# MAGIC     condition_operator,
# MAGIC     condition_min,
# MAGIC     condition_max,
# MAGIC     condition_str_value,
# MAGIC     price_multiplier,
# MAGIC     discount_pct
# MAGIC FROM markdown.hackathon.markdown_rules
# MAGIC ORDER BY rule_id, condition_min;

# COMMAND ----------

# DBTITLE 1,Cell 9
from pyspark.sql.functions import (
    col, when, least, greatest,
    round as spark_round, expr, lit
)

def apply_rules_dynamically(
    input_df,
    catalog     = "markdown",
    rules_table = "hackathon.markdown_rules",
    rule_names  = None    # None = all rules; or ['EXPIRY','SEASONAL']
):

    # ── Step 1: Load rules from UC table (F-string query) ─────────────────
    rule_filter = ""
    if rule_names:
        names_str   = ", ".join([f"'{r}'" for r in rule_names])
        rule_filter = f"WHERE rule_name IN ({names_str})"

    rules_query = f"""
        SELECT
            rule_id,
            rule_name,
            trigger_column,
            condition_operator,
            condition_min,
            condition_max,
            condition_str_value,
            price_multiplier,
            discount_pct
        FROM {catalog}.{rules_table}
        {rule_filter}
        ORDER BY rule_id, condition_min
    """

    rules = spark.sql(rules_query).collect()
    print(f"Loaded {len(rules)} rule conditions")

    df = input_df

    # ── Step 2: Compute derived columns needed by rules ───────────────────
    # Note: Using available columns from bronze_sales schema
    df = df \
        .withColumn("sell_through_rate",
            spark_round(
                col("Historical_Sales") / expr("NULLIF(Stock_Level, 0)") * 100, 2)) \
        .withColumn("competitor_gap_pct",
            spark_round(
                (col("Original_Price") - col("Competitor_Price"))
                / expr("NULLIF(Original_Price, 0)") * 100, 2)) \
        .withColumn("days_in_stock", lit(15.0)) \
        .withColumn("product_rating", col("Customer_Ratings")) \
        .withColumn("trend_score", lit(50.0)) \
        .withColumn("unit_cost", col("Original_Price") * lit(0.6))

    # ── Step 3: Group rules by rule_name ──────────────────────────────────
    groups = {}
    for r in rules:
        groups.setdefault(r["rule_name"], []).append(r)

    rule_price_cols = []

    for rule_name, conditions in groups.items():

        col_name = f"price_{rule_name.lower()}"
        rule_price_cols.append(col_name)
        chain    = None

        for c in conditions:

            trig = c["trigger_column"]
            op   = c["condition_operator"]
            mult = float(c["price_multiplier"])
            cmin = c["condition_min"]
            cmax = c["condition_max"]
            cstr = c["condition_str_value"]

            # ── Build condition from operator ──────────────────────────────
            if   op == "BETWEEN":
                cond = col(trig).between(float(cmin), float(cmax))
            elif op == ">":
                cond = col(trig) > float(cmin)
            elif op == "<":
                cond = col(trig) < float(cmax)
            elif op == ">=":
                cond = col(trig) >= float(cmin)
            elif op == "<=":
                cond = col(trig) <= float(cmax)
            elif op == "=":
                cond = col(trig) == cstr
            else:
                continue

            result = col("Original_Price") * lit(mult)

            # Chain into WHEN - fixed to avoid boolean conversion
            if chain is None:
                chain = when(cond, result)
            else:
                chain = chain.when(cond, result)

        if chain is not None:
            df = df.withColumn(
                col_name,
                chain.otherwise(col("Original_Price"))
            )

    # ── Step 4: Conflict resolution — lowest price wins ───────────────────
    if rule_price_cols:
        df = df.withColumn(
            "raw_recommended_price",
            least(*[col(c) for c in rule_price_cols])
        )
    else:
        df = df.withColumn(
            "raw_recommended_price",
            col("Original_Price")
        )

    # ── Step 5: Floor price (simple — unit_cost × 1.05) ───────────────────
    df = df.withColumn(
        "floor_price",
        col("unit_cost") * lit(1.05)
    )

    # ── Step 6: Apply floor guardrail ─────────────────────────────────────
    df = df.withColumn(
        "recommended_price",
        greatest(col("raw_recommended_price"), col("floor_price"))
    )

    # ── Step 7: Which rule fired? ─────────────────────────────────────────
    reason_chain = None
    for c in rule_price_cols:
        label = c.replace("price_", "").upper()
        cond  = col(c) == col("raw_recommended_price")
        if reason_chain is None:
            reason_chain = when(cond, lit(label))
        else:
            reason_chain = reason_chain.when(cond, lit(label))

    if reason_chain is not None:
        df = df.withColumn(
            "markdown_reason",
            reason_chain.otherwise(lit("FLOOR_GUARDRAIL"))
        )
    else:
        df = df.withColumn("markdown_reason", lit("NO_RULE"))

    # ── Step 8: Final KPI columns ─────────────────────────────────────────
    df = df \
        .withColumn("discount_pct_applied",
            spark_round(
                (col("Original_Price") - col("recommended_price"))
                / expr("NULLIF(Original_Price, 0)") * 100, 1)) \
        .withColumn("gross_amount",
            spark_round(col("Original_Price") * col("Historical_Sales"), 2)) \
        .withColumn("net_amount",
            spark_round(col("recommended_price") * col("Historical_Sales"), 2)) \
        .withColumn("discount_amount",
            spark_round(col("gross_amount") - col("net_amount"), 2))

    return df, rule_price_cols

# COMMAND ----------

# DBTITLE 1,Cell 10
bronze = spark.table("markdown.hackathon.bronze_sales")
result, fired = apply_rules_dynamically(bronze)
display(result.select(
    "Product_ID", "Category",
    "Original_Price", "recommended_price",
    "discount_pct_applied", "markdown_reason",
    *fired
).limit(20))

result.write.mode("overwrite").saveAsTable(
    "markdown.hackathon.pricing_output"
)

# COMMAND ----------

# from pyspark.sql.functions import (
#     col, when, least, greatest,
#     round as spark_round, expr, lit
# )

# def apply_rules_dynamically(
#     input_df,
#     catalog     = "markdown_pricing_catalog",
#     rules_table = "silver.markdown_rules",
#     rule_names  = None   # None = all rules; or ['EXPIRY','SEASONAL']
# ):

#     # ── Step 1: Build F-string SQL query (as senior described) ─────────────
#     rule_filter = ""
#     if rule_names:
#         names_str   = ", ".join([f"'{r}'" for r in rule_names])
#         rule_filter = f"WHERE rule_name IN ({names_str})"

#     # Triple-quoted F-string — exactly what senior said at 3:09
#     rules_query = f"""
#         SELECT
#             rule_id,
#             rule_name,
#             trigger_column,
#             condition_operator,
#             condition_min,
#             condition_max,
#             condition_str_value,
#             price_multiplier,
#             discount_pct
#         FROM {catalog}.{rules_table}
#         {rule_filter}
#         ORDER BY rule_id, condition_min
#     """

#     # collect() — senior mentioned this at 0:59
#     rules = spark.sql(rules_query).collect()
#     print(f"Loaded {len(rules)} rule conditions from {catalog}.{rules_table}")

#     df = input_df

#     # ── Step 2: Compute derived signals ───────────────────────────────────
#     df = df \
#         .withColumn("sell_through_rate",
#             spark_round(
#                 col("Units_Sold") / expr("NULLIF(Stock_Level, 0)") * 100, 2)) \
#         .withColumn("competitor_gap_pct",
#             spark_round(
#                 (col("Original_Price") - col("Competitor_Price"))
#                 / expr("NULLIF(Original_Price, 0)") * 100, 2)) \
#         .withColumn("days_in_stock",
#             col("Days_In_Stock").cast("double"))

#     # ── Step 3: Group rules and build WHEN chains dynamically ──────────────
#     groups = {}
#     for r in rules:
#         groups.setdefault(r["rule_name"], []).append(r)

#     rule_price_cols = []

#     for rule_name, conditions in groups.items():

#         col_name = f"price_{rule_name.lower()}"
#         rule_price_cols.append(col_name)
#         chain    = None

#         for c in conditions:
#             trig = c["trigger_column"]
#             op   = c["condition_operator"]
#             mult = float(c["price_multiplier"])
#             cmin = c["condition_min"]
#             cmax = c["condition_max"]
#             cstr = c["condition_str_value"]

#             # Build condition from operator — CASE/IF from table (senior 7:03)
#             if   op == "BETWEEN":
#                 cond = col(trig).between(float(cmin), float(cmax))
#             elif op == ">":
#                 cond = col(trig) > float(cmin)
#             elif op == "<":
#                 cond = col(trig) < float(cmax)
#             elif op == ">=":
#                 cond = col(trig) >= float(cmin)
#             elif op == "<=":
#                 cond = col(trig) <= float(cmax)
#             elif op == "=":
#                 cond = col(trig) == cstr
#             else:
#                 continue

#             result = col("Original_Price") * lit(mult)

#             chain  = when(cond, result) \
#                      if chain is None   \
#                      else chain.when(cond, result)

#         if chain is not None:
#             df = df.withColumn(
#                 col_name,
#                 chain.otherwise(col("Original_Price"))
#             )

#     # ── Step 4: Conflict resolution — lowest price wins ───────────────────
#     if rule_price_cols:
#         df = df.withColumn(
#             "raw_recommended_price",
#             least(*[col(c) for c in rule_price_cols])
#         )
#     else:
#         df = df.withColumn(
#             "raw_recommended_price",
#             col("Original_Price")
#         )

#     # ── Step 5: Simple floor price — unit_cost × 1.05 ─────────────────────
#     # floor_multiplier removed from schema so hardcoded here
#     df = df.withColumn(
#         "floor_price",
#         col("unit_cost") * lit(1.05)
#     )

#     # ── Step 6: Apply floor guardrail ─────────────────────────────────────
#     df = df.withColumn(
#         "recommended_price",
#         greatest(col("raw_recommended_price"), col("floor_price"))
#     )

#     # ── Step 7: Which rule fired? ─────────────────────────────────────────
#     reason_chain = None
#     for c in rule_price_cols:
#         label = c.replace("price_", "").upper()
#         cond  = col(c) == col("raw_recommended_price")
#         reason_chain = when(cond, lit(label)) \
#                        if reason_chain is None  \
#                        else reason_chain.when(cond, lit(label))

#     df = df.withColumn(
#         "markdown_reason",
#         reason_chain.otherwise(lit("FLOOR_GUARDRAIL"))
#         if reason_chain else lit("NO_RULE")
#     )

#     # ── Step 8: Final KPI columns ─────────────────────────────────────────
#     df = df \
#         .withColumn("discount_pct_applied",
#             spark_round(
#                 (col("Original_Price") - col("recommended_price"))
#                 / expr("NULLIF(Original_Price, 0)") * 100, 1)) \
#         .withColumn("gross_amount",
#             spark_round(col("Original_Price") * col("Units_Sold"), 2)) \
#         .withColumn("net_amount",
#             spark_round(col("recommended_price") * col("Units_Sold"), 2)) \
#         .withColumn("discount_amount",
#             spark_round(col("gross_amount") - col("net_amount"), 2))

#     return df, rule_price_cols

# COMMAND ----------

# DBTITLE 1,Cell 10
# # Load Bronze data
# bronze = spark.table("markdown.hackathon.bronze_sales")

# # Apply all rules
# result, fired = apply_rules_dynamically(bronze)

# # Check output
# display(result.select(
#     "Product_ID", "Category",
#     "Original_Price", "recommended_price",
#     "discount_pct_applied", "markdown_reason",
#     *fired
# ).limit(20))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS markdown.hackathon.product_trends (
# MAGIC   trend_id INT,
# MAGIC   product_id STRING,
# MAGIC   trend_type STRING,
# MAGIC   trend_score DOUBLE,
# MAGIC   trend_direction STRING,
# MAGIC   trend_source STRING,
# MAGIC   trend_date DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO markdown.hackathon.product_trends VALUES
# MAGIC (1, 'P101', 'HIGH_TREND', 90, 'UP', 'SOCIAL_MEDIA', current_date()),
# MAGIC (2, 'P102', 'LOW_TREND', 20, 'DOWN', 'SALES_PATTERN', current_date()),
# MAGIC (3, 'P103', 'SEASONAL_TREND', 75, 'UP', 'FESTIVAL', current_date());
