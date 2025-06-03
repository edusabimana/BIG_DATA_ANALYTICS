from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size

# Initialize Spark session with a custom UI port
spark = SparkSession.builder \
    .appName("EcommerceAnalytics") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

# Load subset of transactions
transactions_df = spark.read.json(r"D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data\transactions.json").limit(10000)
transactions_df = transactions_df.na.fill({"discount": 0.0, "session_id": "none"})
transactions_df.show(5)

# Product recommendation
sessions_df = spark.read.json(r"D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data\sessions\sessions_0.json").limit(10000)
viewed_products_df = sessions_df.filter(size(col("viewed_products")) > 1).select("session_id", "viewed_products")
pairs_df = viewed_products_df.select(
    explode(col("viewed_products")).alias("product1"),
    col("viewed_products").alias("products")
).select(
    col("product1"),
    explode(col("products")).alias("product2")
).filter(col("product1") < col("product2"))
product_pairs = pairs_df.groupBy("product1", "product2").count().orderBy(col("count").desc())
product_pairs.show(10)

# Spark SQL: Revenue by category
transactions_df.createOrReplaceTempView("transactions")
products_df = spark.read.json(r"D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data\products.json").limit(1000)
products_df.createOrReplaceTempView("products")

# Create a temporary view for exploded transactions
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW exploded_transactions AS
    SELECT t.*, item.product_id AS item_product_id
    FROM transactions t
    LATERAL VIEW EXPLODE(t.items) AS item
""")

# Query using the exploded view
result = spark.sql("""
    SELECT p.category_id, SUM(t.total) AS total_revenue
    FROM exploded_transactions t
    JOIN products p ON t.item_product_id = p.product_id
    GROUP BY p.category_id
    ORDER BY total_revenue DESC
""")
result.show()

spark.stop()