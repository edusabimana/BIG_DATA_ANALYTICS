from pymongo import MongoClient
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# MongoDB: Purchase totals
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce"]
pipeline = [{"$group": {"_id": "$user_id", "total_spend": {"$sum": "$total"}}}]
purchase_data = list(db.transactions.aggregate(pipeline))

# HBase: Session counts
connection = happybase.Connection("localhost", port=9090)
table = connection.table("user_sessions")
session_counts = {}
for user_id in set(d["_id"] for d in purchase_data):
    start_row = f"{user_id}#0000000000000000000000"
    end_row = f"{user_id}#9999999999999999999999"
    count = sum(1 for _ in table.scan(row_start=start_row.encode(), row_stop=end_row.encode()))
    session_counts[user_id] = count
connection.close()

# Spark: Compute CLV
spark = SparkSession.builder.appName("CLV").getOrCreate()
purchase_df = spark.createDataFrame(
    [(d["_id"], d["total_spend"]) for d in purchase_data],
    ["user_id", "total_spend"]
)
session_df = spark.createDataFrame(
    [(k, v) for k, v in session_counts.items()],
    ["user_id", "session_count"]
)
clv_df = purchase_df.join(session_df, "user_id", "inner").withColumn(
    "clv", col("total_spend") * (col("session_count") / 10.0)
)
clv_df.show(10)