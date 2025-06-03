from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure  # Updated import
from pprint import pprint

# === MongoDB Setup ===
try:
    client = MongoClient("mongodb://localhost:27017")
    db = client["e_commerce_db"]
    client.admin.command('ping')
except ConnectionFailure as e:  # Updated exception
    print(f"❌ MongoDB connection failed: {e}")
    exit(1)

# === Helper Function for Query Execution ===
def run_query(collection, pipeline, title):
    try:
        results = list(db[collection].aggregate(pipeline))
        if results:
            print(f"\n{title} ({len(results)} found):")
            return results
        print(f"⚠ No data for {title.lower()}.")
        return []
    except OperationFailure as e:
        print(f"❌ Error in {title.lower()}: {e}")
        return []

# === 1. Top-Selling Products ===
pipeline_sales = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id", "sold": {"$sum": "$items.quantity"}, "revenue": {"$sum": "$items.subtotal"}}},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "product_id", "as": "product"}},
    {"$unwind": "$product"},
    {"$project": {"product_id": "$_id", "name": "$product.name", "sold": 1, "revenue": 1, "_id": 0}},
    {"$sort": {"sold": -1}},
    {"$limit": 10}
]
top_sellers = run_query("transactions", pipeline_sales, "Top 10 Best-Selling Products")
for p in top_sellers:
    print(f"- {p['name']} (ID: {p['product_id']}), Sold: {p['sold']}, Revenue: ${p['revenue']:.2f}")

# === 2. Revenue by Category ===
pipeline_revenue = [
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id", "foreignField": "product_id", "as": "product"}},
    {"$unwind": "$product"},
    {"$lookup": {"from": "categories", "localField": "product.category_id", "foreignField": "category_id", "as": "category"}},
    {"$unwind": "$category"},
    {"$group": {"_id": "$category.category_id", "name": {"$first": "$category.name"}, "revenue": {"$sum": "$items.subtotal"}}},
    {"$project": {"category_id": "$_id", "name": 1, "revenue": 1, "_id": 0}},
    {"$sort": {"revenue": -1}}
]
categories = run_query("transactions", pipeline_revenue, "Revenue by Category")
for c in categories:
    print(f"- {c['name']} (ID: {c['category_id']}), Revenue: ${c['revenue']:.2f}")

# === 3. User Segmentation by Purchase Frequency ===
pipeline_segmentation = [
    {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
    {"$lookup": {"from": "users", "localField": "_id", "foreignField": "user_id", "as": "user"}},
    {"$unwind": "$user"},
    {"$bucket": {
        "groupBy": "$count",
        "boundaries": [1, 2, 5, 10, 20],
        "default": "20+",
        "output": {"user_count": {"$sum": 1}, "users": {"$push": {"user_id": "$_id", "name": "$user.name", "count": "$count"}}}
    }},
    {"$project": {
        "segment": {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$_id", 1]}, "then": "One-time"},
                    {"case": {"$eq": ["$_id", 2]}, "then": "Occasional (2-4)"},
                    {"case": {"$eq": ["$_id", 5]}, "then": "Regular (5-9)"},
                    {"case": {"$eq": ["$_id", 10]}, "then": "Frequent (10-19)"},
                    {"case": {"$eq": ["$_id", "20+"]}, "then": "Loyal (20+)"}
                ]
            }
        },
        "user_count": 1, "users": {"$slice": ["$users", 3]}, "_id": 0
    }},
    {"$sort": {"user_count": -1}}
]
segments = run_query("transactions", pipeline_segmentation, "User Segmentation by Purchase Frequency")
for s in segments:
    print(f"- {s['segment']}, Users: {s['user_count']}")
    for u in s['users']:
        print(f"  * {u['name']} (ID: {u['user_id']}), Purchases: {u['count']}")

# === Close Connection ===
client.close()
print("\n✅ Done.")