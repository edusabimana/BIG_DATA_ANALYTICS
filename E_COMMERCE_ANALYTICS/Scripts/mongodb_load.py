import os
import json
from pymongo import MongoClient
from tqdm import tqdm

# === CONFIG ===
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "db_ecommerce"
DATA_DIR = "D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data"

# === INIT CLIENT ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# === COLLECTION MAPPING ===
collections = {
    "users.json": "users",
    "categories.json": "categories",
    "products.json": "products",
    "transactions.json": "transactions"
}

# === LOAD JSON FILES ===
def load_json_file(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]

# === LOAD MAIN ENTITIES ===
for filename, collection_name in collections.items():
    print(f"\nLoading {filename} into {collection_name}...")
    file_path = os.path.join(DATA_DIR, filename)
    data = load_json_file(file_path)

    if data:
        db[collection_name].drop()
        db[collection_name].insert_many(data)
        print(f"✔ Loaded {len(data)} records into {collection_name}")
    else:
        print(f"⚠ No data found in {filename}")

# === LOAD SESSIONS ===
print("\nLoading session files into 'sessions' collection...")
sessions_path = os.path.join(DATA_DIR, "sessions")
session_files = [f for f in os.listdir(sessions_path) if f.endswith(".json")]

all_sessions = []
for file in tqdm(session_files, desc="Reading session files"):
    path = os.path.join(sessions_path, file)
    all_sessions.extend(load_json_file(path))

if all_sessions:
    db["sessions"].drop()
    db["sessions"].insert_many(all_sessions)
    print(f"✔ Loaded {len(all_sessions)} sessions into 'sessions' collection")
else:
    print("⚠ No session data found.")

# === OPTIONAL INDEXES ===
print("\nCreating indexes...")
db["users"].create_index("user_id", unique=True)
db["products"].create_index("product_id", unique=True)
db["transactions"].create_index("transaction_id", unique=True)
db["sessions"].create_index("session_id", unique=True)

print("\n✅ All data successfully loaded into MongoDB.")