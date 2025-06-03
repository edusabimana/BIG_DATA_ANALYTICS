import happybase
from datetime import datetime
import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
base_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(r"D:\Personal\masters\BIG DATA ANALYTICS\E_COMMERCE_ANALYTICS\data\sessions", "sessions_0.json")

# Initialize HBase connection
try:
    connection = happybase.Connection("localhost", port=9090)
    logger.info("Connected to HBase server at localhost:9090")
except Exception as e:
    logger.error(f"Failed to connect to HBase: {e}")
    raise

# Define table schemas and create if they don't exist
tables = {
    "user_sessions": {"session_info": dict(), "page_views": dict()},
    "product_metrics": {"views": dict(), "purchases": dict()}
}

for table_name, families in tables.items():
    table_name_bytes = table_name.encode()
    try:
        if table_name_bytes not in connection.tables():
            logger.info(f"Creating table {table_name}")
            connection.create_table(table_name, families)
            # Verify table creation
            if table_name_bytes in connection.tables():
                logger.info(f"Table {table_name} created successfully")
            else:
                logger.error(f"Failed to create table {table_name}")
                raise RuntimeError(f"Table {table_name} creation failed")
        else:
            logger.info(f"Table {table_name} already exists")
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        connection.close()
        raise

# Access tables
table_sessions = connection.table("user_sessions")
table_metrics = connection.table("product_metrics")

try:
    # Load and process sessions for user_sessions
    logger.info(f"Loading data from {data_path} into user_sessions")
    with open(data_path) as f:
        sessions = json.load(f)[:10000]  # Subset for testing
        for session in sessions:
            user_id = session["user_id"]
            try:
                ts = datetime.fromisoformat(session["start_time"].replace("Z", "+00:00"))
                reverse_ts = f"{(2**63 - int(ts.timestamp() * 1000)):022d}"
                row_key = f"{user_id}#{reverse_ts}"
                data = {
                    b"session_info:session_id": session["session_id"].encode(),
                    b"session_info:start_time": session["start_time"].encode(),
                    b"session_info:duration": str(session["duration_seconds"]).encode()
                }
                for i, pv in enumerate(session["page_views"]):
                    data[f"page_views:pv_{i}_type".encode()] = pv["page_type"].encode()
                    data[f"page_views:pv_{i}_product".encode()] = str(pv.get("product_id", "")).encode()
                table_sessions.put(row_key.encode(), data)
            except Exception as e:
                logger.warning(f"Error processing session for user {user_id}: {e}")
                continue
    logger.info("Finished loading data into user_sessions")

    # Load and process sessions for product_metrics
    logger.info(f"Loading data from {data_path} into product_metrics")
    with open(data_path) as f:
        sessions = json.load(f)[:10000]
        for session in sessions:
            date = session["start_time"].split("T")[0].replace("-", "")
            for pv in session["page_views"]:
                if pv["product_id"]:
                    row_key = f"{pv['product_id']}#{date}"
                    try:
                        table_metrics.put(row_key.encode(), {b"views:count": b"1"})
                    except Exception as e:
                        logger.warning(f"Error updating product_metrics for {row_key}: {e}")
                        continue
    logger.info("Finished loading data into product_metrics")

except Exception as e:
    logger.error(f"Error processing data: {e}")
    raise
finally:
    connection.close()
    logger.info("HBase connection closed")
    