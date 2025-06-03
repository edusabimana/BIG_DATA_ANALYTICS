import happybase

connection = happybase.Connection("localhost", port=9090)
table_sessions = connection.table("user_sessions")

# Query: Retrieve recent sessions for a user
user_id = "user_000042"
start_row = f"{user_id}#0000000000000000000000"
end_row = f"{user_id}#9999999999999999999999"
for key, data in table_sessions.scan(row_start=start_row.encode(), row_stop=end_row.encode(), limit=10):
    print(f"Row: {key.decode()}")
    for k, v in data.items():
        print(f"  {k.decode()}: {v.decode()}")

connection.close()