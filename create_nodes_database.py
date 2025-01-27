import sqlite3

# Source and destination database paths
source_db_path = "mqtt_messages.db"
dest_db_path = "mqtt_messages_nodes.db"

# Connect to source database
source_conn = sqlite3.connect(source_db_path)
source_cursor = source_conn.cursor()

# Connect to destination database
dest_conn = sqlite3.connect(dest_db_path)
dest_cursor = dest_conn.cursor()

# Get the nodes table schema
columns = source_cursor.execute("PRAGMA table_info(nodes)").fetchall()
column_names = [col[1] for col in columns]

# Create table creation and insert statements dynamically
create_table_sql = f"""CREATE TABLE IF NOT EXISTS nodes (
    {', '.join([f"{col[1]} {col[2]}" for col in columns])}
)"""
dest_cursor.execute(create_table_sql)

# Fetch nodes data
nodes_data = source_cursor.execute("SELECT * FROM nodes").fetchall()

# Prepare insert statement dynamically
insert_columns = ', '.join(column_names)
placeholders = ', '.join(['?' for _ in column_names])
insert_sql = f"INSERT INTO nodes ({insert_columns}) VALUES ({placeholders})"

# Insert nodes data into destination database
dest_cursor.executemany(insert_sql, nodes_data)

# Commit changes and close connections
dest_conn.commit()
source_conn.close()
dest_conn.close()

print(f"Nodes table copied to {dest_db_path}. Total nodes: {len(nodes_data)}")