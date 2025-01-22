import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict

data="data" # data directory

def get_node_shortname(cursor, node_id):
    """Get shortname for a node ID from the nodes table"""
    cursor.execute("SELECT shortname FROM nodes WHERE id = ?", (node_id,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return f"!{node_id:x}"  # Return hex format if no shortname found

def export_neighbors_to_json(db_path, json_output_path, time_limit_minutes):
    try:
        # Establish a connection to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Calculate the timestamp cutoff based on the given time limit
        cutoff_time = int((datetime.now() - timedelta(minutes=time_limit_minutes)).timestamp())

        # Query the database for the newest neighbor relationships not older than the cutoff time
        cursor.execute(
            """SELECT node_id, neighbor_id, snr, COUNT(*) as appearance_count
               FROM neighbors
               WHERE timestamp >= ?
               GROUP BY node_id, neighbor_id""",
            (cutoff_time,)
        )
        rows = cursor.fetchall()

        # Count connections for each node
        connection_counts = defaultdict(int)
        valid_connections = []

        # First pass: count valid connections and store them
        for row in rows:
            node_id, neighbor_id, snr, appearance_count = row
            
            # Skip invalid records
            if node_id < 2 or neighbor_id < 2 or node_id > 4294967294 or neighbor_id > 4294967294:
                continue
                
            # Store valid connection
            valid_connections.append((node_id, neighbor_id, snr, appearance_count))
            
            # Count connections for both nodes
            connection_counts[node_id] += 1
            connection_counts[neighbor_id] += 1

        # Prepare the data in the Cytoscape.js JSON format
        cytoscape_data = []
        processed_nodes = set()

        # Add nodes with their connection counts
        for node_id, connections in connection_counts.items():
            node_hex = f"!{node_id:x}"
            node_label = get_node_shortname(cursor, node_id)
            
            cytoscape_data.append({
                "data": {
                    "id": node_hex,
                    "label": node_label,
                    "connections": connections
                }
            })
            processed_nodes.add(node_hex)

        # Add edges (neighbor relationships)
        for node_id, neighbor_id, snr, appearance_count in valid_connections:
            node_hex = f"!{node_id:x}"
            neighbor_hex = f"!{neighbor_id:x}"

            cytoscape_data.append({
                "data": {
                    "id": f"{node_hex}_{neighbor_hex}",
                    "source": node_hex,
                    "target": neighbor_hex,
                    "snr": snr,
                    "weight": 2 if snr > 0 else 1,  # Optional: weight based on SNR quality
                    "appearance_count": appearance_count  # Number of times this neighbor relationship was seen
                }
            })

        # Write the data to the JSON output file
        with open(json_output_path, "w") as json_file:
            json.dump(cytoscape_data, json_file, indent=2)

        print(f"Neighbor data successfully exported to {json_output_path}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if conn:
            conn.close()

def export_to_json(db_path, json_output_path, time_limit_minutes):
    try:
        # Establish a connection to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Calculate the timestamp cutoff based on the given time limit
        cutoff_time = int((datetime.now() - timedelta(minutes=time_limit_minutes)).timestamp())

        # Query the database for the newest message per sender-receiver pair not older than the cutoff time
        cursor.execute(
            """SELECT sender, receiver, COUNT(*) as count, rssi 
                   FROM messages
                   WHERE timestamp >= ?
                   GROUP BY sender, receiver""",
            (cutoff_time,)
        )
        rows = cursor.fetchall()

        # Count connections for each node
        connection_counts = defaultdict(int)
        valid_connections = []

        # First pass: count valid connections
        for row in rows:
            sender, receiver, count, rssi = row
            
            # Skip invalid records
            if sender < 2 or receiver < 2 or sender > 4294967294 or receiver > 4294967294:
                continue
                
            # Store valid connection
            valid_connections.append((sender, receiver, count, rssi))
            
            # Count connections for both sender and receiver
            connection_counts[sender] += 1
            connection_counts[receiver] += 1

        # Prepare the data in the Cytoscape.js JSON format
        cytoscape_data = []
        processed_nodes = set()  # Keep track of processed nodes

        # Add nodes with their connection counts
        for node_id, connections in connection_counts.items():
            node_hex = f"!{node_id:x}"
            node_label = get_node_shortname(cursor, node_id)
            
            cytoscape_data.append({
                "data": {
                    "id": node_hex,
                    "label": node_label,
                    "connections": connections
                }
            })
            processed_nodes.add(node_hex)

        # Add edges
        for sender, receiver, count, rssi in valid_connections:
            sender_hex = f"!{sender:x}"
            receiver_hex = f"!{receiver:x}"

            cytoscape_data.append({
                "data": {
                    "id": f"{sender_hex}_{receiver_hex}",
                    "source": sender_hex,
                    "target": receiver_hex,
                    "rssi": rssi,
                    "count": count,
                }
            })

        # Write the data to the JSON output file
        with open(json_output_path, "w") as json_file:
            json.dump(cytoscape_data, json_file, indent=2)

        print(f"Data successfully exported to {json_output_path}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        if conn:
            conn.close()

# Example usage
if __name__ == "__main__":
    # Path to the SQLite database
    db_path = "mqtt_messages.db"

    # Time windows in minutes: 1 hour, 3 hours, 24 hours
    time_windows = [15, 30, 60, 3*60, 24*60]
    
    # Generate files for each time window
    for minutes in time_windows:
        # Create descriptive time window string
        if minutes == 60:
            time_str = "1h"
        elif minutes == 3*60:
            time_str = "3h"
        elif minutes == 24*60:
            time_str = "24h"
        elif minutes == 30:
            time_str = "30min"
        elif minutes == 15:
            time_str = "15min"
            
        # Generate filenames with time window
        messages_json = f"{data}/cytoscape_messages_{time_str}.json"
        neighbors_json = f"{data}/cytoscape_neighbors_{time_str}.json"
        
        print(f"\nExporting data for {time_str} time window...")
        
        # Export both message and neighbor data
        export_to_json(db_path, messages_json, minutes)
        export_neighbors_to_json(db_path, neighbors_json, minutes)