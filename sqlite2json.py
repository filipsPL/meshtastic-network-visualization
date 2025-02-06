import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict

import matplotlib.pyplot as plt
import matplotlib.dates as mdates


data = "data"  # data directory


def get_node_info(cursor, node_id):
    """Get shortname and role for a node ID from the nodes table"""
    cursor.execute("SELECT shortname, role FROM nodes WHERE id = ?", (node_id,))
    result = cursor.fetchone()
    if result:
        return result[0], result[1]  # shortname, role
    return f"!{node_id:x}", None  # Return hex format if no shortname found, None for role


def export_neighbors_to_json(db_path, json_output_path, time_limit_minutes):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cutoff_time = int((datetime.now() - timedelta(minutes=time_limit_minutes)).timestamp())

        cursor.execute(
            """SELECT node_id, neighbor_id, snr, COUNT(*) as appearance_count
               FROM neighbors
               WHERE timestamp >= ?
               GROUP BY node_id, neighbor_id""",
            (cutoff_time,),
        )
        rows = cursor.fetchall()

        connection_counts = defaultdict(int)
        valid_connections = []

        for row in rows:
            node_id, neighbor_id, snr, appearance_count = row

            if node_id < 2 or neighbor_id < 2 or node_id > 4294967294 or neighbor_id > 4294967294:
                continue

            valid_connections.append((node_id, neighbor_id, snr, appearance_count))

            connection_counts[node_id] += 1
            connection_counts[neighbor_id] += 1

        cytoscape_data = []
        processed_nodes = set()

        for node_id, connections in connection_counts.items():
            node_hex = f"!{node_id:x}"
            node_label, node_role = get_node_info(cursor, node_id)

            node_label = node_label if node_label else node_hex

            cytoscape_data.append({"data": {"id": node_hex, "label": node_label, "role": node_role, "connections": connections}})
            processed_nodes.add(node_hex)

        for node_id, neighbor_id, snr, appearance_count in valid_connections:
            node_hex = f"!{node_id:x}"
            neighbor_hex = f"!{neighbor_id:x}"

            cytoscape_data.append(
                {
                    "data": {
                        "id": f"{node_hex}_{neighbor_hex}",
                        "source": node_hex,
                        "target": neighbor_hex,
                        "snr": snr,
                        "weight": 2 if snr > 0 else 1,
                        "appearance_count": appearance_count,
                    }
                }
            )

        with open(json_output_path, "w") as json_file:
            json.dump(cytoscape_data, json_file, indent=2)

        print(f"Neighbor data successfully exported to {json_output_path}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


def export_to_json(db_path, json_output_path, time_limit_minutes, use_physical_sender=False):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        current_time = int(datetime.now().timestamp())
        cutoff_time = int((datetime.now() - timedelta(minutes=time_limit_minutes)).timestamp())

        # Use physical_sender instead of sender if specified
        sender_field = "physical_sender" if use_physical_sender else "sender"

        cursor.execute(
            f"""SELECT {sender_field}, receiver, COUNT(*) as count, rssi 
               FROM messages
               WHERE timestamp >= ? AND timestamp <= ?
               GROUP BY {sender_field}, receiver""",
            (cutoff_time, current_time),
        )
        rows = cursor.fetchall()

        connection_counts = defaultdict(int)
        valid_connections = []

        for row in rows:
            sender, receiver, count, rssi = row

            if sender < 2 or receiver < 2 or sender > 4294967294 or receiver > 4294967294:
                continue

            valid_connections.append((sender, receiver, count, rssi))

            connection_counts[sender] += 1
            connection_counts[receiver] += 1

        cytoscape_data = []
        processed_nodes = set()

        for node_id, connections in connection_counts.items():
            node_hex = f"!{node_id:x}"
            node_label, node_role = get_node_info(cursor, node_id)

            node_label = node_label if node_label else node_hex

            cytoscape_data.append({"data": {"id": node_hex, "label": node_label, "role": node_role, "connections": connections}})
            processed_nodes.add(node_hex)

        for sender, receiver, count, rssi in valid_connections:
            sender_hex = f"!{sender:x}"
            receiver_hex = f"!{receiver:x}"

            cytoscape_data.append(
                {
                    "data": {
                        "id": f"{sender_hex}_{receiver_hex}",
                        "source": sender_hex,
                        "target": receiver_hex,
                        "rssi": rssi,
                        "count": count,
                    }
                }
            )

        with open(json_output_path, "w") as json_file:
            json.dump(cytoscape_data, json_file, indent=2)

        print(f"Data successfully exported to {json_output_path}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


def get_node_shortname_and_role(cursor, longname):
    """Get shortname and role for a node by its longname from the nodes table"""
    cursor.execute("SELECT shortname, role FROM nodes WHERE longname = ?", (longname,))
    result = cursor.fetchone()
    if result:
        return result[0], result[1]  # shortname, role
    return longname, None  # Return original longname if no match found

def export_traceroutes_to_json(db_path, json_output_path, time_limit_minutes):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cutoff_time = int((datetime.now() - timedelta(minutes=time_limit_minutes)).timestamp())
        current_time = int(datetime.now().timestamp())

        cursor.execute(
            """SELECT from_node, to_node, COUNT(*) as count
               FROM traceroutes
               WHERE timestamp >= ? AND timestamp <= ?
               GROUP BY from_node, to_node""",
            (cutoff_time,current_time),
        )
        rows = cursor.fetchall()

        connection_counts = defaultdict(int)
        valid_connections = []

        for row in rows:
            from_node, to_node, count = row
            valid_connections.append((from_node, to_node, count))
            connection_counts[from_node] += 1
            connection_counts[to_node] += 1

        cytoscape_data = []
        processed_nodes = set()

        # Add nodes
        for node_name in connection_counts.keys():
            if node_name not in processed_nodes:
                shortname, role = get_node_shortname_and_role(cursor, node_name)
                node_id = shortname  # Use shortname as node ID

                label = shortname if shortname else node_name
                
                cytoscape_data.append({
                    "data": {
                        "id": node_id,
                        "label": label,
                        "role": role,
                        "connections": connection_counts[node_name],
                        "original_name": node_name
                    }
                })
                processed_nodes.add(node_name)

        # Add edges
        for from_node, to_node, count in valid_connections:
            from_shortname, _ = get_node_shortname_and_role(cursor, from_node)
            to_shortname, _ = get_node_shortname_and_role(cursor, to_node)

            cytoscape_data.append({
                "data": {
                    "id": f"{from_shortname}_{to_shortname}",
                    "source": from_shortname,
                    "target": to_shortname,
                    "count": count,
                    "original_source": from_node,
                    "original_target": to_node
                }
            })

        with open(json_output_path, "w") as json_file:
            json.dump(cytoscape_data, json_file, indent=2)

        print(f"Traceroute data successfully exported to {json_output_path}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


def export_hourly_messages(db_path, days=1):
    """
    Export the number of messages received in each hour for the past N days to a JSON file,
    broken down by message type.
    
    Parameters:
    db_path (str): Path to the SQLite database
    days (int): Number of days to look back (default: 1)
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Calculate the cutoff timestamp
        cutoff_time = int((datetime.now() - timedelta(days=days)).timestamp())
        current_time = int(datetime.now().timestamp())
        
        # Query to get message counts by hour and type
        cursor.execute("""
            SELECT 
                strftime('%Y-%m-%d %H:00:00', datetime(timestamp, 'unixepoch', 'localtime')) as hour,
                type,
                COUNT(*) as message_count
            FROM messages 
            WHERE timestamp >= ? AND timestamp <= ?
            GROUP BY hour, type
            ORDER BY hour, type
        """, (cutoff_time,current_time))
        
        rows = cursor.fetchall()
        
        # Process data into format suitable for stacked bar chart
        hours = []
        message_types = set()
        hour_type_counts = defaultdict(lambda: defaultdict(int))
        
        for row in rows:
            hour_str, msg_type, count = row
            hours.append(hour_str)
            message_types.add(msg_type)
            hour_type_counts[hour_str][msg_type] = count
        
        hours = sorted(set(hours))
        message_types = sorted(message_types)
        
        # Create data structure for Plotly
        plotly_data = {
            "x": hours,
            "types": list(message_types),
            "data": {},
            "metadata": {
                "days": days,
                "generated_at": datetime.now().isoformat(),
                "total_messages": 0,
                "messages_by_type": defaultdict(int)
            }
        }
        
        # Fill in the data for each type
        for msg_type in message_types:
            plotly_data["data"][msg_type] = []
            for hour in hours:
                count = hour_type_counts[hour][msg_type]
                plotly_data["data"][msg_type].append(count)
                plotly_data["metadata"]["total_messages"] += count
                plotly_data["metadata"]["messages_by_type"][msg_type] += count
        
        # Calculate percentages for each type
        total = plotly_data["metadata"]["total_messages"]
        if total > 0:
            for msg_type in message_types:
                plotly_data["metadata"]["messages_by_type"][f"{msg_type}_percentage"] = (
                    plotly_data["metadata"]["messages_by_type"][msg_type] / total * 100
                )
            
        # Save to JSON file
        output_path = f"{data}/hourly_messages_by_type_{days}d.json"
        with open(output_path, "w") as f:
            json.dump(plotly_data, f, indent=2)
            
        print(f"Hourly message data by type exported to {output_path}")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


def export_hourly_unique_senders(db_path, days=1):
    """
    Export the number of unique senders and physical senders in each hour for the past N days.
    
    Parameters:
    db_path (str): Path to the SQLite database
    days (int): Number of days to look back (default: 1)
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Calculate the cutoff timestamp
        cutoff_time = int((datetime.now() - timedelta(days=days)).timestamp())
        current_time = int(datetime.now().timestamp())
        
        # Query to get unique sender counts by hour
        cursor.execute("""
            SELECT 
                strftime('%Y-%m-%d %H:00:00', datetime(timestamp, 'unixepoch', 'localtime')) as hour,
                COUNT(DISTINCT sender) as unique_senders,
                COUNT(DISTINCT physical_sender) as unique_physical_senders
            FROM messages 
            WHERE timestamp >= ? AND timestamp <= ?
            GROUP BY hour
            ORDER BY hour
        """, (cutoff_time, current_time))
        
        rows = cursor.fetchall()
        
        # Process data into format suitable for Plotly
        plotly_data = {
            "x": [],  # hours
            "unique_senders": [],
            "unique_physical_senders": [],
            "metadata": {
                "days": days,
                "generated_at": datetime.now().isoformat(),
                "total_unique_senders": 0,
                "total_unique_physical_senders": 0,
                "average_unique_senders_per_hour": 0,
                "average_unique_physical_senders_per_hour": 0
            }
        }
        
        total_unique_senders = 0
        total_unique_physical_senders = 0
        hour_count = 0
        
        for row in rows:
            hour_str, unique_senders, unique_physical_senders = row
            plotly_data["x"].append(hour_str)
            plotly_data["unique_senders"].append(unique_senders)
            plotly_data["unique_physical_senders"].append(unique_physical_senders)
            
            total_unique_senders += unique_senders
            total_unique_physical_senders += unique_physical_senders
            hour_count += 1
        
        # Calculate averages
        if hour_count > 0:
            plotly_data["metadata"]["average_unique_senders_per_hour"] = total_unique_senders / hour_count
            plotly_data["metadata"]["average_unique_physical_senders_per_hour"] = total_unique_physical_senders / hour_count
        
        # Get total unique senders across all hours
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT sender) as total_unique_senders,
                COUNT(DISTINCT physical_sender) as total_unique_physical_senders
            FROM messages 
            WHERE timestamp >= ? AND timestamp <= ?
        """, (cutoff_time, current_time))
        
        total_row = cursor.fetchone()
        if total_row:
            plotly_data["metadata"]["total_unique_senders"] = total_row[0]
            plotly_data["metadata"]["total_unique_physical_senders"] = total_row[1]
            
        # Save to JSON file
        output_path = f"{data}/hourly_unique_senders_{days}d.json"
        with open(output_path, "w") as f:
            json.dump(plotly_data, f, indent=2)
            
        print(f"Hourly unique senders data exported to {output_path}")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    db_path = "mqtt_messages.db"
    time_windows = [15, 30, 60, 3 * 60, 24 * 60]

    for minutes in time_windows:
        if minutes == 60:
            time_str = "1h"
        elif minutes == 3 * 60:
            time_str = "3h"
        elif minutes == 24 * 60:
            time_str = "24h"
        elif minutes == 30:
            time_str = "30min"
        elif minutes == 15:
            time_str = "15min"

        # Generate filenames with time window
        messages_json = f"{data}/cytoscape_messages_{time_str}.json"
        messages_physical_json = f"{data}/cytoscape_messages_physical_{time_str}.json"
        neighbors_json = f"{data}/cytoscape_neighbors_{time_str}.json"
        traceroutes_json = f"{data}/cytoscape_traceroutes_{time_str}.json"  # New file for traceroutes

        print(f"\nExporting data for {time_str} time window...")

        # Export all data types
        export_to_json(db_path, messages_json, minutes, use_physical_sender=False)
        export_to_json(db_path, messages_physical_json, minutes, use_physical_sender=True)
        export_neighbors_to_json(db_path, neighbors_json, minutes)
        export_traceroutes_to_json(db_path, traceroutes_json, minutes) 

    print("\nGenerating message count plots...")
    for days in [1, 7, 14]:
        export_hourly_messages(db_path, days=days)
        export_hourly_unique_senders(db_path, days=days)