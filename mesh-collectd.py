import ssl
import json
import time
import os
import sqlite3
import threading
from queue import Queue
from datetime import datetime
from paho.mqtt.client import Client


def load_config(config_path="config.json"):
    try:
        with open(config_path, "r") as config_file:
            return json.load(config_file)
    except Exception as e:
        print(f"Error loading config file: {e}")
        exit(1)


def log_message(msg_type, node_id, details):
    """
    Standardized logging function that formats all messages consistently.

    Args:
        msg_type (str): Type of message (MESSAGE, NODEINFO, NEIGHBORS, POSITION, TRACEROUTE)
        node_id: ID of the node
        details (dict): Additional details to log
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    base_info = f"[{timestamp}] [{msg_type}] Node {node_id}"

    if msg_type == "MESSAGE":
        print(
            f"{base_info} → {details['receiver']} | "
            f"Type: {details['type']} | RSSI: {details['rssi']} | "
            f"SNR: {details['snr']} | Physical Sender: {details['physical_sender']}"
        )

    elif msg_type == "NODEINFO":
        print(
            f"{base_info} | "
            f"Long: {details['longname']} | Short: {details['shortname']} | "
            f"HW: {details['hardware']} | Role: {details['role']}"
        )

    elif msg_type == "NEIGHBORS":
        print(f"{base_info} | Neighbor count: {details['count']}")
        for neighbor in details["neighbors"]:
            print(f"    → Neighbor {neighbor['node_id']} | SNR: {neighbor.get('snr', 'N/A')}")

    elif msg_type == "POSITION":
        print(f"{base_info} | " f"Lat: {details['latitude']:.5f} | Lon: {details['longitude']:.5f}")

    elif msg_type == "TRACEROUTE":
        print(f"{base_info} | Route length: {len(details['route'])}")
        for i in range(len(details["route"]) - 1):
            print(f"    {details['route'][i]} → {details['route'][i+1]}")


def init_db(db_path="mqtt_messages.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop and recreate nodes table with explicit PRIMARY KEY constraint
    # cursor.execute("DROP TABLE IF EXISTS nodes")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS nodes (
               id INTEGER NOT NULL PRIMARY KEY,  -- This ensures uniqueness
               longname TEXT,
               shortname TEXT,
               hardware INTEGER,
               role INTEGER,
               last_seen INTEGER,
               latitude REAL,
               longitude REAL
           )"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS messages (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               topic TEXT,
               sender INTEGER,
               receiver INTEGER,
               physical_sender INTEGER,
               timestamp INTEGER,
               rssi REAL,
               snr REAL,
               type TEXT
           )"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS neighbors (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               node_id INTEGER,
               neighbor_id INTEGER,
               snr REAL,
               timestamp INTEGER,
               FOREIGN KEY (node_id) REFERENCES nodes (id),
               FOREIGN KEY (neighbor_id) REFERENCES nodes (id)
           )"""
    )

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS traceroutes (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               from_node TEXT,
               to_node TEXT,
               timestamp INTEGER
           )"""
    )

    # Add new nodes_count table
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS nodes_count (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               node_id INTEGER,
               timestamp INTEGER,
               count_30min INTEGER,
               count_60min INTEGER,
               count_120min INTEGER
           )"""
    )

    conn.commit()
    return conn


def save_nodes_count_to_db(conn, node_id, timestamp, counts):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT INTO nodes_count 
               (node_id, timestamp, count_30min, count_60min, count_120min)
               VALUES (?, ?, ?, ?, ?)""",
            (node_id, timestamp, counts.get("30min", 0), counts.get("60min", 0), counts.get("120min", 0)),
        )
        conn.commit()
    except Exception as e:
        print(f"Error saving node counts: {e}")
        conn.rollback()


def save_nodeinfo_to_db(conn, node_id, longname=None, shortname=None, hardware=None, role=None, timestamp=None, latitude=None, longitude=None):
    cursor = conn.cursor()
    try:
        # First check if the node exists
        cursor.execute("SELECT longname, shortname, hardware, role, latitude, longitude FROM nodes WHERE id = ?", (node_id,))
        existing = cursor.fetchone()

        if existing:
            # Update existing node, keeping old values when new ones aren't provided
            update_values = [
                longname if longname is not None else existing[0],
                shortname if shortname is not None else existing[1],
                hardware if hardware is not None else existing[2],
                role if role is not None else existing[3],
                timestamp,  # Always update timestamp if provided
                latitude if latitude is not None else existing[4],
                longitude if longitude is not None else existing[5],
                node_id
            ]
            
            cursor.execute(
                """UPDATE nodes 
                   SET longname = ?, shortname = ?, hardware = ?, role = ?, 
                       last_seen = ?, latitude = ?, longitude = ?
                   WHERE id = ?""",
                tuple(update_values)
            )
        else:
            # Insert new node
            cursor.execute(
                """INSERT INTO nodes 
                   (id, longname, shortname, hardware, role, last_seen, latitude, longitude)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (node_id, longname, shortname, hardware, role, timestamp, latitude, longitude)
            )
        
        conn.commit()
        print(f"Successfully {'updated' if existing else 'inserted'} node {node_id}")
    
    except Exception as e:
        print(f"Error saving node info for node {node_id}: {e}")
        conn.rollback()


def save_traceroute_to_db(conn, route, timestamp):
    cursor = conn.cursor()
    try:
        # Process each consecutive pair in the route
        for i in range(len(route) - 1):
            from_node = sanitize_string(route[i])
            to_node = sanitize_string(route[i + 1])

            if "Unknown" not in [from_node, to_node]:
                cursor.execute(
                    """INSERT INTO traceroutes (from_node, to_node, timestamp)
                    VALUES (?, ?, ?)""",
                    (from_node, to_node, timestamp),
                )
            conn.commit()
    except Exception as e:
        print(f"Error saving traceroute info: {e}")


def save_message_to_db(conn, topic, sender, receiver, physical_sender, timestamp, rssi, snr, type):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO messages (topic, sender, receiver, physical_sender, timestamp, rssi, snr, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (topic, sender, receiver, physical_sender, timestamp, rssi, snr, type),
    )
    conn.commit()


def save_neighbors_to_db(conn, node_id, neighbors, timestamp):
    cursor = conn.cursor()
    try:
        # First, delete old neighbor entries for this node
        cursor.execute("DELETE FROM neighbors WHERE node_id = ? AND timestamp = ?", (node_id, timestamp))

        # Insert new neighbor relationships
        for neighbor in neighbors:
            cursor.execute(
                """INSERT INTO neighbors (node_id, neighbor_id, snr, timestamp)
                   VALUES (?, ?, ?, ?)""",
                (node_id, neighbor["node_id"], neighbor["snr"], timestamp),
            )
        conn.commit()
    except Exception as e:
        print(f"Error saving neighbor info: {e}")


def db_worker(queue, db_path):
    conn = sqlite3.connect(db_path)
    while True:
        item = queue.get()
        if item is None:
            break  # Stop signal
        try:
            if item[0] == "message":
                save_message_to_db(conn, *item[1:])
            elif item[0] == "nodeinfo":
                save_nodeinfo_to_db(conn, *item[1:])
            elif item[0] == "neighbors":
                save_neighbors_to_db(conn, *item[1:])
            elif item[0] == "position":
                save_nodeinfo_to_db(conn, *item[1:])
            elif item[0] == "traceroute":
                save_traceroute_to_db(conn, *item[1:])
            elif item[0] == "nodes_count":
                save_nodes_count_to_db(conn, *item[1:])
        except Exception as e:
            print(f"Database error: {e}")
    conn.close()


def hex_to_int(hex_id):
    """Convert hex node ID to integer, removing the leading '!' if present"""
    try:
        if hex_id.startswith("!"):
            hex_id = hex_id[1:]
        return int(hex_id, 16)
    except (ValueError, AttributeError):
        print(f"Failed to convert hex ID: {hex_id}")
        return None


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(config["MQTT_TOPIC"])
    else:
        print(f"Failed to connect, return code {rc}")


def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT Broker. Attempting to reconnect...")
    while True:
        try:
            client.reconnect()
            print("Reconnected to MQTT Broker!")
            break
        except Exception:
            time.sleep(5)


def sanitize_string(input_str):
    """
    Sanitize and clean a string by:
    1. Converting to ASCII (removing non-ASCII characters)
    2. Stripping leading and trailing whitespaces
    3. Replacing multiple whitespaces with a single space
    """
    # if not isinstance(input_str, str):
    #     return input_str

    input_str = str(input_str)

    # Convert to ASCII, strip, and replace multiple spaces
    return " ".join(input_str.encode("ascii", "ignore").decode("ascii").split())


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except:
            return

        node_id = payload.get("from")
        receiver = payload.get("to")
        timestamp = payload.get("timestamp")
        rssi = payload.get("rssi")
        snr = payload.get("snr")
        msg_type = payload.get("type")
        physical_sender = hex_to_int(payload.get("sender"))

        # Log base message information
        log_message("MESSAGE", node_id, {"receiver": receiver, "type": msg_type, "rssi": rssi, "snr": snr, "physical_sender": physical_sender})

        userdata.put(("message", topic, node_id, receiver, physical_sender, timestamp, rssi, snr, msg_type))

        # Handle nodes count messages
        if "nodes_count" in topic:
            try:
                node_id = int(topic.split("/")[-1], 16)  # Convert hex node ID to int
                payload = json.loads(msg.payload.decode("utf-8"))
                timestamp = int(time.time())

                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [NODES_COUNT] "
                    f"Node {node_id} | 30min: {payload.get('30min', 0)} | "
                    f"60min: {payload.get('60min', 0)} | 120min: {payload.get('120min', 0)}"
                )

                userdata.put(("nodes_count", node_id, timestamp, payload))
                return
            except Exception as e:
                print(f"Error processing nodes count message: {e}")
                return

        # Rest of the existing message handling...
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except:
            return

        if msg_type == "nodeinfo" and "payload" in payload:
            node_payload = payload["payload"]
            if all(k in node_payload for k in ["id", "longname", "shortname", "hardware", "role"]):
                log_message(
                    "NODEINFO",
                    node_id,
                    {
                        "longname": sanitize_string(node_payload["longname"]),
                        "shortname": sanitize_string(node_payload["shortname"]),
                        "hardware": node_payload["hardware"],
                        "role": node_payload["role"],
                    },
                )

                userdata.put(
                    (
                        "nodeinfo",
                        node_id,
                        sanitize_string(node_payload["longname"]),
                        sanitize_string(node_payload["shortname"]),
                        node_payload["hardware"],
                        node_payload["role"],
                        timestamp,
                    )
                )

        elif msg_type == "neighborinfo" and "payload" in payload:
            neighbor_payload = payload["payload"]
            if "node_id" in neighbor_payload and "neighbors" in neighbor_payload:
                neighbors = neighbor_payload["neighbors"]
                log_message("NEIGHBORS", node_id, {"count": len(neighbors), "neighbors": neighbors})

                userdata.put(("neighbors", node_id, neighbors, timestamp))

        elif msg_type == "traceroute" and "payload" in payload:
            route_payload = payload["payload"]
            if "route" in route_payload:
                route = route_payload["route"]
                current_time = int(time.time())

                log_message("TRACEROUTE", node_id, {"route": route})

                userdata.put(("traceroute", route, current_time))

        elif msg_type == "position" and "payload" in payload:
            pos_payload = payload["payload"]
            if all(k in pos_payload for k in ["latitude_i", "longitude_i"]):
                latitude = float(int(pos_payload["latitude_i"]) * 1e-7)
                longitude = float(int(pos_payload["longitude_i"]) * 1e-7)

                log_message("POSITION", node_id, {"latitude": latitude, "longitude": longitude})

                # Update only location information for the node
                userdata.put(
                    (
                        "position",
                        node_id,  # node_id
                        None,  # longname (no update)
                        None,  # shortname (no update)
                        None,  # hardware (no update)
                        None,  # role (no update)
                        timestamp,  # last_seen
                        latitude,  # latitude
                        longitude,  # longitude
                    )
                )

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ERROR] Error processing message: {e}")


def cleanup_duplicate_nodes(conn):
    cursor = conn.cursor()
    try:
        # Find and remove duplicates, keeping the most recent entry
        cursor.execute(
            """
            DELETE FROM nodes 
            WHERE rowid NOT IN (
                SELECT MAX(rowid)
                FROM nodes
                GROUP BY id
            )
        """
        )
        conn.commit()
        print("Cleaned up duplicate node entries")
    except Exception as e:
        print(f"Error cleaning up duplicates: {e}")
        conn.rollback()


# Load configuration
config = load_config()

# Initialize database
db_path = "mqtt_messages.db"
db_conn = init_db(db_path)

cleanup_duplicate_nodes(db_conn)

# Create a thread-safe queue for database operations
message_queue = Queue()

# Start the database worker thread
db_thread = threading.Thread(target=db_worker, args=(message_queue, db_path), daemon=True)
db_thread.start()

# Create MQTT client
client = Client(client_id=config.get("CLIENT_ID"))
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# Pass the queue to the MQTT client as userdata
client.user_data_set(message_queue)

# Set username and password if provided
if config.get("MQTT_USERNAME") and config.get("MQTT_PASSWORD"):
    client.username_pw_set(config["MQTT_USERNAME"], config["MQTT_PASSWORD"])

# Enable SSL if specified
if config.get("USE_SSL"):
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)

# Connect to the MQTT broker
client.connect(config["MQTT_BROKER"], config["MQTT_PORT"], 60)

# Start MQTT loop
client.loop_start()

# Keep the script running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Disconnecting...")
finally:
    # Stop the MQTT client
    client.loop_stop()
    client.disconnect()

    # Stop the database worker
    message_queue.put(None)  # Signal the worker to exit
    db_thread.join()
    print("Disconnected and database closed.")
