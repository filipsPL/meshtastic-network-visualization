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


def init_db(db_path="mqtt_messages.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS nodes (
               id INTEGER PRIMARY KEY,
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

    conn.commit()
    return conn


def save_nodeinfo_to_db(conn, node_id, longname=None, shortname=None, hardware=None, role=None, timestamp=None, latitude=None, longitude=None):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT INTO nodes (id, longname, shortname, hardware, role, last_seen, latitude, longitude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
               longname=COALESCE(?, nodes.longname),
               shortname=COALESCE(?, nodes.shortname),
               hardware=COALESCE(?, nodes.hardware),
               role=COALESCE(?, nodes.role),
               last_seen=MAX(excluded.last_seen, nodes.last_seen),
               latitude=COALESCE(?, nodes.latitude),
               longitude=COALESCE(?, nodes.longitude)""",
            (node_id, longname, shortname, hardware, role, timestamp, latitude, longitude,
             longname, shortname, hardware, role, latitude, longitude)
        )
        conn.commit()
    except Exception as e:
        print(f"Error saving node info: {e}")


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


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except:
            return

        # https://meshtastic.org/docs/software/integrations/mqtt/
        # "sender" is the hexadecimal Node ID of the gateway device
        # "from" is the unique decimal-equivalent Node ID of the node on the mesh that sent this message.
        # "to" is the decimal-equivalent Node ID of the destination of the message.


        # Primary node identification using 'from'
        node_id = payload.get("from")
        receiver = payload.get("to")
        timestamp = payload.get("timestamp")
        rssi = payload.get("rssi")
        snr = payload.get("snr")
        msg_type = payload.get("type")

        physical_sender = hex_to_int(payload.get("sender"))

        # pretty pring the message
        print(
            f"Node {node_id} received message from {receiver} at {timestamp} with RSSI {rssi} and SNR {snr} and type {msg_type}"
        )

        userdata.put(("message", topic, node_id, receiver, physical_sender, timestamp, rssi, snr, msg_type))

        if msg_type == "nodeinfo" and "payload" in payload:
            node_payload = payload["payload"]
            if all(k in node_payload for k in ["id", "longname", "shortname", "hardware", "role"]):
                userdata.put(
                    (
                        "nodeinfo",
                        node_id,  # Use 'from' as primary identifier
                        node_payload["longname"].encode('ascii', 'ignore').decode('ascii').strip(),
                        node_payload["shortname"].encode('ascii', 'ignore').decode('ascii').strip(),
                        node_payload["hardware"],
                        node_payload["role"],
                        timestamp,
                    )
                )
                # print message details in one line
                print(
                    f"Node {node_id}: {timestamp} {node_id} |{node_payload['longname']}|{node_payload['longname'].encode('ascii', 'ignore').decode('ascii').strip()}| {node_payload['shortname']} {node_payload['hardware']} {node_payload['role']}"
                )


        elif msg_type == "neighborinfo" and "payload" in payload:
            neighbor_payload = payload["payload"]
            if "node_id" in neighbor_payload and "neighbors" in neighbor_payload:
                neighbors = neighbor_payload["neighbors"]
                userdata.put(("neighbors", node_id, neighbors, timestamp))

                # print message details in one line
                print(
                    f"Node {node_id}: {timestamp} {node_id} {node_payload['role']}"
                )

        elif msg_type == "position" and "payload" in payload:
            pos_payload = payload["payload"]
            if all(k in pos_payload for k in ["latitude_i", "longitude_i"]):
                latitude = f"{(int(pos_payload['latitude_i']) * 1e-7):.5f}"
                longitude = f"{(int(pos_payload['longitude_i']) * 1e-7):.5f}"
                userdata.put(
                    (
                        "position",
                        node_id,  # Use 'from' as primary identifier
                        None,  # longname
                        None,  # shortname
                        None,  # hardware
                        None,  # role
                        timestamp,
                        float(latitude),
                        float(longitude),
                    )
                )

                # print message details in one line
                print(f"Node {node_id}: {timestamp} {node_id} {latitude} {longitude}")


    except Exception as e:
        print(f"Error processing message: {e}")


# Load configuration
config = load_config()

# Initialize database
db_path = "mqtt_messages.db"
db_conn = init_db(db_path)

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
