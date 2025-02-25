import ssl
import json
import time

# import os
import sqlite3
import threading
from queue import Queue
from datetime import datetime
from paho.mqtt.client import Client

from google.protobuf import json_format
from meshtastic.protobuf import mqtt_pb2, mesh_pb2, portnums_pb2, telemetry_pb2

import base64
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def load_config(config_path="config.json"):
    try:
        with open(config_path, "r") as config_file:
            return json.load(config_file)
    except Exception as e:
        print(f"Error loading config file: {e}")
        exit(1)


# --------cryptography ------------------------------------------------------ #


class MeshtasticDecryptor:
    """Class to handle Meshtastic message decryption using AES256-CTR"""

    def __init__(self, channel_key_base64="AQ=="):
        """Initialize with base64 encoded channel key"""
        # Decode the base64 key
        key_bytes = base64.b64decode(channel_key_base64)

        # AES256 requires a 32-byte key
        if len(key_bytes) < 32:
            # Pad the key according to Meshtastic's method
            # It uses a key derivation based on the short channel key
            key_bytes = self._derive_key(key_bytes)

        self.key = key_bytes

    def _derive_key(self, short_key):
        """
        Derive a 32-byte key from a shorter key using Meshtastic's method

        Meshtastic uses a simple key derivation: it repeats the key bytes
        until reaching 32 bytes
        """
        derived_key = bytearray(32)
        for i in range(32):
            derived_key[i] = short_key[i % len(short_key)]
        return bytes(derived_key)

    def decrypt(self, encrypted_data, packet_info):
        """
        Decrypt the encrypted data from a Meshtastic packet using AES256-CTR

        Args:
            encrypted_data (str): Base64 encoded encrypted data
            packet_info (dict): Packet information including 'from', 'to', 'id', etc.

        Returns:
            dict: The decrypted data as a dictionary
        """
        try:
            # Decode the base64 encrypted data
            encrypted_bytes = base64.b64decode(encrypted_data)

            # Generate IV (nonce) from packet info (16 bytes for AES-CTR)
            iv = self._generate_iv(packet_info)

            # Create AES-CTR cipher
            cipher = Cipher(algorithms.AES(self.key), modes.CTR(iv), backend=default_backend())

            # Create decryptor
            decryptor = cipher.decryptor()

            # Decrypt the data
            decrypted_bytes = decryptor.update(encrypted_bytes) + decryptor.finalize()

            # Try to parse the decrypted data
            result = self._parse_decrypted_data(decrypted_bytes)

            # Add debug info
            result["debug"] = {"key_length": len(self.key), "iv": iv.hex(), "encrypted_length": len(encrypted_bytes)}

            return result

        except Exception as e:
            print(f"Decryption error: {e}")
            import traceback

            traceback.print_exc()
            return None

    def _generate_iv(self, packet_info):
        """
        Generate the IV (nonce) for AES-CTR from packet information

        In Meshtastic, the IV is derived from packet fields such as
        from, channel, and id
        """
        # Create a 16-byte IV (AES block size)
        iv = bytearray(16)

        # Extract packet fields
        from_id = packet_info.get("from", 0)
        channel = packet_info.get("channel", 0)
        packet_id = packet_info.get("id", 0)

        # First 4 bytes: from (sender ID) in little-endian
        iv[0:4] = from_id.to_bytes(4, byteorder="little")

        # 5th byte: channel number
        iv[4] = channel & 0xFF

        # Remaining bytes: packet ID in little-endian
        # In practice, Meshtastic only uses 8 bytes for packet ID
        id_bytes = packet_id.to_bytes(8, byteorder="little")
        iv[5:13] = id_bytes

        # Fill remaining bytes with zeros
        iv[13:16] = bytes([0, 0, 0])

        return bytes(iv)

    def _parse_decrypted_data(self, decrypted_bytes):
        """
        Try to parse the decrypted bytes as a Meshtastic protobuf message
        """
        result = {"raw_bytes": decrypted_bytes.hex(), "text": self._try_text_decode(decrypted_bytes)}

        # Try various protobuf message types
        try:
            data = mesh_pb2.Data()
            data.ParseFromString(decrypted_bytes)
            result["data"] = json_format.MessageToDict(data, preserving_proto_field_name=True)
            return result
        except:
            pass

        try:
            user = mesh_pb2.User()
            user.ParseFromString(decrypted_bytes)
            result["user"] = json_format.MessageToDict(user, preserving_proto_field_name=True)
            return result
        except:
            pass

        try:
            position = mesh_pb2.Position()
            position.ParseFromString(decrypted_bytes)
            result["position"] = json_format.MessageToDict(position, preserving_proto_field_name=True)
            return result
        except:
            pass

        try:
            telemetry = telemetry_pb2.Telemetry()
            telemetry.ParseFromString(decrypted_bytes)
            result["telemetry"] = json_format.MessageToDict(telemetry, preserving_proto_field_name=True)
            return result
        except:
            pass

        # Return the raw bytes if no protobuf parsing succeeded
        return result

    def _try_text_decode(self, data_bytes):
        """Try to decode bytes as UTF-8 text"""
        try:
            text = data_bytes.decode("utf-8")
            if all(c.isprintable() or c.isspace() for c in text):
                return text
            return None
        except:
            return None


# Example usage
def decrypt_packet(packet_data, channel_key="AQ=="):
    """
    Decrypt a Meshtastic packet

    Args:
        packet_data (dict): The packet data from MQTT
        channel_key (str): Base64 encoded channel key

    Returns:
        dict: Updated packet data with decrypted information
    """
    decryptor = MeshtasticDecryptor(channel_key)

    # Get the encrypted data from the packet
    if "packet" in packet_data and "encrypted" in packet_data["packet"]:
        packet = packet_data["packet"]
        encrypted = packet["encrypted"]

        print(f"Attempting to decrypt message with key: {channel_key}")
        print(f"Packet ID: {packet.get('id')}, From: {packet.get('from')}, Channel: {packet.get('channel')}")

        # Decrypt
        decrypted = decryptor.decrypt(encrypted, packet)

        # Add the decrypted data to the packet
        if decrypted:
            packet["decrypted"] = decrypted

            # Print decryption results
            print("Decryption successful!")
            if decrypted.get("text"):
                print(f"Decoded text: {decrypted['text']}")
            if "user" in decrypted:
                print(f"User data: {json.dumps(decrypted['user'], indent=2)}")
            if "position" in decrypted:
                print(f"Position data: {json.dumps(decrypted['position'], indent=2)}")
            if "telemetry" in decrypted:
                print(f"Telemetry data: {json.dumps(decrypted['telemetry'], indent=2)}")

    return packet_data


# ----------------------------------------------------------------#


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
                node_id,
            ]

            cursor.execute(
                """UPDATE nodes 
                   SET longname = ?, shortname = ?, hardware = ?, role = ?, 
                       last_seen = ?, latitude = ?, longitude = ?
                   WHERE id = ?""",
                tuple(update_values),
            )
        else:
            # Insert new node
            cursor.execute(
                """INSERT INTO nodes 
                   (id, longname, shortname, hardware, role, last_seen, latitude, longitude)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (node_id, longname, shortname, hardware, role, timestamp, latitude, longitude),
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


class MeshtasticParser:
    """Helper class to parse Meshtastic protobuf messages"""

    MESSAGE_TYPES = {
        "MeshPacket": mesh_pb2.MeshPacket,
        "NodeInfo": mesh_pb2.NodeInfo,
        "Position": mesh_pb2.Position,
        "User": mesh_pb2.User,
        "Telemetry": telemetry_pb2.Telemetry,
        "ServiceEnvelope": mqtt_pb2.ServiceEnvelope,
    }

    # Message type mapping for portnum values
    PORT_TO_TYPE = {
        portnums_pb2.PortNum.NODEINFO_APP: "nodeinfo",
        portnums_pb2.PortNum.POSITION_APP: "position",
        portnums_pb2.PortNum.TEXT_MESSAGE_APP: "text",
        portnums_pb2.PortNum.TELEMETRY_APP: "telemetry",
        portnums_pb2.PortNum.ROUTING_APP: "routing",
        portnums_pb2.PortNum.TRACEROUTE_APP: "traceroute",
    }

    @staticmethod
    def parse_protobuf(raw_bytes, topic):
        """Parse protobuf message bytes into a dictionary"""
        try:
            # First try to parse as ServiceEnvelope
            envelope = mqtt_pb2.ServiceEnvelope()
            envelope.ParseFromString(raw_bytes)

            # Convert to dictionary for easier handling
            result = json_format.MessageToDict(envelope, preserving_proto_field_name=True)

            # Add message type information
            result["packet_type"] = "service_envelope"

            print(f"\nSuccessfully parsed ServiceEnvelope:")
            print(json.dumps(result, indent=2))

            return result

        except Exception as e:
            print(f"Error parsing as ServiceEnvelope: {e}")

            # Try to parse based on topic
            message_type = MeshtasticParser.detect_message_type(topic)
            if message_type not in MeshtasticParser.MESSAGE_TYPES:
                print(f"Unknown message type for topic {topic}")
                return None

            try:
                message = MeshtasticParser.MESSAGE_TYPES[message_type]()
                message.ParseFromString(raw_bytes)
                result = json_format.MessageToDict(message, preserving_proto_field_name=True)
                result["packet_type"] = message_type.lower()

                print(f"\nSuccessfully parsed {message_type}:")
                print(json.dumps(result, indent=2))

                return result
            except Exception as e:
                print(f"Error parsing protobuf message as {message_type}: {e}")
                print(f"Raw bytes: {raw_bytes.hex()}")
                return None

    @staticmethod
    def detect_message_type(topic):
        """Detect message type from MQTT topic"""
        topic_parts = topic.lower().split("/")

        type_mapping = {
            "nodeinfo": "NodeInfo",
            "position": "Position",
            "user": "User",
            "telemetry": "Telemetry",
            "traceroute": "MeshPacket",  # Traceroute uses MeshPacket
        }

        for part in topic_parts:
            if part in type_mapping:
                return type_mapping[part]

        return "MeshPacket"  # Default type

    @staticmethod
    def parse_packet_fields(packet):
        """Extract common fields from a packet"""
        try:
            node_id = packet.get("from")
            receiver = packet.get("to")

            # Try to get message type from port_num
            port_num = packet.get("port_num")
            if port_num is not None:
                try:
                    port_int = int(port_num)
                    msg_type = MeshtasticParser.PORT_TO_TYPE.get(port_int, portnums_pb2.PortNum.Name(port_int))
                except ValueError:
                    msg_type = "unknown"
            else:
                msg_type = packet.get("type", "unknown")

            return node_id, receiver, msg_type
        except Exception as e:
            print(f"Error parsing packet fields: {e}")
            return None, None, "unknown"

    @staticmethod
    def get_type_from_portnum(portnum):
        """Convert a portnum value to a message type string"""
        portnum_map = {
            "NODEINFO_APP": "nodeinfo",
            "POSITION_APP": "position",
            "TEXT_MESSAGE_APP": "text",
            "TELEMETRY_APP": "telemetry",
            "ROUTING_APP": "routing",
            "TRACEROUTE_APP": "traceroute",
        }

        return portnum_map.get(portnum, "unknown")


def handle_nodeinfo(userdata, payload, timestamp, node_id, is_protobuf):
    """Handle nodeinfo messages"""
    try:
        longname = None
        shortname = None
        hardware = None
        role = None

        if is_protobuf:
            packet = payload.get("packet", {})

            # Check for decrypted data first
            if "decrypted" in packet and "user" in packet["decrypted"]:
                user_data = packet["decrypted"]["user"]
                longname = user_data.get("long_name")
                shortname = user_data.get("short_name")
                hardware = user_data.get("hw_model", 0)
                role = 0  # Default role
            # If no decrypted data, check for decoded data
            elif "decoded" in packet and "user" in packet["decoded"]:
                user_data = packet["decoded"]["user"]
                longname = user_data.get("long_name")
                shortname = user_data.get("short_name")
                hardware = user_data.get("hw_model", 0)
                role = 0  # Default role
        else:
            # JSON format
            node_data = payload.get("payload", {})
            longname = node_data.get("longname")
            shortname = node_data.get("shortname")
            hardware = node_data.get("hardware")
            role = node_data.get("role", 0)

        if longname or shortname:  # Allow one to be empty
            print(f"NodeInfo - Long: {longname}, Short: {shortname}, HW: {hardware}")
            userdata.put(
                (
                    "nodeinfo",
                    node_id,
                    sanitize_string(longname) if longname else "",
                    sanitize_string(shortname) if shortname else "",
                    hardware,
                    role,
                    timestamp,
                )
            )
        else:
            print("Missing required fields in nodeinfo message")

    except Exception as e:
        print(f"Error handling nodeinfo: {e}")
        import traceback

        traceback.print_exc()

def handle_traceroute(userdata, payload, timestamp, node_id, is_protobuf):
    """Handle traceroute messages"""
    try:
        route = []
        
        if is_protobuf:
            packet = payload.get('packet', {})
            
            # Check for decrypted data first
            if 'decrypted' in packet:
                decrypted = packet['decrypted']
                
                # Check if we have text data (some traceroutes are comma-separated text)
                if decrypted.get('text'):
                    text_data = decrypted['text']
                    # Parse comma-separated list of nodes
                    if ',' in text_data:
                        route = [item.strip() for item in text_data.split(',')]
                    # Parse space-separated list of nodes
                    elif ' ' in text_data:
                        route = [item.strip() for item in text_data.split()]
                
                # Check if we have data that might be traceroute protobuf
                elif 'raw_bytes' in decrypted:
                    try:
                        # Try to parse using mesh_pb2.RouteDiscovery (if available)
                        raw_bytes = bytes.fromhex(decrypted['raw_bytes'])
                        # Note: This may need to be adjusted based on actual protobuf definition
                        from meshtastic.protobuf import mesh_pb2
                        if hasattr(mesh_pb2, 'RouteDiscovery'):
                            route_discovery = mesh_pb2.RouteDiscovery()
                            route_discovery.ParseFromString(raw_bytes)
                            route = [str(node) for node in route_discovery.route]
                    except Exception as e:
                        print(f"Failed to parse as RouteDiscovery: {e}")
                        # Try alternative parsing if needed
            
            # If no decrypted data or parsing failed, check for decoded data
            if not route and 'decoded' in packet:
                decoded = packet['decoded']
                
                # Some implementations include route directly
                if 'route' in decoded:
                    route_data = decoded['route']
                    if isinstance(route_data, list):
                        route = route_data
                
                # Others might include it in payload
                elif 'payload' in decoded:
                    payload_data = decoded['payload']
                    # Try to decode if it's base64
                    try:
                        # If it's a base64 string
                        if isinstance(payload_data, str):
                            payload_bytes = base64.b64decode(payload_data)
                            # Try to parse traceroute data
                            route = parse_traceroute_payload(payload_bytes)
                    except:
                        pass
        else:
            # JSON format - typically includes a 'route' array in the payload
            route_data = payload.get('payload', {}).get('route', [])
            if isinstance(route_data, list):
                route = route_data
        
        # Process the route if we found one
        if route and len(route) > 1:
            print(f"Traceroute - Route: {' -> '.join(str(node) for node in route)}")
            
            # Process each consecutive pair in the route
            for i in range(len(route) - 1):
                from_node = sanitize_string(str(route[i]))
                to_node = sanitize_string(str(route[i + 1]))
                
                # Skip if either node is "Unknown"
                if "Unknown" not in [from_node, to_node]:
                    # Store in database
                    userdata.put((
                        "traceroute",
                        [from_node, to_node],  # Route pair
                        timestamp
                    ))
        else:
            print("No valid route found in traceroute message")
            
    except Exception as e:
        print(f"Error handling traceroute: {e}")
        import traceback
        traceback.print_exc()

def parse_traceroute_payload(payload_bytes):
    """
    Attempt to parse binary payload data as traceroute information
    
    Args:
        payload_bytes: Raw binary payload
        
    Returns:
        list: List of node IDs in the route
    """
    try:
        # First try if it's a text representation
        try:
            text = payload_bytes.decode('utf-8')
            if ',' in text:
                return [item.strip() for item in text.split(',')]
            elif ' ' in text:
                return [item.strip() for item in text.split()]
        except:
            pass
            
        # If not text, try various binary formats
        # This is a simplified approach and might need adjustments
        # based on actual Meshtastic traceroute format
        route = []
        
        # Look for 4-byte node IDs in the payload
        # (This assumes a simple format where node IDs are packed sequentially)
        if len(payload_bytes) % 4 == 0:
            for i in range(0, len(payload_bytes), 4):
                node_id = int.from_bytes(payload_bytes[i:i+4], byteorder='little')
                if node_id != 0:  # Skip zero IDs
                    route.append(f"!{node_id:x}")  # Format as hex
        
        return route
            
    except Exception as e:
        print(f"Error parsing traceroute payload: {e}")
        return []
    
def handle_position(userdata, payload, timestamp, node_id, is_protobuf):
    """Handle position messages"""
    try:
        latitude = None
        longitude = None

        if is_protobuf:
            packet = payload.get("packet", {})

            # Check for decrypted data first
            if "decrypted" in packet and "position" in packet["decrypted"]:
                pos_data = packet["decrypted"]["position"]
                latitude = float(pos_data.get("latitude_i", 0)) * 1e-7
                longitude = float(pos_data.get("longitude_i", 0)) * 1e-7
            # If no decrypted data, check for decoded data
            elif "decoded" in packet and "position" in packet["decoded"]:
                pos_data = packet["decoded"]["position"]
                latitude = float(pos_data.get("latitude_i", 0)) * 1e-7
                longitude = float(pos_data.get("longitude_i", 0)) * 1e-7
            # If no position object, check for payload that might contain position data
            elif "decoded" in packet and "payload" in packet["decoded"]:
                try:
                    # Try to decode base64 payload
                    pos_payload = base64.b64decode(packet["decoded"]["payload"])
                    # Try to parse as position protobuf
                    position = mesh_pb2.Position()
                    position.ParseFromString(pos_payload)
                    latitude = float(position.latitude_i) * 1e-7
                    longitude = float(position.longitude_i) * 1e-7
                except:
                    pass
        else:
            # JSON format
            pos_data = payload.get("payload", {})
            latitude = float(pos_data.get("latitude_i", 0)) * 1e-7
            longitude = float(pos_data.get("longitude_i", 0)) * 1e-7

        if latitude is not None and longitude is not None:
            print(f"Position - Lat: {latitude}, Lon: {longitude}")
            userdata.put(
                (
                    "position",
                    node_id,
                    None,  # longname
                    None,  # shortname
                    None,  # hardware
                    None,  # role
                    timestamp,
                    latitude,
                    longitude,
                )
            )
        else:
            print("Missing latitude/longitude in position message")

    except Exception as e:
        print(f"Error handling position: {e}")
        import traceback

        traceback.print_exc()


def handle_text(userdata, payload, timestamp, node_id, is_protobuf):
    """Handle text messages"""
    try:
        text = None

        if is_protobuf:
            packet = payload.get("packet", {})

            # Check for decrypted text
            if "decrypted" in packet and packet["decrypted"].get("text"):
                text = packet["decrypted"]["text"]
            # If no decrypted text, check for decoded data
            elif "decoded" in packet and "text" in packet["decoded"]:
                text = packet["decoded"]["text"]
        else:
            # JSON format
            text = payload.get("payload", {}).get("text")

        if text:
            print(f"Text message: {text}")
            # Add your text message handling code here - you might want
            # to store these in a separate table

    except Exception as e:
        print(f"Error handling text message: {e}")
        import traceback

        traceback.print_exc()


def handle_telemetry(userdata, payload, timestamp, node_id, is_protobuf):
    """Handle telemetry messages"""
    try:
        telemetry_data = None

        if is_protobuf:
            packet = payload.get("packet", {})

            # Check for decrypted telemetry
            if "decrypted" in packet and "telemetry" in packet["decrypted"]:
                telemetry_data = packet["decrypted"]["telemetry"]
            # If no decrypted telemetry, check for decoded data
            elif "decoded" in packet and "telemetry" in packet["decoded"]:
                telemetry_data = packet["decoded"]["telemetry"]
        else:
            # JSON format
            telemetry_data = payload.get("payload", {})

        if telemetry_data:
            print(f"Telemetry data: {json.dumps(telemetry_data, indent=2)}")
            # Add your telemetry handling code here

    except Exception as e:
        print(f"Error handling telemetry: {e}")
        import traceback

        traceback.print_exc()


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        timestamp = int(time.time())
        
        # First try to parse as JSON
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            is_protobuf = False
            print(f"\nParsed JSON message on {topic}")
        except: # json.JSONDecodeError:
            # If JSON parsing fails, try protobuf
            payload = MeshtasticParser.parse_protobuf(msg.payload, topic)
            if payload is None:
                print(f"Failed to parse message as either JSON or Protobuf on topic {topic}")
                return
            is_protobuf = True

        # If we have a protobuf message with encryption, try to decrypt it
        if is_protobuf and payload.get('packet_type') == 'service_envelope':
            packet = payload.get('packet', {})
            
            # Check if the packet has encrypted data
            if 'encrypted' in packet:
                # Extract channel_id for potential channel-specific keys
                channel_id = payload.get('channel_id', 'default')
                
                # Get channel key (could be channel-specific in a more complete implementation)
                channel_key = config.get('CHANNEL_KEYS', {}).get(channel_id, "AQ==")
                
                # Attempt decryption
                decrypted_packet = decrypt_packet(payload, channel_key)
                
                # Update the payload with decrypted data
                payload = decrypted_packet
                
                # If we have decrypted data, check its type and update message type
                decrypted_data = packet.get('decrypted', {})
                msg_type = None
                
                if 'user' in decrypted_data:
                    msg_type = "nodeinfo"
                elif 'position' in decrypted_data:
                    msg_type = "position"
                elif 'telemetry' in decrypted_data:
                    msg_type = "telemetry"
                elif 'text' in decrypted_data and decrypted_data['text']:
                    msg_type = "text"
                
                if msg_type:
                    print(f"Detected message type from decrypted data: {msg_type}")
        
        # Extract fields from the payload
        if is_protobuf:
            # Extract fields from ServiceEnvelope if present
            if payload.get('packet_type') == 'service_envelope':
                packet = payload.get('packet', {})
                
                # Get node_id and receiver
                node_id = packet.get('from')
                receiver = packet.get('to')
                
                # Try to determine message type
                # First check if we detected it from decrypted data
                if 'decrypted' in packet:
                    decrypted = packet['decrypted']
                    
                    if 'user' in decrypted:
                        msg_type = "nodeinfo"
                    elif 'position' in decrypted:
                        msg_type = "position"
                    elif 'telemetry' in decrypted:
                        msg_type = "telemetry"
                    elif decrypted.get('text'):
                        msg_type = "text"
                    else:
                        # Fall back to decoded portnum if available
                        if 'decoded' in packet and 'portnum' in packet['decoded']:
                            portnum = packet['decoded']['portnum']
                            msg_type = MeshtasticParser.get_type_from_portnum(portnum)
                        else:
                            msg_type = "unknown"
                else:
                    # No decryption, try to get from decoded field
                    if 'decoded' in packet and 'portnum' in packet['decoded']:
                        portnum = packet['decoded']['portnum']
                        msg_type = MeshtasticParser.get_type_from_portnum(portnum)
                    else:
                        msg_type = "unknown"
            else:
                node_id, receiver, msg_type = MeshtasticParser.parse_packet_fields(payload)
        else:
            # Extract fields from JSON
            node_id = payload.get("from")
            receiver = payload.get("to")
            msg_type = payload.get("type")

        print(f"Processing message:")
        print(f"Format: {'protobuf' if is_protobuf else 'json'}")
        print(f"From: {node_id}")
        print(f"To: {receiver}")
        print(f"Type: {msg_type}")

        # Store basic message info in database
        if node_id is not None:
            userdata.put((
                "message",
                topic,
                node_id,
                receiver,
                node_id,  # physical_sender same as from for now
                timestamp,
                payload.get('rssi'),
                payload.get('snr'),
                msg_type
            ))

        # Handle specific message types
        if msg_type == "nodeinfo":
            handle_nodeinfo(userdata, payload, timestamp, node_id, is_protobuf)
        elif msg_type == "position":
            handle_position(userdata, payload, timestamp, node_id, is_protobuf)
        elif msg_type == "telemetry":
            handle_telemetry(userdata, payload, timestamp, node_id, is_protobuf)
        elif msg_type == "text":
            handle_text(userdata, payload, timestamp, node_id, is_protobuf)
        elif msg_type == "traceroute":
            handle_traceroute(userdata, payload, timestamp, node_id, is_protobuf)

    except Exception as e:
        print(f"Error processing message: {e}")
        import traceback
        traceback.print_exc()

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
