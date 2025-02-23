import sqlite3
from datetime import datetime, timedelta
import sys
import os

def init_distilled_db(db_path):
    """Initialize the distilled database with required tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Hourly message counts by type
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_message_counts (
            hour TEXT,           -- Format: YYYY-MM-DD HH:00
            message_type TEXT,
            count INTEGER,
            PRIMARY KEY (hour, message_type)
        )
    """)
    
    # Daily message counts by type
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_message_counts (
            date TEXT,           -- Format: YYYY-MM-DD
            message_type TEXT,
            count INTEGER,
            PRIMARY KEY (date, message_type)
        )
    """)
    
    # Hourly unique senders
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_unique_senders (
            hour TEXT,           -- Format: YYYY-MM-DD HH:00
            unique_senders INTEGER,
            unique_physical_senders INTEGER,
            PRIMARY KEY (hour)
        )
    """)
    
    # Daily unique senders
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_unique_senders (
            date TEXT,           -- Format: YYYY-MM-DD
            unique_senders INTEGER,
            unique_physical_senders INTEGER,
            PRIMARY KEY (date)
        )
    """)
    
    conn.commit()
    return conn

def get_hour_bucket(timestamp):
    """Convert Unix timestamp to hour bucket string YYYY-MM-DD HH:00."""
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime('%Y-%m-%d %H:00')

def get_date_bucket(timestamp):
    """Convert Unix timestamp to date bucket string YYYY-MM-DD."""
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime('%Y-%m-%d')

def get_bucket_timestamps(bucket_start):
    """Convert hour string to Unix timestamp range."""
    dt = datetime.strptime(bucket_start, '%Y-%m-%d %H:00')
    start_ts = int(dt.timestamp())
    end_ts = start_ts + 3600
    return start_ts, end_ts

def process_hour(source_cursor, dest_conn, hour_timestamp):
    """Process one hour of data and store aggregated statistics."""
    dest_cursor = dest_conn.cursor()
    
    # Convert Unix timestamp to hour and date strings
    hour_str = get_hour_bucket(hour_timestamp)
    date_str = get_date_bucket(hour_timestamp)
    
    # Get timestamp range for the hour
    start_ts, end_ts = get_bucket_timestamps(hour_str)
    
    print(f"Processing hour: {hour_str}")
    
    # Get message counts by type for this hour, ignoring pre-2020 messages
    min_valid_ts = get_min_valid_timestamp()
    source_cursor.execute("""
        SELECT 
            type,
            COUNT(*) as message_count
        FROM messages 
        WHERE timestamp >= ? AND timestamp < ? 
        AND timestamp >= ?
        GROUP BY type
    """, (start_ts, end_ts, min_valid_ts))
    
    # Store hourly message counts
    for msg_type, count in source_cursor.fetchall():
        if not msg_type:  # Skip if message type is None or empty
            continue
            
        # Store hourly counts
        dest_cursor.execute("""
            INSERT OR REPLACE INTO hourly_message_counts 
            (hour, message_type, count)
            VALUES (?, ?, ?)
        """, (hour_str, msg_type, count))
        
        # Update daily counts
        dest_cursor.execute("""
            INSERT INTO daily_message_counts 
            (date, message_type, count)
            VALUES (?, ?, ?)
            ON CONFLICT(date, message_type) 
            DO UPDATE SET count = count + excluded.count
        """, (date_str, msg_type, count))
    
    # Get unique sender counts for this hour, ignoring pre-2025 messages
    source_cursor.execute("""
        SELECT 
            COUNT(DISTINCT sender) as unique_senders,
            COUNT(DISTINCT physical_sender) as unique_physical_senders
        FROM messages 
        WHERE timestamp >= ? AND timestamp < ?
        AND timestamp >= ?
    """, (start_ts, end_ts, min_valid_ts))
    
    unique_senders, unique_physical_senders = source_cursor.fetchone()
    
    # Store hourly unique senders
    dest_cursor.execute("""
        INSERT OR REPLACE INTO hourly_unique_senders 
        (hour, unique_senders, unique_physical_senders)
        VALUES (?, ?, ?)
    """, (hour_str, unique_senders, unique_physical_senders))
    
    # Update daily unique senders
    dest_cursor.execute("""
        INSERT INTO daily_unique_senders 
        (date, unique_senders, unique_physical_senders)
        VALUES (?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
        unique_senders = MAX(unique_senders, excluded.unique_senders),
        unique_physical_senders = MAX(unique_physical_senders, excluded.unique_physical_senders)
    """, (date_str, unique_senders, unique_physical_senders))
    
    dest_conn.commit()

def get_min_valid_timestamp():
    """Return Unix timestamp for start of 2025."""
    return int(datetime(2025, 1, 1).timestamp())

def process_data(source_path, dest_path, hours_back=2):
    """
    Process data and update statistics.
    If destination database doesn't exist, process all historical data since 2020.
    Otherwise, process only recent hours specified by hours_back.
    """
    try:
        # Connect to source database
        source_conn = sqlite3.connect(source_path)
        source_cursor = source_conn.cursor()
        
        # Check if destination database exists
        db_exists = os.path.exists(dest_path)
        
        # Initialize or connect to destination database
        dest_conn = init_distilled_db(dest_path)
        
        # Calculate current hour boundary
        current_time = int(datetime.now().timestamp())
        current_hour = current_time - (current_time % 3600)  # Round to start of current hour
        
        if not db_exists:
            # Process all historical data since 2020
            print("Destination database not found. Processing all historical data since 2020...")
            min_valid_ts = get_min_valid_timestamp()
            source_cursor.execute("SELECT MIN(timestamp) FROM messages WHERE timestamp >= ?", (min_valid_ts,))
            min_timestamp = source_cursor.fetchone()[0]
            
            if min_timestamp is None:
                print("No data found in source database.")
                return
                
            # Round down to the start of the hour
            start_time = min_timestamp - (min_timestamp % 3600)
            print(f"Starting from: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:00')}")
        else:
            # Process recent data only
            start_time = current_hour - (hours_back * 3600)
            print("Processing recent data only...")
        
        # Process each hour
        total_hours = (current_hour - start_time) // 3600
        processed_hours = 0
        
        for hour_start in range(start_time, current_hour + 3600, 3600):
            process_hour(source_cursor, dest_conn, hour_start)
            
            processed_hours += 1
            if total_hours > 0:
                progress = (processed_hours / total_hours) * 100
                print(f"Progress: {progress:.1f}% ({processed_hours}/{total_hours} hours)")
        
        print("Successfully updated distilled statistics")
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'source_conn' in locals():
            source_conn.close()
        if 'dest_conn' in locals():
            dest_conn.close()

if __name__ == "__main__":
    source_db = "mqtt_messages.db"
    dest_db = "mqtt_messages_distilled.db"
    hours_back = 2  # Process last 2 hours to ensure we catch any late-arriving data
    
    process_data(source_db, dest_db, hours_back)