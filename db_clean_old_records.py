import sqlite3
import sys
from datetime import datetime, timedelta
import argparse

def get_cutoff_timestamp(days_back):
    """Calculate Unix timestamp for N days ago."""
    cutoff_date = datetime.now() - timedelta(days=days_back)
    return int(cutoff_date.timestamp())

def cleanup_database(db_path, days_back, dry_run=False):
    """
    Clean up old records from the database.
    
    Args:
        db_path (str): Path to the SQLite database
        days_back (int): Remove records older than this many days
        dry_run (bool): If True, only print what would be deleted without actual deletion
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get cutoff timestamp
        cutoff_timestamp = get_cutoff_timestamp(days_back)
        cutoff_date = datetime.fromtimestamp(cutoff_timestamp)
        
        print(f"\nCleaning up records older than {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Cutoff timestamp: {cutoff_timestamp}\n")
        
        # Tables and their timestamp columns
        tables = {
            'messages': 'timestamp',
            'neighbors': 'timestamp',
            'traceroutes': 'timestamp',
            'nodes_count': 'timestamp'
        }
        
        total_deleted = 0
        
        # First, get counts and sample of old records
        for table, timestamp_col in tables.items():
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE {timestamp_col} < ?
            """, (cutoff_timestamp,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                print(f"\nFound {count} old records in {table}")
                
                # Show sample of records to be deleted
                cursor.execute(f"""
                    SELECT * FROM {table}
                    WHERE {timestamp_col} < ?
                    LIMIT 3
                """, (cutoff_timestamp,))
                sample = cursor.fetchall()
                print(f"Sample records to be deleted from {table}:")
                for record in sample:
                    print(f"  {record}")
                
                if not dry_run:
                    # Perform deletion in smaller batches to avoid locking the database for too long
                    batch_size = 10000
                    deleted = 0
                    
                    while True:
                        # First get a batch of IDs to delete
                        cursor.execute(f"""
                            SELECT id FROM {table}
                            WHERE {timestamp_col} < ?
                            ORDER BY id
                            LIMIT {batch_size}
                        """, (cutoff_timestamp,))
                        
                        ids = [row[0] for row in cursor.fetchall()]
                        if not ids:
                            break
                            
                        # Delete this batch
                        id_placeholders = ','.join('?' * len(ids))
                        cursor.execute(f"""
                            DELETE FROM {table}
                            WHERE id IN ({id_placeholders})
                        """, ids)
                        
                        deleted += len(ids)
                        
                        # Commit each batch
                        conn.commit()
                        print(f"  Deleted {deleted} records from {table}...")
                    
                    print(f"Deleted {deleted} records from {table}")
                    total_deleted += deleted
                else:
                    print(f"Would delete {count} records from {table} (dry run)")
                    total_deleted += count
        
        # Update the nodes table last_seen field
        # Note: We don't delete nodes, just update their last_seen status
        if not dry_run:
            cursor.execute("""
                UPDATE nodes
                SET last_seen = NULL
                WHERE last_seen < ?
            """, (cutoff_timestamp,))
            updated_nodes = cursor.rowcount
            print(f"\nUpdated last_seen for {updated_nodes} nodes")
            
            # Final commit
            conn.commit()
            
            # Vacuum the database to reclaim space
            print("\nVacuuming database to reclaim space...")
            cursor.execute("VACUUM")
            conn.commit()
        
        print(f"\nTotal records {'would be ' if dry_run else ''}deleted: {total_deleted}")
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Clean up old records from Meshtastic MQTT database')
    parser.add_argument('--db', default='mqtt_messages.db', help='Database file path')
    parser.add_argument('--days', type=int, default=7, help='Remove records older than this many days')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    
    args = parser.parse_args()
    
    print(f"\nDatabase cleanup starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {args.db}")
    print(f"Days threshold: {args.days}")
    print(f"Dry run: {args.dry_run}")
    
    cleanup_database(args.db, args.days, args.dry_run)

if __name__ == "__main__":
    main()