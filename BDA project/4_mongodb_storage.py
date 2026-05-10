
#!/usr/bin/env python3
"""
MongoDB Data Storage for Noise Pollution Analysis (Simulated)
Stores structured noise data locally using JSON and SQLite
"""

import pandas as pd
import json
import sqlite3
from datetime import datetime
import sys

class NoiseDataMongoDB:
    """Simulated MongoDB handler for noise pollution data"""
    
    def __init__(self, host='localhost', port=27017, db_name='noise_pollution'):
        """Initialize local database connection"""
        self.db_name = db_name
        self.db_file = f'{db_name}.db'
        self.json_export = f'{db_name}_export.jsonl'
        
        # Create SQLite connection
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()
        
        print(f"Initialized local database: {self.db_file}")
        
    def create_collections_and_indexes(self):
        """Create collections with appropriate indexes"""
        print("Creating collections and indexes...")
        
        # Collection 1: Raw sensor readings
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                participant_id TEXT,
                latitude REAL,
                longitude REAL,
                noise_level_db REAL,
                noise_category TEXT,
                location_type TEXT,
                noise_kind TEXT,
                hour INTEGER,
                day_of_week INTEGER,
                geo_zone TEXT
            )
        ''')
        
        # Create indexes
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON sensor_readings(timestamp)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_geo_zone ON sensor_readings(geo_zone)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_noise_level ON sensor_readings(noise_level_db DESC)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_geo_time ON sensor_readings(geo_zone, timestamp)')
        
        print("  ✓ sensor_readings collection created with indexes")
        
        # Collection 2: Aggregated statistics by zone
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS zone_statistics (
                geo_zone TEXT PRIMARY KEY,
                avg_noise_db REAL,
                max_noise_db REAL,
                min_noise_db REAL,
                std_dev_db REAL,
                sample_count INTEGER,
                last_updated TEXT
            )
        ''')
        
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_zone_avg ON zone_statistics(avg_noise_db DESC)')
        print("  ✓ zone_statistics collection created with indexes")
        
        # Collection 3: Hourly trends
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS hourly_trends (
                hour INTEGER,
                avg_noise_db REAL,
                sample_count INTEGER,
                max_noise_db REAL,
                PRIMARY KEY (hour)
            )
        ''')
        
        print("  ✓ hourly_trends collection created")
        
        self.conn.commit()
        
    def import_sensor_data(self, csv_file, batch_size=1000):
        """Import sensor readings from CSV"""
        print(f"\nImporting sensor data from {csv_file}...")
        
        df = pd.read_csv(csv_file, low_memory=False)
        
        # Prepare documents
        records = []
        
        for idx, row in df.iterrows():
            if pd.notna(row.get('noise_level_db')) and pd.notna(row.get('ws_latitude')) and pd.notna(row.get('ws_longitude')):
                record = (
                    str(row.get('time', '')),
                    str(row.get('id_participant', '')),
                    float(row.get('ws_latitude', 0)),
                    float(row.get('ws_longitude', 0)),
                    float(row.get('noise_level_db', 0)),
                    str(row.get('noise_category', 'Unknown')),
                    str(row.get('q_location', '')),
                    str(row.get('q_noise_kind', '')),
                    int(row.get('hour', 0)),
                    int(row.get('day_of_week', 0)),
                    str(row.get('geo_zone', ''))
                )
                records.append(record)
                
                # Insert in batches
                if len(records) >= batch_size:
                    self.cursor.executemany('''
                        INSERT INTO sensor_readings 
                        (timestamp, participant_id, latitude, longitude, noise_level_db, 
                         noise_category, location_type, noise_kind, hour, day_of_week, geo_zone)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', records)
                    self.conn.commit()
                    print(f"  Inserted {len(records)} records (Total: {idx + 1})")
                    records = []
        
        # Insert remaining documents
        if records:
            self.cursor.executemany('''
                INSERT INTO sensor_readings 
                (timestamp, participant_id, latitude, longitude, noise_level_db, 
                 noise_category, location_type, noise_kind, hour, day_of_week, geo_zone)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', records)
            self.conn.commit()
            print(f"  Inserted final {len(records)} records")
        
        total_count = self.cursor.execute('SELECT COUNT(*) FROM sensor_readings').fetchone()[0]
        print(f"\n  ✓ Total records in sensor_readings: {total_count}")
        
    def compute_zone_statistics(self):
        """Compute and store aggregated statistics by zone"""
        print("\nComputing zone statistics...")
        
        zones = self.cursor.execute('''
            SELECT DISTINCT geo_zone FROM sensor_readings WHERE geo_zone IS NOT NULL
        ''').fetchall()
        
        stats_records = []
        for (zone,) in zones:
            stats = self.cursor.execute('''
                SELECT 
                    AVG(noise_level_db) as avg_noise,
                    MAX(noise_level_db) as max_noise,
                    MIN(noise_level_db) as min_noise,
                    COUNT(*) as sample_count
                FROM sensor_readings
                WHERE geo_zone = ?
            ''', (zone,)).fetchone()
            
            if stats:
                avg_noise, max_noise, min_noise, count = stats
                
                # Calculate standard deviation
                std_dev = self.cursor.execute('''
                    SELECT 
                        CASE 
                            WHEN COUNT(*) <= 1 THEN 0
                            ELSE SQRT(SUM((noise_level_db - ?) * (noise_level_db - ?)) / (COUNT(*) - 1))
                        END as std_dev
                    FROM sensor_readings
                    WHERE geo_zone = ?
                ''', (avg_noise, avg_noise, zone)).fetchone()[0]
                
                stats_records.append((
                    zone,
                    round(avg_noise, 2) if avg_noise else None,
                    round(max_noise, 2) if max_noise else None,
                    round(min_noise, 2) if min_noise else None,
                    round(std_dev, 2) if std_dev else None,
                    count,
                    datetime.now().isoformat()
                ))
        
        # Insert or update zone statistics
        self.cursor.executemany('''
            INSERT OR REPLACE INTO zone_statistics
            (geo_zone, avg_noise_db, max_noise_db, min_noise_db, std_dev_db, sample_count, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', stats_records)
        
        self.conn.commit()
        print(f"  ✓ Computed statistics for {len(stats_records)} zones")
        
    def compute_hourly_patterns(self):
        """Compute hourly noise trends"""
        print("\nComputing hourly trends...")
        
        hourly_data = self.cursor.execute('''
            SELECT 
                hour,
                AVG(noise_level_db) as avg_noise,
                MAX(noise_level_db) as max_noise,
                COUNT(*) as sample_count
            FROM sensor_readings
            WHERE hour IS NOT NULL
            GROUP BY hour
            ORDER BY hour
        ''').fetchall()
        
        records = [(h, round(avg, 2), max_n, cnt) for h, avg, max_n, cnt in hourly_data]
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO hourly_trends (hour, avg_noise_db, max_noise_db, sample_count)
            VALUES (?, ?, ?, ?)
        ''', records)
        
        self.conn.commit()
        print(f"  ✓ Computed hourly trends for {len(records)} hours")
        
    def identify_hotspots(self, threshold=75.0):
        """Identify noise pollution hotspots"""
        print(f"\nIdentifying noise hotspots (threshold: {threshold} dB)...")
        
        zones = self.cursor.execute('''
            SELECT geo_zone, avg_noise_db, sample_count FROM zone_statistics
            WHERE avg_noise_db >= ? AND sample_count >= 10
            ORDER BY avg_noise_db DESC
        ''', (threshold,)).fetchall()
        
        print(f"  ✓ Identified {len(zones)} noise hotspots above {threshold} dB")
        
    def export_to_json(self):
        """Export all data to JSONL format"""
        print(f"\nExporting data to {self.json_export}...")
        
        sensor_data = self.cursor.execute('SELECT * FROM sensor_readings').fetchall()
        column_names = [description[0] for description in self.cursor.description]
        
        with open(self.json_export, 'w') as f:
            for row in sensor_data:
                record = dict(zip(column_names, row))
                f.write(json.dumps(record) + '\n')
        
        print(f"  ✓ Exported {len(sensor_data)} records to {self.json_export}")
        
    def close(self):
        """Close database connection"""
        self.conn.close()
        print("\nDatabase connection closed")

def main():
    """Main execution"""
    print("="*60)
    print("MONGODB NOISE POLLUTION DATA STORAGE (Simulated)")
    print("="*60)
    
    # Load processed data
    try:
        df = pd.read_csv('processed_noise_data.csv')
        print(f"\nLoaded {len(df)} records from processed_noise_data.csv")
    except FileNotFoundError:
        print("ERROR: processed_noise_data.csv not found")
        print("Please run 1_data_preprocessing.py first")
        sys.exit(1)
    
    # Initialize database
    db = NoiseDataMongoDB(db_name='noise_pollution')
    
    try:
        # Create collections and indexes
        db.create_collections_and_indexes()
        
        # Import sensor readings
        db.import_sensor_data('processed_noise_data.csv')
        
        # Compute aggregations
        db.compute_zone_statistics()
        db.compute_hourly_patterns()
        
        # Identify hotspots
        db.identify_hotspots(threshold=75.0)
        
        # Export to JSON
        db.export_to_json()
        
        # Display statistics
        print("\n" + "="*60)
        print("DATABASE STATISTICS")
        print("="*60)
        
        stats = db.cursor.execute('''
            SELECT 
                COUNT(*) as total,
                AVG(noise_level_db) as avg,
                MIN(noise_level_db) as min_val,
                MAX(noise_level_db) as max_val,
                COUNT(DISTINCT participant_id) as participants,
                COUNT(DISTINCT geo_zone) as zones
            FROM sensor_readings
        ''').fetchone()
        
        total, avg, min_n, max_n, participants, zones = stats
        
        print(f"\nTotal Records: {total}")
        print(f"Average Noise: {avg:.2f} dB")
        print(f"Min Noise: {min_n:.2f} dB")
        print(f"Max Noise: {max_n:.2f} dB")
        print(f"Total Participants: {participants}")
        print(f"Total Geographic Zones: {zones}")
        
        # Show top noisy zones
        print("\n" + "-"*60)
        print("TOP 10 NOISIEST ZONES")
        print("-"*60)
        
        top_zones = db.cursor.execute('''
            SELECT geo_zone, avg_noise_db, max_noise_db, sample_count 
            FROM zone_statistics 
            ORDER BY avg_noise_db DESC 
            LIMIT 10
        ''').fetchall()
        
        for zone, avg_noise, max_noise, count in top_zones:
            print(f"{zone:12} | Avg: {avg_noise:6.2f} dB | Max: {max_noise:6.2f} dB | Count: {count:6}")
        
        # Show hourly statistics
        print("\n" + "-"*60)
        print("HOURLY NOISE STATISTICS")
        print("-"*60)
        
        hourly = db.cursor.execute('''
            SELECT hour, avg_noise_db, max_noise_db, sample_count 
            FROM hourly_trends 
            ORDER BY hour
        ''').fetchall()
        
        for hour, avg_noise, max_noise, count in hourly:
            print(f"Hour {hour:02d} | Avg: {avg_noise:6.2f} dB | Max: {max_noise:6.2f} dB | Count: {count:6}")
        
        print("\n" + "="*60)
        print("✓ DATA STORAGE COMPLETE")
        print("="*60)
        print(f"\nFiles created:")
        print(f"  - {db.db_file} (SQLite database)")
        print(f"  - {db.json_export} (JSON export)")
        
    finally:
        db.close()

if __name__ == '__main__':
    main()
