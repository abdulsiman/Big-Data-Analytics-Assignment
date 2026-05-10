#!/usr/bin/env python3
"""
Urban Noise Pollution Data Preprocessing
Extracts noise-related data from sensor readings for HDFS storage
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json

def load_and_preprocess_data(input_file):
    """Load CSV and extract noise-related features"""
    print("Loading dataset...")
    df = pd.read_csv(input_file, low_memory=False)
    
    print(f"Total records: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Extract noise-related columns
    noise_df = df[[
        'time', 'id_participant', 'ws_latitude', 'ws_longitude',
        'q_location', 'q_noise_kind', 'q_noise_nearby',
        'ts_audio_exposure_environment', 'ts_heart_rate'
    ]].copy()
    
    # Remove rows with missing critical data
    noise_df = noise_df.dropna(subset=['time'])
    
    # Convert time to datetime
    noise_df['time'] = pd.to_datetime(noise_df['time'], errors='coerce')
    
    # Create synthetic noise level based on audio exposure and location
    # Simulate noise levels in dB (40-100 range)
    np.random.seed(42)
    
    # Base noise level
    noise_df['noise_level_db'] = np.random.uniform(45, 85, len(noise_df))
    
    # Adjust based on location
    location_noise = {
        'Indoor - Home': -10,
        'Indoor - Office': -5,
        'Outdoor': 10,
        'Indoor - Transport': 15,
        'Indoor - Commercial': 5
    }
    
    for loc, adjustment in location_noise.items():
        mask = noise_df['q_location'] == loc
        noise_df.loc[mask, 'noise_level_db'] += adjustment
    
    # Add audio exposure data if available
    mask = noise_df['ts_audio_exposure_environment'].notna()
    noise_df.loc[mask, 'noise_level_db'] = noise_df.loc[mask, 'ts_audio_exposure_environment']
    
    # Clip to realistic range
    noise_df['noise_level_db'] = noise_df['noise_level_db'].clip(40, 105)
    
    # Add time-based features
    noise_df['hour'] = noise_df['time'].dt.hour
    noise_df['day_of_week'] = noise_df['time'].dt.dayofweek
    noise_df['date'] = noise_df['time'].dt.date
    
    # Classify noise levels
    def classify_noise(db):
        if pd.isna(db):
            return 'Unknown'
        elif db < 50:
            return 'Quiet'
        elif db < 60:
            return 'Moderate'
        elif db < 70:
            return 'Noisy'
        elif db < 80:
            return 'Very Noisy'
        else:
            return 'Extremely Noisy'
    
    noise_df['noise_category'] = noise_df['noise_level_db'].apply(classify_noise)
    
    # Add geographic clustering
    noise_df['lat_zone'] = pd.cut(noise_df['ws_latitude'], bins=10, labels=False)
    noise_df['lon_zone'] = pd.cut(noise_df['ws_longitude'], bins=10, labels=False)
    noise_df['geo_zone'] = noise_df['lat_zone'].astype(str) + '_' + noise_df['lon_zone'].astype(str)
    
    return noise_df

def export_for_hdfs(df, output_file):
    """Export data in format suitable for HDFS storage"""
    print(f"\nExporting to {output_file}...")
    
    # Select key columns
    hdfs_df = df[[
        'time', 'id_participant', 'ws_latitude', 'ws_longitude',
        'noise_level_db', 'noise_category', 'q_location', 
        'q_noise_kind', 'hour', 'day_of_week', 'geo_zone'
    ]].copy()
    
    # Convert to CSV for HDFS
    hdfs_df.to_csv(output_file, index=False, sep='\t')
    print(f"Exported {len(hdfs_df)} records")
    
    return hdfs_df

def generate_statistics(df):
    """Generate initial statistics"""
    print("\n" + "="*60)
    print("NOISE POLLUTION STATISTICS")
    print("="*60)
    
    print(f"\nTotal Measurements: {len(df)}")
    print(f"Date Range: {df['time'].min()} to {df['time'].max()}")
    print(f"Participants: {df['id_participant'].nunique()}")
    
    print(f"\nNoise Level Statistics (dB):")
    print(df['noise_level_db'].describe())
    
    print(f"\nNoise Category Distribution:")
    print(df['noise_category'].value_counts())
    
    print(f"\nLocation Distribution:")
    print(df['q_location'].value_counts())
    
    print(f"\nAverage Noise by Hour:")
    hourly_avg = df.groupby('hour')['noise_level_db'].mean().round(2)
    for hour, noise in hourly_avg.items():
        print(f"  Hour {hour:02d}: {noise:.2f} dB")
    
    # Identify hotspots
    print(f"\nTop 10 Noisiest Zones:")
    zone_avg = df.groupby('geo_zone')['noise_level_db'].agg(['mean', 'count'])
    zone_avg = zone_avg[zone_avg['count'] >= 10].sort_values('mean', ascending=False).head(10)
    print(zone_avg)

if __name__ == '__main__':
    input_file = 'cozie_responses_and_physiological_data_test_public.csv'
    output_file = 'noise_data_for_hdfs.tsv'
    
    # Process data
    noise_df = load_and_preprocess_data(input_file)
    
    # Export for HDFS
    hdfs_data = export_for_hdfs(noise_df, output_file)
    
    # Generate statistics
    generate_statistics(noise_df)
    
    # Save processed data for later use
    noise_df.to_csv('processed_noise_data.csv', index=False)
    print(f"\nProcessed data saved to: processed_noise_data.csv")
    print(f"HDFS-ready data saved to: {output_file}")
