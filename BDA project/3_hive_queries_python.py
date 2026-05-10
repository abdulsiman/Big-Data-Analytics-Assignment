#!/usr/bin/env python3
"""
HIVE QUERIES FOR NOISE POLLUTION TREND ANALYSIS (Python/Pandas Implementation)
Simulates Hive queries using processed noise data
"""

import pandas as pd
import numpy as np
from collections import defaultdict

# Load the processed noise data
print("Loading processed noise data...")
df = pd.read_csv('processed_noise_data.csv')

# Convert time to datetime
df['time'] = pd.to_datetime(df['time'], errors='coerce')

print(f"Loaded {len(df)} records\n")

# ============================================================
# QUERY 3: CITYWIDE NOISE TRENDS BY HOUR
# ============================================================
print("="*60)
print("QUERY 3: CITYWIDE NOISE TRENDS BY HOUR")
print("="*60)

query3 = df[df['noise_level_db'].notna()].groupby('hour').agg({
    'noise_level_db': ['count', 'mean', 'std', 'min', 'max']
}).round(2)

query3.columns = ['measurement_count', 'avg_noise_db', 'std_dev_db', 'min_noise_db', 'max_noise_db']
query3 = query3.reset_index()

# Add percentiles
for idx, row in query3.iterrows():
    hour_data = df[df['hour'] == row['hour']]['noise_level_db']
    query3.loc[idx, 'median_noise_db'] = round(hour_data.quantile(0.5), 2)
    query3.loc[idx, 'p95_noise_db'] = round(hour_data.quantile(0.95), 2)

print(query3.to_string())
query3.to_csv('query3_hourly_trends.csv', index=False)
print("\nSaved to: query3_hourly_trends.csv\n")

# ============================================================
# QUERY 4: NOISE HOTSPOT IDENTIFICATION
# ============================================================
print("="*60)
print("QUERY 4: NOISE HOTSPOT IDENTIFICATION")
print("="*60)

query4 = df[df['geo_zone'].notna()].groupby('geo_zone').agg({
    'noise_level_db': ['count', 'mean', 'max']
}).round(2)

query4.columns = ['measurement_count', 'avg_noise_db', 'max_noise_db']
query4 = query4.reset_index()

# Add extreme noise count and percentage
query4['extreme_noise_count'] = df[df['geo_zone'].notna()].groupby('geo_zone').apply(
    lambda x: (x['noise_level_db'] > 80).sum()
).values

query4['extreme_noise_pct'] = (query4['extreme_noise_count'] / query4['measurement_count'] * 100).round(2)

# Filter for minimum sample size
query4 = query4[query4['measurement_count'] >= 10].sort_values('avg_noise_db', ascending=False).head(20)

print(query4.to_string())
query4.to_csv('query4_noise_hotspots.csv', index=False)
print("\nSaved to: query4_noise_hotspots.csv\n")

# ============================================================
# QUERY 5: WEEKDAY VS WEEKEND COMPARISON
# ============================================================
print("="*60)
print("QUERY 5: WEEKDAY VS WEEKEND COMPARISON")
print("="*60)

df['period_type'] = df['day_of_week'].apply(lambda x: 'Weekend' if x in [5, 6] else 'Weekday')

query5 = df[df['noise_level_db'].notna()].groupby('period_type').agg({
    'noise_level_db': ['count', 'mean', 'std', 'min', 'max']
}).round(2)

query5.columns = ['measurement_count', 'avg_noise_db', 'std_dev_db', 'min_noise_db', 'max_noise_db']
query5 = query5.reset_index()

print(query5.to_string())
query5.to_csv('query5_weekday_weekend.csv', index=False)
print("\nSaved to: query5_weekday_weekend.csv\n")

# ============================================================
# QUERY 6: LOCATION TYPE ANALYSIS
# ============================================================
print("="*60)
print("QUERY 6: LOCATION TYPE ANALYSIS")
print("="*60)

query6 = df[(df['q_location'].notna()) & (df['noise_level_db'].notna())].groupby('q_location').agg({
    'noise_level_db': ['count', 'mean', 'std', 'min', 'max']
}).round(2)

query6.columns = ['measurement_count', 'avg_noise_db', 'std_dev_db', 'min_noise_db', 'max_noise_db']
query6 = query6.reset_index()
query6.columns = ['location_type', 'measurement_count', 'avg_noise_db', 'std_dev_db', 'min_noise_db', 'max_noise_db']
query6 = query6.sort_values('avg_noise_db', ascending=False)

# Add most common category
query6['most_common_category'] = query6['location_type'].apply(
    lambda loc: df[df['q_location'] == loc]['noise_category'].mode().values[0] 
    if len(df[df['q_location'] == loc]['noise_category'].mode()) > 0 else 'Unknown'
)

print(query6.to_string())
query6.to_csv('query6_location_analysis.csv', index=False)
print("\nSaved to: query6_location_analysis.csv\n")

# ============================================================
# QUERY 7: PEAK NOISE HOURS BY LOCATION
# ============================================================
print("="*60)
print("QUERY 7: PEAK NOISE HOURS BY LOCATION")
print("="*60)

query7 = df[(df['q_location'].notna()) & (df['noise_level_db'].notna())].groupby(['q_location', 'hour']).agg({
    'noise_level_db': ['mean', 'count']
}).round(2)

query7.columns = ['avg_noise_db', 'measurement_count']
query7 = query7.reset_index()
query7.columns = ['location_type', 'hour', 'avg_noise_db', 'measurement_count']
query7 = query7.sort_values(['location_type', 'avg_noise_db'], ascending=[True, False])

print(query7.head(20).to_string())
query7.to_csv('query7_peak_hours_by_location.csv', index=False)
print("\nSaved to: query7_peak_hours_by_location.csv\n")

# ============================================================
# QUERY 8: TEMPORAL TRENDS - MONTHLY AGGREGATION
# ============================================================
print("="*60)
print("QUERY 8: TEMPORAL TRENDS - MONTHLY AGGREGATION")
print("="*60)

df['month'] = df['time'].dt.strftime('%Y-%m')

query8 = df[df['noise_level_db'].notna()].groupby('month').agg({
    'noise_level_db': ['count', 'mean', 'std']
}).round(2)

query8.columns = ['measurement_count', 'avg_noise_db', 'std_dev_db']
query8 = query8.reset_index()

query8['high_noise_count'] = df[df['noise_level_db'].notna()].groupby('month').apply(
    lambda x: (x['noise_level_db'] > 70).sum()
).values

query8 = query8.sort_values('month')

print(query8.to_string())
query8.to_csv('query8_monthly_trends.csv', index=False)
print("\nSaved to: query8_monthly_trends.csv\n")

# ============================================================
# QUERY 9: NOISE CATEGORY DISTRIBUTION
# ============================================================
print("="*60)
print("QUERY 9: NOISE CATEGORY DISTRIBUTION")
print("="*60)

query9 = df[df['noise_category'].notna()].groupby('noise_category').agg({
    'noise_level_db': ['count', 'mean', 'min', 'max']
}).round(2)

query9.columns = ['count', 'avg_noise_db', 'min_noise_db', 'max_noise_db']
query9 = query9.reset_index()
query9.columns = ['noise_category', 'count', 'avg_noise_db', 'min_noise_db', 'max_noise_db']

total = query9['count'].sum()
query9['percentage'] = (query9['count'] / total * 100).round(2)

query9 = query9.sort_values('count', ascending=False)

print(query9.to_string())
query9.to_csv('query9_category_distribution.csv', index=False)
print("\nSaved to: query9_category_distribution.csv\n")

# ============================================================
# QUERY 10: GEOGRAPHIC CORRELATION ANALYSIS
# ============================================================
print("="*60)
print("QUERY 10: GEOGRAPHIC CORRELATION ANALYSIS (Top 30)")
print("="*60)

query10 = df[(df['geo_zone'].notna()) & (df['noise_level_db'].notna())].groupby(['geo_zone', 'hour', 'day_of_week']).agg({
    'noise_level_db': ['count', 'mean', 'std']
}).round(2)

query10.columns = ['sample_size', 'avg_noise_db', 'std_dev_db']
query10 = query10.reset_index()

query10 = query10[query10['sample_size'] >= 5].sort_values('avg_noise_db', ascending=False).head(30)

print(query10.to_string())
query10.to_csv('query10_geographic_correlation.csv', index=False)
print("\nSaved to: query10_geographic_correlation.csv\n")

# ============================================================
# SUMMARY STATISTICS
# ============================================================
print("="*60)
print("SUMMARY STATISTICS")
print("="*60)
print(f"Total Records: {len(df)}")
print(f"Average Noise Level: {df['noise_level_db'].mean():.2f} dB")
print(f"Std Dev: {df['noise_level_db'].std():.2f} dB")
print(f"Min: {df['noise_level_db'].min():.2f} dB")
print(f"Max: {df['noise_level_db'].max():.2f} dB")
print(f"Median: {df['noise_level_db'].median():.2f} dB")
print("\n✅ All queries completed! Results saved to CSV files.\n")
