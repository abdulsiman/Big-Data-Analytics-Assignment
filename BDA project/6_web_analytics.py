
"""
Web Analytics for Public Noise Complaints
Analyzes patterns in public complaints about noise pollution
"""

import pandas as pd
import numpy as np
from collections import Counter
import re
from datetime import datetime
import json

class NoiseComplaintAnalyzer:
    """Analyze public complaints about noise pollution"""
    
    def __init__(self):
        """Initialize analyzer"""
        self.complaint_keywords = {
            'traffic': ['traffic', 'car', 'vehicle', 'honking', 'horn', 'road'],
            'construction': ['construction', 'drilling', 'building', 'renovation', 'hammer'],
            'industrial': ['factory', 'industrial', 'machinery', 'equipment', 'plant'],
            'entertainment': ['music', 'party', 'club', 'bar', 'nightclub', 'concert'],
            'aircraft': ['aircraft', 'airplane', 'plane', 'helicopter', 'airport', 'flight'],
            'neighbors': ['neighbor', 'apartment', 'upstairs', 'next door', 'loud talking'],
            'animals': ['dog', 'barking', 'pet', 'animal', 'rooster', 'bird'],
            'public_events': ['event', 'festival', 'parade', 'fireworks', 'celebration']
        }
        
    def generate_synthetic_complaints(self, sensor_data_df, num_complaints=500):
        """Generate synthetic complaint data based on sensor readings"""
        print("Generating synthetic complaint data...")
        
        # Filter high noise readings for complaints
        high_noise = sensor_data_df[sensor_data_df['noise_level_db'] > 70].copy()
        
        if len(high_noise) < num_complaints:
            high_noise = sensor_data_df.sample(n=min(num_complaints, len(sensor_data_df)))
        else:
            high_noise = high_noise.sample(n=num_complaints)
        
        complaints = []
        
        for idx, row in high_noise.iterrows():
            # Determine complaint type based on location and time
            hour = row.get('hour', 12)
            location_type = str(row.get('q_location', 'Unknown'))
            noise_db = row.get('noise_level_db', 65)
            
            # Select complaint category
            if 'Transport' in location_type:
                category = 'traffic'
            elif 7 <= hour <= 18:
                category = np.random.choice(['construction', 'traffic', 'industrial'], p=[0.5, 0.3, 0.2])
            elif 18 <= hour <= 23:
                category = np.random.choice(['entertainment', 'neighbors', 'traffic'], p=[0.4, 0.4, 0.2])
            else:
                category = np.random.choice(['neighbors', 'animals', 'traffic'], p=[0.5, 0.3, 0.2])
            
            # Generate complaint text
            templates = {
                'traffic': [
                    "Excessive traffic noise from main road",
                    "Constant honking and vehicle sounds disturbing peace",
                    "Heavy truck traffic causing loud noise pollution",
                    "Motorcycles racing and creating noise disturbance"
                ],
                'construction': [
                    "Construction noise from nearby building site",
                    "Drilling and hammering noise all day long",
                    "Building renovation causing severe noise pollution",
                    "Construction equipment operating without permits"
                ],
                'industrial': [
                    "Factory machinery running 24/7 causing noise",
                    "Industrial equipment creating loud disturbances",
                    "Manufacturing plant noise affecting residential area",
                    "Warehouse operations making excessive noise"
                ],
                'entertainment': [
                    "Loud music from nearby club/bar",
                    "Party noise continuing past midnight",
                    "Nightclub bass disturbing entire neighborhood",
                    "Concert venue causing noise complaints"
                ],
                'aircraft': [
                    "Low-flying aircraft creating noise pollution",
                    "Helicopter noise disrupting daily activities",
                    "Airport traffic causing constant noise",
                    "Frequent flights over residential area"
                ],
                'neighbors': [
                    "Neighbors making loud noise late at night",
                    "Upstairs neighbor stomping and moving furniture",
                    "Adjacent apartment playing loud music",
                    "Next door renovation noise unreasonable hours"
                ],
                'animals': [
                    "Dogs barking continuously throughout the day",
                    "Neighbor's pet creating noise disturbance",
                    "Rooster crowing early morning hours",
                    "Multiple dogs barking in the area"
                ],
                'public_events': [
                    "Festival fireworks causing noise pollution",
                    "Public event loudspeakers too loud",
                    "Street parade creating excessive noise",
                    "Community celebration disturbing peace"
                ]
            }
            
            complaint_text = np.random.choice(templates[category])
            
            complaint = {
                'complaint_id': f"NC{idx:06d}",
                'timestamp': row.get('time', datetime.now()),
                'latitude': row.get('ws_latitude'),
                'longitude': row.get('ws_longitude'),
                'category': category,
                'complaint_text': complaint_text,
                'estimated_noise_db': noise_db,
                'location_type': location_type,
                'hour': hour,
                'day_of_week': row.get('day_of_week', 0),
                'geo_zone': row.get('geo_zone', 'Unknown'),
                'status': np.random.choice(['Open', 'In Progress', 'Resolved', 'Closed'], 
                                          p=[0.3, 0.2, 0.3, 0.2])
            }
            
            complaints.append(complaint)
        
        complaints_df = pd.DataFrame(complaints)
        print(f"Generated {len(complaints_df)} synthetic complaints")
        
        return complaints_df
    
    def analyze_complaint_patterns(self, complaints_df):
        """Analyze patterns in complaints"""
        print("\n" + "="*60)
        print("COMPLAINT PATTERN ANALYSIS")
        print("="*60)
        
        # 1. Category distribution
        print("\n1. Complaint Category Distribution:")
        category_counts = complaints_df['category'].value_counts()
        for cat, count in category_counts.items():
            pct = 100 * count / len(complaints_df)
            print(f"   {cat:15s}: {count:4d} ({pct:5.1f}%)")
        
        # 2. Temporal patterns
        print("\n2. Complaints by Hour:")
        hourly = complaints_df.groupby('hour').size()
        for hour, count in hourly.items():
            print(f"   Hour {hour:02d}: {count:4d} complaints")
        
        # 3. Geographic hotspots
        print("\n3. Top 10 Complaint Zones:")
        zone_counts = complaints_df['geo_zone'].value_counts().head(10)
        for zone, count in zone_counts.items():
            avg_noise = complaints_df[complaints_df['geo_zone'] == zone]['estimated_noise_db'].mean()
            print(f"   Zone {zone:8s}: {count:4d} complaints (Avg: {avg_noise:.1f} dB)")
        
        # 4. Status distribution
        print("\n4. Complaint Status:")
        status_counts = complaints_df['status'].value_counts()
        for status, count in status_counts.items():
            pct = 100 * count / len(complaints_df)
            print(f"   {status:12s}: {count:4d} ({pct:5.1f}%)")
        
        # 5. Day of week analysis
        print("\n5. Complaints by Day of Week:")
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_counts = complaints_df['day_of_week'].value_counts().sort_index()
        for day, count in dow_counts.items():
            if day < len(day_names):
                print(f"   {day_names[day]:10s}: {count:4d} complaints")
    
    def perform_text_analytics(self, complaints_df):
        """Perform text analytics on complaint descriptions"""
        print("\n" + "="*60)
        print("TEXT ANALYTICS")
        print("="*60)
        
        all_words = []
        for text in complaints_df['complaint_text']:
            words = re.findall(r'\b[a-z]{4,}\b', text.lower())
            all_words.extend(words)
        
        # Word frequency
        word_freq = Counter(all_words)
        
        print("\nTop 20 Most Common Words:")
        for word, count in word_freq.most_common(20):
            print(f"   {word:15s}: {count:4d}")
        
        # Keyword analysis
        print("\nKeyword Category Matches:")
        for category, keywords in self.complaint_keywords.items():
            total_matches = sum(1 for text in complaints_df['complaint_text'] 
                              if any(kw in text.lower() for kw in keywords))
            pct = 100 * total_matches / len(complaints_df)
            print(f"   {category:15s}: {total_matches:4d} ({pct:5.1f}%)")
    
    def generate_insights(self, complaints_df, sensor_df):
        """Generate actionable insights"""
        print("\n" + "="*60)
        print("ACTIONABLE INSIGHTS")
        print("="*60)
        
        insights = []
        
        # Insight 1: High complaint zones
        top_zones = complaints_df['geo_zone'].value_counts().head(5).index.tolist()
        avg_noise = complaints_df[complaints_df['geo_zone'].isin(top_zones)]['estimated_noise_db'].mean()
        
        insights.append({
            'type': 'Geographic Hotspot',
            'priority': 'High',
            'description': f"Top 5 zones account for {100 * len(complaints_df[complaints_df['geo_zone'].isin(top_zones)]) / len(complaints_df):.1f}% of complaints",
            'recommendation': f"Deploy additional noise monitoring sensors in zones: {', '.join(top_zones)}",
            'avg_noise_db': round(avg_noise, 2)
        })
        
        # Insight 2: Peak complaint hours
        peak_hour = complaints_df['hour'].value_counts().idxmax()
        peak_count = complaints_df['hour'].value_counts().max()
        
        insights.append({
            'type': 'Temporal Pattern',
            'priority': 'Medium',
            'description': f"Peak complaint hour is {peak_hour}:00 with {peak_count} complaints",
            'recommendation': f"Increase noise patrols during hours {peak_hour-1}-{peak_hour+1}",
            'peak_hour': peak_hour
        })
        
        # Insight 3: Category concentration
        top_category = complaints_df['category'].value_counts().idxmax()
        top_cat_pct = 100 * complaints_df['category'].value_counts().max() / len(complaints_df)
        
        insights.append({
            'type': 'Complaint Category',
            'priority': 'High' if top_cat_pct > 30 else 'Medium',
            'description': f"{top_category.title()} complaints represent {top_cat_pct:.1f}% of total",
            'recommendation': f"Targeted intervention for {top_category} noise sources",
            'category': top_category
        })
        
        # Insight 4: Unresolved complaints
        unresolved = complaints_df[complaints_df['status'].isin(['Open', 'In Progress'])]
        unresolved_pct = 100 * len(unresolved) / len(complaints_df)
        
        insights.append({
            'type': 'Response Efficiency',
            'priority': 'High' if unresolved_pct > 50 else 'Medium',
            'description': f"{unresolved_pct:.1f}% of complaints are unresolved",
            'recommendation': "Review complaint resolution workflow and allocate more resources",
            'unresolved_count': len(unresolved)
        })
        
        print("\nKey Insights:")
        for i, insight in enumerate(insights, 1):
            print(f"\n{i}. [{insight['priority']}] {insight['type']}")
            print(f"   Issue: {insight['description']}")
            print(f"   Action: {insight['recommendation']}")
        
        return insights
    
    def export_analytics_report(self, complaints_df, sensor_df, insights, output_file='web_analytics_report.json'):
        """Export comprehensive analytics report"""
        print(f"\nExporting analytics report to {output_file}...")
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_complaints': len(complaints_df),
                'total_sensors': len(sensor_df),
                'date_range': {
                    'start': str(complaints_df['timestamp'].min()),
                    'end': str(complaints_df['timestamp'].max())
                },
                'avg_noise_level_db': float(complaints_df['estimated_noise_db'].mean()),
                'max_noise_level_db': float(complaints_df['estimated_noise_db'].max())
            },
            'category_distribution': complaints_df['category'].value_counts().to_dict(),
            'hourly_distribution': complaints_df['hour'].value_counts().sort_index().to_dict(),
            'status_distribution': complaints_df['status'].value_counts().to_dict(),
            'top_complaint_zones': complaints_df['geo_zone'].value_counts().head(10).to_dict(),
            'insights': insights
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"✓ Analytics report exported")

def main():
    """Main execution"""
    print("="*60)
    print("WEB ANALYTICS - PUBLIC NOISE COMPLAINTS")
    print("="*60)
    
    # Load sensor data
    print("\nLoading sensor data...")
    sensor_df = pd.read_csv('processed_noise_data.csv')
    print(f"Loaded {len(sensor_df)} sensor readings")
    
    # Initialize analyzer
    analyzer = NoiseComplaintAnalyzer()
    
    # Generate synthetic complaints
    complaints_df = analyzer.generate_synthetic_complaints(sensor_df, num_complaints=500)
    
    # Save complaints
    complaints_df.to_csv('public_complaints.csv', index=False)
    print("✓ Complaints saved to public_complaints.csv")
    
    # Analyze patterns
    analyzer.analyze_complaint_patterns(complaints_df)
    
    # Text analytics
    analyzer.perform_text_analytics(complaints_df)
    
    # Generate insights
    insights = analyzer.generate_insights(complaints_df, sensor_df)
    
    # Export report
    analyzer.export_analytics_report(complaints_df, sensor_df, insights)
    
    print("\n" + "="*60)
    print("WEB ANALYTICS COMPLETE")
    print("="*60)
    print("\nOutput Files:")
    print("  - public_complaints.csv (Complaint records)")
    print("  - web_analytics_report.json (Analytics report)")

if __name__ == '__main__':
    main()
