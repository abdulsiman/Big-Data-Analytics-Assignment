
"""
MapReduce Job for Noise Signal Processing
Processes noise sensor data to aggregate by location and time
"""

import sys
from collections import defaultdict
import math

class NoiseMapper:
    """Mapper: Emits (zone_hour, noise_level) pairs"""
    
    def map(self, line):
        """
        Input: Tab-separated sensor reading
        Output: (key, value) where key = zone_hour, value = noise_level
        """
        try:
            parts = line.strip().split('\t')
            
            if len(parts) < 11 or parts[0] == 'time':  # Skip header
                return []
            
            time, participant, lat, lon, noise_db, category, location, noise_kind, hour, day, geo_zone = parts[:11]
            
            # Skip if noise level is missing
            if not noise_db or noise_db == '':
                return []
            
            noise_db = float(noise_db)
            hour = int(float(hour))
            
            # Emit multiple keys for different aggregations
            results = []
            
            # 1. By geographic zone and hour
            key1 = f"zone_hour:{geo_zone}:{hour}"
            results.append((key1, noise_db))
            
            # 2. By location type and hour
            if location and location != '':
                key2 = f"location_hour:{location}:{hour}"
                results.append((key2, noise_db))
            
            # 3. By day of week
            key3 = f"day:{day}"
            results.append((key3, noise_db))
            
            # 4. By hour only (citywide)
            key4 = f"hour:{hour}"
            results.append((key4, noise_db))
            
            # 5. By geographic zone only
            key5 = f"zone:{geo_zone}"
            results.append((key5, noise_db))
            
            return results
            
        except Exception as e:
            # Skip malformed lines
            return []

class NoiseCombiner:
    """Combiner: Local aggregation before reduce"""
    
    def combine(self, key, values):
        """
        Perform local aggregation
        Output: (key, summary_stats)
        """
        values = [float(v) for v in values]
        
        if not values:
            return None
        
        # Calculate statistics
        count = len(values)
        total = sum(values)
        mean = total / count
        min_val = min(values)
        max_val = max(values)
        
        # Calculate variance for std dev
        variance = sum((x - mean) ** 2 for x in values) / count
        
        # Emit summary: count, sum, min, max, sum_of_squares
        sum_of_squares = sum(x ** 2 for x in values)
        
        return (key, f"{count},{total},{min_val},{max_val},{sum_of_squares}")

class NoiseReducer:
    """Reducer: Final aggregation and statistics"""
    
    def reduce(self, key, values):
        """
        Aggregate all values for a key
        Output: (key, aggregated_statistics)
        """
        # Parse combined values
        all_counts = []
        all_sums = []
        all_mins = []
        all_maxs = []
        all_sum_squares = []
        
        for value in values:
            if ',' in value:
                # From combiner
                count, total, min_val, max_val, sum_sq = value.split(',')
                all_counts.append(float(count))
                all_sums.append(float(total))
                all_mins.append(float(min_val))
                all_maxs.append(float(max_val))
                all_sum_squares.append(float(sum_sq))
            else:
                # Raw value
                val = float(value)
                all_counts.append(1)
                all_sums.append(val)
                all_mins.append(val)
                all_maxs.append(val)
                all_sum_squares.append(val ** 2)
        
        # Final aggregation
        total_count = sum(all_counts)
        total_sum = sum(all_sums)
        final_min = min(all_mins)
        final_max = max(all_maxs)
        total_sum_squares = sum(all_sum_squares)
        
        # Calculate statistics
        mean = total_sum / total_count
        variance = (total_sum_squares / total_count) - (mean ** 2)
        std_dev = math.sqrt(max(0, variance))
        
        # Format output
        result = f"{key}\t{total_count:.0f}\t{mean:.2f}\t{std_dev:.2f}\t{final_min:.2f}\t{final_max:.2f}"
        
        return result

def run_mapper(input_file, output_file):
    """Run mapper phase"""
    mapper = NoiseMapper()
    
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            results = mapper.map(line)
            for key, value in results:
                outfile.write(f"{key}\t{value}\n")
    
    print(f"Mapper output written to {output_file}")

def run_combiner(input_file, output_file):
    """Run combiner phase (optional optimization)"""
    combiner = NoiseCombiner()
    
    # Group by key
    key_values = defaultdict(list)
    
    with open(input_file, 'r') as infile:
        for line in infile:
            parts = line.strip().split('\t')
            if len(parts) == 2:
                key, value = parts
                key_values[key].append(value)
    
    # Combine
    with open(output_file, 'w') as outfile:
        for key, values in key_values.items():
            result = combiner.combine(key, values)
            if result:
                k, v = result
                outfile.write(f"{k}\t{v}\n")
    
    print(f"Combiner output written to {output_file}")

def run_reducer(input_file, output_file):
    """Run reducer phase"""
    reducer = NoiseReducer()
    
    # Group by key
    key_values = defaultdict(list)
    
    with open(input_file, 'r') as infile:
        for line in infile:
            parts = line.strip().split('\t')
            if len(parts) == 2:
                key, value = parts
                key_values[key].append(value)
    
    # Reduce
    with open(output_file, 'w') as outfile:
        # Write header
        outfile.write("key\tcount\tmean_db\tstd_dev\tmin_db\tmax_db\n")
        
        for key, values in sorted(key_values.items()):
            result = reducer.reduce(key, values)
            outfile.write(f"{result}\n")
    
    print(f"Reducer output written to {output_file}")

def run_full_mapreduce(input_file):
    """Run complete MapReduce pipeline"""
    print("="*60)
    print("MAPREDUCE NOISE SIGNAL PROCESSING")
    print("="*60)
    
    # Phase 1: Map
    print("\n[1/3] Running Mapper...")
    run_mapper(input_file, 'mapper_output.txt')
    
    # Phase 2: Combine (optional)
    print("\n[2/3] Running Combiner...")
    run_combiner('mapper_output.txt', 'combiner_output.txt')
    
    # Phase 3: Reduce
    print("\n[3/3] Running Reducer...")
    run_reducer('combiner_output.txt', 'mapreduce_results.txt')
    
    print("\n" + "="*60)
    print("MapReduce Complete!")
    print("="*60)
    print("\nResults saved to: mapreduce_results.txt")
    
    # Display sample results
    print("\nSample Results (Top 20 rows):")
    with open('mapreduce_results.txt', 'r') as f:
        for i, line in enumerate(f):
            if i < 21:
                print(line.strip())
            else:
                break

if __name__ == '__main__':
    input_file = 'noise_data_for_hdfs.tsv'
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    run_full_mapreduce(input_file)
