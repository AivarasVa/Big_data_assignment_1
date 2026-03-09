import csv
import math
import time
import multiprocessing as mp
from datetime import datetime
from itertools import groupby
from pathlib import Path
import pandas as pd


# --- CORE MATH FUNCTIONS ---

def calculate_maritime_distance(lat1, lon1, lat2, lon2):
    """Calculates the great-circle distance in Nautical Miles (NM)."""
    R = 3440.065  # Radius of Earth in Nautical Miles
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def parse_timestamp(time_str):
    """Converts the AIS timestamp string to a datetime object."""
    try:
        return datetime.strptime(time_str.strip(), "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None


# --- THE PARALLEL WORKER ---

def analyze_shard(args):
    """
    Worker function: Scans a single sorted shard for Anomaly A.
    Memory remains incredibly low because itertools.groupby only
    loads one ship's history into RAM at a time.
    """
    shard_file, mmsi_col, time_col, lat_col, lon_col = args
    results = []

    if not shard_file.exists():
        return results

    GAP_THRESHOLD_SECONDS = 4 * 3600  # 4 hours
    MOVEMENT_THRESHOLD_NM = 1.0  # 1 Nautical Mile

    with open(shard_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Stream data one ship at a time
        for mmsi, group in groupby(reader, key=lambda x: x.get(mmsi_col)):
            previous_ping = None

            for ping in group:
                try:
                    curr_time = parse_timestamp(ping.get(time_col, ""))
                    curr_lat = float(ping.get(lat_col, ""))
                    curr_lon = float(ping.get(lon_col, ""))
                except (ValueError, TypeError):
                    continue  # Skip corrupted rows

                if curr_time is None:
                    continue

                if previous_ping is not None:
                    time_diff = (curr_time - previous_ping['time']).total_seconds()

                    # 1. Did it go dark for > 4 hours?
                    if time_diff > GAP_THRESHOLD_SECONDS:
                        distance_nm = calculate_maritime_distance(
                            previous_ping['lat'], previous_ping['lon'],
                            curr_lat, curr_lon
                        )

                        # 2. Did it move significantly while dark?
                        if distance_nm > MOVEMENT_THRESHOLD_NM:
                            implied_knots = distance_nm / (time_diff / 3600.0)
                            # print(f"MMSI:{mmsi},Current location: {curr_lat}, {curr_lon}, Previous location: {previous_ping['lat']}, {previous_ping['lon']}, Implied knots: {implied_knots}")
                            results.append({
                                "MMSI": mmsi,
                                "Disappeared_Time": previous_ping['time'],
                                "Reappeared_Time": curr_time,
                                "Gap_Hours": round(time_diff / 3600.0, 2),
                                "Distance_NM": round(distance_nm, 2),
                                "Implied_Knots": round(implied_knots, 2),
                                "Start_Lat": previous_ping['lat'],
                                "Start_Lon": previous_ping['lon'],
                                "End_Lat": curr_lat,
                                "End_Lon": curr_lon
                            })
                            # We only need to catch them doing it once for the DFSI flag
                            break

                previous_ping = {
                    'time': curr_time,
                    'lat': curr_lat,
                    'lon': curr_lon
                }

    return results


# --- MAIN EXECUTION (POOLING & UNIFICATION) ---

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--cores", type=int, default=mp.cpu_count() - 1)
    parser.add_argument("--chunk_size", type=int, default=100000)  # Only needed if script uses it
    args = parser.parse_args()

    num_cores = args.cores

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = BASE_DIR / "output"

    # Configuration
    mmsi_column = "MMSI"
    time_column = "# Timestamp"
    lat_column = "Latitude"
    lon_column = "Longitude"

    print(f"Starting Phase 3: Shadow Fleet Anomaly Search using {num_cores} cores...")
    start_time = time.perf_counter()
    # 1. Prepare arguments for the parallel pool
    pool_args = []
    for i in range(num_cores):
        shard_path = OUTPUT_DIR / f"final_shard_{i}.csv"
        pool_args.append((shard_path, mmsi_column, time_column, lat_column, lon_column))

    # 2. Execute the pool
    with mp.Pool(processes=num_cores) as pool:
        # returns a list of lists: [ [results_from_shard_0], [results_from_shard_1], ... ]
        nested_results = pool.map(analyze_shard, pool_args)

    # 3. Unify the results (Flatten the list of lists)
    flat_results = [item for sublist in nested_results for item in sublist]

    end_time = time.perf_counter()

    execution_time = end_time - start_time

    print(f"Search complete! Found {len(flat_results)} vessels committing Anomaly A.")
    print(f"Execution time: {execution_time:.2f} seconds.")




    # 4. The Pandas Reward (Legal for final formatting!)
    if flat_results:
        df = pd.DataFrame(flat_results)

        # Sort by the most suspicious vessels (longest gap combined with movement)
        df = df.sort_values(by=['Implied_Knots', 'Gap_Hours'], ascending=[False, False])

        # Save unified results to CSV
        final_csv_path = OUTPUT_DIR / "anomaly_A_results.csv"
        df.to_csv(final_csv_path, index=False)

        print(f"\nSaved unified results to: {final_csv_path.name}")
        print("\n--- TOP 5 MOST SUSPICIOUS VESSELS (ANOMALY A) ---")
        print(df[['MMSI', 'Gap_Hours', 'Distance_NM', 'Implied_Knots']].head(5).to_string(index=False))