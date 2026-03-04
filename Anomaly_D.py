import csv
import math
import time
import multiprocessing as mp
from datetime import datetime
from itertools import groupby
from pathlib import Path
import pandas as pd


# --- HELPER FUNCTIONS ---

def calculate_maritime_distance(lat1, lon1, lat2, lon2):
    """Calculates the great-circle distance in Nautical Miles (NM)."""
    R = 3440.065
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def parse_timestamp(time_str):
    try:
        return datetime.strptime(time_str.strip(), "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None


# --- THE PARALLEL WORKER ---

def analyze_shard_anomaly_d(args):
    """
    Worker function: Scans a single sorted shard for Anomaly D.
    Detects impossible speeds (> 60 knots) indicating MMSI cloning.
    """
    shard_file, mmsi_col, time_col, lat_col, lon_col = args
    results = []

    if not shard_file.exists():
        return results

    SPEED_THRESHOLD_KNOTS = 60.0
    # Minimum distance required to rule out sub-second GPS multipath jitter
    MIN_DISTANCE_NM = 1.0

    with open(shard_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for mmsi, group in groupby(reader, key=lambda x: x.get(mmsi_col)):
            previous_ping = None

            for ping in group:
                try:
                    curr_time = parse_timestamp(ping.get(time_col, ""))
                    curr_lat = float(ping.get(lat_col, ""))
                    curr_lon = float(ping.get(lon_col, ""))


                except (ValueError, TypeError):
                    continue

                if curr_time is None:
                    continue

                if previous_ping is not None:
                    time_diff_seconds = (curr_time - previous_ping['time']).total_seconds()

                    # Prevent ZeroDivisionError for simultaneous duplicate pings
                    if time_diff_seconds == 0:
                        time_diff_seconds = 1


                    distance_nm = calculate_maritime_distance(
                        previous_ping['lat'], previous_ping['lon'],
                        curr_lat, curr_lon
                    )

                    # Only check speed if they are far enough apart to ignore GPS noise
                    if distance_nm > MIN_DISTANCE_NM:

                        hours = time_diff_seconds / 3600.0
                        speed_knots = distance_nm / hours

                        # 1. Did it teleport faster than 60 knots?
                        if speed_knots > SPEED_THRESHOLD_KNOTS:
                            results.append({
                                "MMSI": mmsi,
                                "Time_1": previous_ping['time'],
                                "Time_2": curr_time,
                                "Time_Diff_Seconds": time_diff_seconds,
                                "Distance_NM": round(distance_nm, 2),
                                "Implied_Speed_Knots": round(speed_knots, 2),
                                "Lat_1": previous_ping['lat'],
                                "Lon_1": previous_ping['lon'],
                                "Lat_2": curr_lat,
                                "Lon_2": curr_lon
                            })
                            # We only need to catch the clone once
                            break

                previous_ping = {
                    'time': curr_time,
                    'lat': curr_lat,
                    'lon': curr_lon
                }

    return results


# --- MAIN EXECUTION ---

if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = BASE_DIR / "output"

    mmsi_column = "MMSI"
    time_column = "# Timestamp"
    lat_column = "Latitude"
    lon_column = "Longitude"
    num_shards = max(1, mp.cpu_count() - 1)

    print(f"Starting Phase 3: Shadow Fleet Anomaly D Search using {num_shards} cores...")

    start_time = time.perf_counter()

    pool_args = []
    for i in range(num_shards):
        shard_path = OUTPUT_DIR / f"final_shard_{i}.csv"
        pool_args.append((shard_path, mmsi_column, time_column, lat_column, lon_column))

    with mp.Pool(processes=num_shards) as pool:
        nested_results = pool.map(analyze_shard_anomaly_d, pool_args)

    flat_results = [item for sublist in nested_results for item in sublist]

    end_time = time.perf_counter()

    execution_time = end_time - start_time

    print(f"Search complete! Found {len(flat_results)} vessels committing Anomaly D (Cloning).")
    print(f"Execution time: {execution_time:.2f} seconds.")

    if flat_results:
        df = pd.DataFrame(flat_results)

        # Sort by the most ridiculous speeds (the clearest evidence of cloning)
        df = df.sort_values(by=['Implied_Speed_Knots'], ascending=False)

        final_csv_path = OUTPUT_DIR / "anomaly_D_results.csv"
        df.to_csv(final_csv_path, index=False)

        print(f"\nSaved unified results to: {final_csv_path.name}")
        print("\n--- TOP 5 MOST SUSPICIOUS VESSELS (ANOMALY D) ---")
        print(df[['MMSI', 'Distance_NM', 'Implied_Speed_Knots', 'Time_1', 'Time_2']].head(5).to_string(index=False))