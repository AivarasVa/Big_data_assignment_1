import csv
import time
import multiprocessing as mp
from itertools import groupby
from pathlib import Path
import pandas as pd
from helper_functions import calculate_maritime_distance, parse_timestamp


def analyze_shard(args):
    """
    Scans a single sorted shard for Anomaly A
    """
    shard_file, mmsi_col, time_col, lat_col, lon_col = args
    results = []

    if not shard_file.exists():
        return results

    GAP_THRESHOLD_SECONDS = 4 * 3600
    MOVEMENT_THRESHOLD_NM = 1.0 

    with open(shard_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Stream data one ship at a time
        for mmsi, group in groupby(reader, key=lambda x: x.get(mmsi_col)):
            previous_ping = None

            for ping in group:
                curr_time = parse_timestamp(ping.get(time_col, ""))
                curr_lat = float(ping.get(lat_col, ""))
                curr_lon = float(ping.get(lon_col, ""))

                if curr_time is None:
                    continue

                if previous_ping is not None:
                    time_diff = (curr_time - previous_ping['time']).total_seconds()

                    # Did it go dark for > 4 hours?
                    if time_diff > GAP_THRESHOLD_SECONDS:
                        distance_nm = calculate_maritime_distance(previous_ping['lat'], previous_ping['lon'], curr_lat, curr_lon)

                        # Did it move significantly while dark?
                        if distance_nm > MOVEMENT_THRESHOLD_NM:
                            implied_knots = distance_nm / (time_diff / 3600.0)
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
                            break

                previous_ping = {
                    'time': curr_time,
                    'lat': curr_lat,
                    'lon': curr_lon
                }

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--cores", type=int, default=mp.cpu_count() - 1)
    parser.add_argument("--chunk_size", type=int, default=100000)
    args = parser.parse_args()

    num_cores = args.cores

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = BASE_DIR / "output"

    mmsi_column = "MMSI"
    time_column = "# Timestamp"
    lat_column = "Latitude"
    lon_column = "Longitude"

    print(f"Shadow Fleet Anomaly Search using {num_cores} cores")
    start_time = time.perf_counter()

    pool_args = []
    for i in range(num_cores):
        shard_path = OUTPUT_DIR / f"final_shard_{i}.csv"
        pool_args.append((shard_path, mmsi_column, time_column, lat_column, lon_column))

    with mp.Pool(processes=num_cores) as pool:
        nested_results = pool.map(analyze_shard, pool_args)

    flat_results = [item for sublist in nested_results for item in sublist]

    end_time = time.perf_counter()

    execution_time = end_time - start_time

    print(f"{len(flat_results)} vessels committing Anomaly A.")
    print(f"Execution time: {execution_time:.2f} seconds.")

    if flat_results:
        df = pd.DataFrame(flat_results)

        df = df.sort_values(by=['Implied_Knots', 'Gap_Hours'], ascending=[False, False])

        final_csv_path = OUTPUT_DIR / "anomaly_A_results.csv"
        df.to_csv(final_csv_path, index=False)

        print("\n--- TOP 5 MOST SUSPICIOUS VESSELS (ANOMALY A) ---")
        print(df[['MMSI', 'Gap_Hours', 'Distance_NM', 'Implied_Knots']].head(5).to_string(index=False))