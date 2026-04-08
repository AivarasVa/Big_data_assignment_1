import csv
import time
import multiprocessing as mp
from itertools import groupby
from pathlib import Path
import pandas as pd
from helper_functions import calculate_maritime_distance, parse_timestamp


def analyze_shard_anomaly_d(args):
    """
    Scans a single sorted shard for Anomaly D.
    Detects impossible speeds (> 60 knots) indicating MMSI cloning.
    """
    shard_file, mmsi_col, time_col, lat_col, lon_col = args
    results = []

    if not shard_file.exists():
        return results

    SPEED_THRESHOLD_KNOTS = 60.0
    MIN_DISTANCE_NM = 1.0

    with open(shard_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for mmsi, group in groupby(reader, key=lambda x: x.get(mmsi_col)):
            previous_ping = None

            for ping in group:
                curr_time = parse_timestamp(ping.get(time_col, ""))
                curr_lat = float(ping.get(lat_col, ""))
                curr_lon = float(ping.get(lon_col, ""))

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

                        # Did it teleport faster than 60 knots?
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


    print(f"Shadow Fleet Anomaly D Search using {num_cores} cores.")

    start_time = time.perf_counter()

    pool_args = []
    for i in range(num_cores):
        shard_path = OUTPUT_DIR / f"final_shard_{i}.csv"
        pool_args.append((shard_path, mmsi_column, time_column, lat_column, lon_column))

    with mp.Pool(processes=num_cores) as pool:
        nested_results = pool.map(analyze_shard_anomaly_d, pool_args)

    flat_results = [item for sublist in nested_results for item in sublist]

    end_time = time.perf_counter()

    execution_time = end_time - start_time

    print(f"{len(flat_results)} vessels committing Anomaly D.")
    print(f"Execution time: {execution_time:.2f} seconds.")

    if flat_results:
        df = pd.DataFrame(flat_results)

        df = df.sort_values(by=['Implied_Speed_Knots'], ascending=False)

        final_csv_path = OUTPUT_DIR / "anomaly_D_results.csv"
        df.to_csv(final_csv_path, index=False)

        print("\n--- TOP 5 MOST SUSPICIOUS VESSELS (ANOMALY D) ---")
        print(df[['MMSI', 'Distance_NM', 'Implied_Speed_Knots', 'Time_1', 'Time_2']].head(5).to_string(index=False))