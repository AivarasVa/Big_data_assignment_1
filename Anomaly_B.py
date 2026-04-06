import csv
import math
import multiprocessing as mp
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from helper_functions import calculate_maritime_distance


def sweep_line_worker_block(args):
    """
    Worker function: Takes the full sorted list of events, but only processes
    the outer loop from 'start_row' to 'end_row'.
    """
    start_row, end_row, events = args
    results = []

    DISTANCE_THRESHOLD_NM = 0.2699
    TWO_HOURS = timedelta(hours=2)
    total_events = len(events)

    for i in range(start_row, end_row):
        e1 = events[i]

        for j in range(i + 1, total_events):
            e2 = events[j]

            if e2["Start_Time"] > (e1["End_Time"] - TWO_HOURS):
                break

            if e1["MMSI"] == e2["MMSI"]:
                continue

            overlap_start = max(e1["Start_Time"], e2["Start_Time"])
            overlap_end = min(e1["End_Time"], e2["End_Time"])
            overlap_duration = (overlap_end - overlap_start).total_seconds()

            if overlap_duration > (2 * 3600):
                distance = calculate_maritime_distance(e1["Lat"], e1["Lon"], e2["Lat"], e2["Lon"])

                if distance <= DISTANCE_THRESHOLD_NM:
                    results.append({
                        "Ship_1_MMSI": e1["MMSI"],
                        "Ship_2_MMSI": e2["MMSI"],
                        "Overlap_Start": overlap_start,
                        "Overlap_End": overlap_end,
                        "Overlap_Hours": round(overlap_duration / 3600.0, 2),
                        "Distance_NM": round(distance, 3),
                        "Lat": e1["Lat"],
                        "Lon": e1["Lon"]
                    })

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--cores", type=int, default=mp.cpu_count() - 1)
    parser.add_argument("--chunk_size", type=int, default=100000)  # Only needed if script uses it
    args = parser.parse_args()

    num_cores = args.cores

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = BASE_DIR / "output"

    input_csv = OUTPUT_DIR / "all_loitering_events.csv"

    if not input_csv.exists():
        print("Could not find all_loitering_events.csv. Run Phase 1 first!")
        exit()

    print("Loading and preparing loitering events...")
    events = []

    # 1. Load the data
    with open(input_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            events.append({
                "MMSI": row["MMSI"],
                "Start_Time": datetime.strptime(row["Start_Time"], "%d/%m/%Y %H:%M:%S"),
                "End_Time": datetime.strptime(row["End_Time"], "%d/%m/%Y %H:%M:%S"),
                "Lat": float(row["Lat"]),
                "Lon": float(row["Lon"])
            })

    # 2. Sort the full list chronologically
    print(f"Sorting {len(events)} events...")
    events.sort(key=lambda x: x["Start_Time"])

    print(f"Executing Parallel Block Partitioning across {num_cores} cores...")

    # 3. Calculate the boundaries (chunks) for each core
    pool_args = []
    total_rows = len(events)
    chunk_size = math.ceil(total_rows / num_cores)

    for i in range(num_cores):
        start_row = i * chunk_size
        end_row = min((i + 1) * chunk_size, total_rows)

        # If we've already assigned all rows, stop creating arguments
        if start_row >= total_rows:
            break

        # Give the core its start row, its end row, and a copy of the list
        pool_args.append((start_row, end_row, events))

    # 4. Fire the parallel workers
    with mp.Pool(processes=num_cores) as pool:
        nested_results = pool.map(sweep_line_worker_block, pool_args)

    anomaly_b_results = [item for sublist in nested_results for item in sublist]

    print(f"Search complete! Found {len(anomaly_b_results)} suspected ship-to-ship transfers.")

    # 5. Final Formatting
    if anomaly_b_results:
        df = pd.DataFrame(anomaly_b_results)

        # Clean up duplicates (A->B and B->A)
        df['Pair'] = df.apply(lambda row: tuple(sorted([row['Ship_1_MMSI'], row['Ship_2_MMSI']])), axis=1)
        df = df.drop_duplicates(subset=['Pair', 'Overlap_Start']).drop(columns=['Pair'])

        df = df.sort_values(by=['Overlap_Hours'], ascending=False)

        final_csv_path = OUTPUT_DIR / "anomaly_B_results.csv"
        df.to_csv(final_csv_path, index=False)

        print(f"\nSaved unified results to: {final_csv_path.name}")
        print("\n--- TOP 5 MOST SUSPICIOUS TRANSFERS (ANOMALY B) ---")
        print(df[['Ship_1_MMSI', 'Ship_2_MMSI', 'Overlap_Hours', 'Distance_NM']].head(5).to_string(index=False))