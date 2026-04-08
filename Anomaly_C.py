import csv
import time
import multiprocessing as mp
from itertools import groupby
from pathlib import Path
import pandas as pd
from helper_functions import parse_timestamp


def analyze_shard_anomaly_c(args):
    """
    Scans a single sorted shard for Anomaly C.
    Detects a >5% draught change during a >2 hour blackout.
    """
    shard_file, mmsi_col, time_col, draft_col = args
    results = []

    if not shard_file.exists():
        return results

    GAP_THRESHOLD_SECONDS = 2 * 3600
    DRAFT_CHANGE_THRESHOLD = 0.05

    with open(shard_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Stream data one ship at a time
        for mmsi, group in groupby(reader, key=lambda x: x.get(mmsi_col)):
            previous_ping = None

            for ping in group:
                curr_time = parse_timestamp(ping.get(time_col, ""))
                draft_str = ping.get(draft_col, "").strip()

                if not draft_str:
                    continue

                curr_draft = float(draft_str)

                if curr_draft <= 0.0:
                    continue
                
                if curr_time is None:
                    continue

                if previous_ping is not None:
                    time_diff = (curr_time - previous_ping['time']).total_seconds()

                    # Did it go dark for > 2 hours?
                    if time_diff > GAP_THRESHOLD_SECONDS:

                        prev_draft = previous_ping['draft']

                        # Calculate the absolute percentage change safely
                        draft_diff = abs(curr_draft - prev_draft)
                        pct_change = draft_diff / prev_draft

                        # Did the draft change by more than 5%?
                        if pct_change > DRAFT_CHANGE_THRESHOLD:
                            results.append({
                                "MMSI": mmsi,
                                "Disappeared_Time": previous_ping['time'],
                                "Reappeared_Time": curr_time,
                                "Gap_Hours": round(time_diff / 3600.0, 2),
                                "Old_Draft": prev_draft,
                                "New_Draft": curr_draft,
                                "Pct_Change": round(pct_change * 100, 2)
                            })
                            break

                previous_ping = {
                    'time': curr_time,
                    'draft': curr_draft
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

    # Configuration
    mmsi_column = "MMSI"
    time_column = "# Timestamp"
    draft_column = "Draught"


    print(f"Shadow Fleet Anomaly C Search using {num_cores} cores")

    start_time = time.perf_counter()

    pool_args = []
    for i in range(num_cores):
        shard_path = OUTPUT_DIR / f"final_shard_{i}.csv"
        pool_args.append((shard_path, mmsi_column, time_column, draft_column))

    with mp.Pool(processes=num_cores) as pool:
        nested_results = pool.map(analyze_shard_anomaly_c, pool_args)

    flat_results = [item for sublist in nested_results for item in sublist]

    end_time = time.perf_counter()

    execution_time = end_time - start_time

    print(f"{len(flat_results)} vessels committing Anomaly C.")
    print(f"Execution time: {execution_time:.2f} seconds.")

    if flat_results:
        df = pd.DataFrame(flat_results)

        # Sort by the most suspicious vessels
        df = df.sort_values(by=['Pct_Change', 'Gap_Hours'], ascending=[False, False])

        final_csv_path = OUTPUT_DIR / "anomaly_C_results.csv"
        df.to_csv(final_csv_path, index=False)

        print("\n--- TOP 5 MOST SUSPICIOUS VESSELS (ANOMALY C) ---")
        print(df[['MMSI', 'Gap_Hours', 'Old_Draft', 'New_Draft', 'Pct_Change']].head(5).to_string(index=False))