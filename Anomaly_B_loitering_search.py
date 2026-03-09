import csv
import multiprocessing as mp
import heapq
from datetime import datetime
from itertools import groupby
from pathlib import Path


# --- HELPER PARSER ---
def parse_time_for_sort(row):
    """Helper function to guarantee correct chronological sorting of the European date format."""
    return datetime.strptime(row["Start_Time"], "%d/%m/%Y %H:%M:%S")


# --- THE EXTERNAL MERGE SORT ---

def external_merge_sort_csv(input_file, output_file, chunk_size=50000):
    """
    Sorts a CSV file using strict memory limits.
    Reads chunks into RAM, sorts them, writes to temp files, and merges them using a heap.
    """
    temp_files = []
    current_chunk = []
    chunk_index = 0
    fieldnames = None

    print("\nStarting memory-safe External Merge Sort on loitering events...")

    # --- STEP 1: CHUNK AND SORT ---
    with open(input_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            current_chunk.append(row)

            # Flush to disk when memory buffer is full
            if len(current_chunk) >= chunk_size:
                current_chunk.sort(key=parse_time_for_sort)
                temp_filepath = input_file.with_name(f"temp_loiter_{chunk_index}.csv")

                with open(temp_filepath, "w", newline="", encoding="utf-8") as tf:
                    writer = csv.DictWriter(tf, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(current_chunk)

                temp_files.append(temp_filepath)
                chunk_index += 1
                current_chunk = []  # Clear RAM

        # Flush any remaining rows
        if current_chunk:
            current_chunk.sort(key=parse_time_for_sort)
            temp_filepath = input_file.with_name(f"temp_loiter_{chunk_index}.csv")
            with open(temp_filepath, "w", newline="", encoding="utf-8") as tf:
                writer = csv.DictWriter(tf, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(current_chunk)
            temp_files.append(temp_filepath)

    # --- STEP 2: STREAMING MERGE ---
    if not temp_files:
        return  # Nothing to sort

    # Open all temporary files simultaneously
    file_handles = [open(tf, "r", newline="", encoding="utf-8") for tf in temp_files]
    readers = [csv.DictReader(fh) for fh in file_handles]

    with open(output_file, "w", newline="", encoding="utf-8") as final_f:
        writer = csv.DictWriter(final_f, fieldnames=fieldnames)
        writer.writeheader()

        # heapq.merge streams rows one-by-one chronologically across all readers
        merged_stream = heapq.merge(*readers, key=parse_time_for_sort)

        for row in merged_stream:
            writer.writerow(row)

    # --- STEP 3: CLEANUP ---
    for fh in file_handles:
        fh.close()
    for tf in temp_files:
        tf.unlink()

    print(f"Sort complete. Final sorted file saved at: {output_file.name}")


# --- THE CONSUMER (THE WRITER) ---

def csv_writer_process(queue, output_filepath):
    """
    Runs in the background. Listens to the queue and writes directly to disk.
    Shuts down safely when it receives the 'POISON_PILL'.
    """
    fieldnames = ["MMSI", "Start_Time", "End_Time", "Duration_Hours", "Lat", "Lon"]

    with open(output_filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        events_written = 0
        while True:
            event = queue.get()

            if event == "POISON_PILL":
                break

            writer.writerow(event)
            events_written += 1

    print(f"Writer Process finished. Successfully wrote {events_written} unsorted events to disk.")


# --- THE PRODUCERS (THE WORKERS) ---

def find_loitering_worker(args):
    """
    Worker function: Scans a shard and pushes events to the Queue.
    """
    shard_file, queue, mmsi_col, time_col, lat_col, lon_col, sog_col = args

    if not shard_file.exists():
        return

    SOG_THRESHOLD = 1.0
    TIME_MIN_SECONDS = 2 * 3600

    with open(shard_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for mmsi, group in groupby(reader, key=lambda x: x.get(mmsi_col)):
            loiter_start = None
            loiter_lat = None
            loiter_lon = None
            last_valid_time = None

            for ping in group:
                try:
                    curr_time = datetime.strptime(ping.get(time_col, "").strip(), "%d/%m/%Y %H:%M:%S")
                    curr_lat = float(ping.get(lat_col, ""))
                    curr_lon = float(ping.get(lon_col, ""))
                    sog_str = ping.get(sog_col, "").strip()

                    if not sog_str:
                        continue
                    curr_sog = float(sog_str)

                    if curr_lat == 0.0 or curr_lon == 0.0 or abs(curr_lat) > 90 or abs(curr_lon) > 180:
                        continue

                except (ValueError, TypeError):
                    continue

                if curr_sog < SOG_THRESHOLD:
                    if loiter_start is None:
                        loiter_start = curr_time
                        loiter_lat = curr_lat
                        loiter_lon = curr_lon
                    last_valid_time = curr_time
                else:
                    if loiter_start is not None and last_valid_time is not None:
                        duration = (last_valid_time - loiter_start).total_seconds()
                        if duration > TIME_MIN_SECONDS:
                            queue.put({
                                "MMSI": mmsi,
                                "Start_Time": loiter_start.strftime("%d/%m/%Y %H:%M:%S"),
                                "End_Time": last_valid_time.strftime("%d/%m/%Y %H:%M:%S"),
                                "Duration_Hours": round(duration / 3600.0, 2),
                                "Lat": loiter_lat,
                                "Lon": loiter_lon
                            })
                    loiter_start = None
                    last_valid_time = None

            if loiter_start is not None and last_valid_time is not None:
                duration = (last_valid_time - loiter_start).total_seconds()
                if duration > TIME_MIN_SECONDS:
                    queue.put({
                        "MMSI": mmsi,
                        "Start_Time": loiter_start.strftime("%d/%m/%Y %H:%M:%S"),
                        "End_Time": last_valid_time.strftime("%d/%m/%Y %H:%M:%S"),
                        "Duration_Hours": round(duration / 3600.0, 2),
                        "Lat": loiter_lat,
                        "Lon": loiter_lon
                    })


# --- MAIN EXECUTION ---

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--cores", type=int, default=mp.cpu_count() - 1)
    parser.add_argument("--chunk_size", type=int, default=50000)
    args = parser.parse_args()

    num_cores = args.cores # or num_cores = args.cores depending on what you named it

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = BASE_DIR / "output"

    mmsi_col = "MMSI"
    time_col = "# Timestamp"
    lat_col = "Latitude"
    lon_col = "Longitude"
    sog_col = "SOG"


    raw_output_csv = OUTPUT_DIR / "all_loitering_events_unsorted.csv"
    final_sorted_csv = OUTPUT_DIR / "all_loitering_events.csv"

    print(f"Starting Queue-Based Data Reduction using {num_cores} workers...")

    # 1. Create the Manager and the memory-capped Queue
    manager = mp.Manager()
    event_queue = manager.Queue(maxsize=2000)

    # 2. Start the dedicated Writer Process
    writer_proc = mp.Process(target=csv_writer_process, args=(event_queue, raw_output_csv))
    writer_proc.start()

    # 3. Prepare arguments and execute the parallel workers
    pool_args = []
    for i in range(num_cores):
        shard_path = OUTPUT_DIR / f"final_shard_{i}.csv"
        pool_args.append((shard_path, event_queue, mmsi_col, time_col, lat_col, lon_col, sog_col))

    with mp.Pool(processes=num_cores) as pool:
        pool.map(find_loitering_worker, pool_args)

    # 4. Graceful Shutdown of the Writer
    event_queue.put("POISON_PILL")
    writer_proc.join()

    # 5. External Merge Sort the raw file
    if raw_output_csv.exists():
        external_merge_sort_csv(raw_output_csv, final_sorted_csv, chunk_size=args.chunk_size)

        # Optional: Delete the unsorted file to keep the output directory clean
        raw_output_csv.unlink()

    print(f"Phase 1 Complete. Master sorted file generated at: {final_sorted_csv.name}")