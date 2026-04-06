import csv
import multiprocessing as mp
import re
import time
import zlib
from pathlib import Path
import heapq

SEQUENCE_INVALID = {"123456789", "987654321", "111111111", "222222222", "333333333", "444444444", "555555555", "666666666", "777777777", "888888888", "999999999"}
MMSI_REGEX = re.compile(r"^\d{9}$")


def is_valid_row(row, mmsi_column, lat_column, lon_column):
	try:
		lat = float(row.get(lat_column, ""))
		lon = float(row.get(lon_column, ""))
		if lat == 0 or lon == 0 or abs(lat) > 90 or abs(lon) > 180:
			return False
	except ValueError:
		return False

	mmsi = row.get(mmsi_column, "").strip()

	if not MMSI_REGEX.fullmatch(mmsi):
		return False

	if mmsi == mmsi[0] * 9:
		return False

	if mmsi in SEQUENCE_INVALID:
		return False

	return True


def shard_file(args):
	"""Reads one input file, filters, and splits into shard files"""
	input_file, output_dir, file_suffix, num_cores, mmsi_column, lon_column, lat_column = args
	output_dir.mkdir(parents=True, exist_ok=True)

	total_rows = 0
	kept_rows = 0

	with open(input_file, "r", newline="", encoding="utf-8") as infile:
		reader = csv.DictReader(infile)
		fieldnames = reader.fieldnames

		# Open files handles simultaneously for this specific worker
		out_files = []
		writers = []
		for i in range(num_cores):
			filepath = output_dir / f"shard_{i}_{file_suffix}.csv"
			f = open(filepath, "w", newline="", encoding="utf-8")
			out_files.append(f)
			writer = csv.DictWriter(f, fieldnames=fieldnames)
			writer.writeheader()
			writers.append(writer)

		# Stream row by row
		for row in reader:
			total_rows += 1
			mmsi = row.get(mmsi_column, "").strip()

			if is_valid_row(row, mmsi_column, lat_column, lon_column):
				kept_rows += 1
				# Hash the string to get a uniform integer
				mmsi_hash = zlib.crc32(mmsi.encode("utf-8"))
				shard_id = mmsi_hash % num_cores
				writers[shard_id].writerow(row)

		# Close all file handles
		for f in out_files:
			f.close()

	return total_rows, kept_rows


def merge_and_sort_shard(args):
	"""Memory-Safe External Merge Sort for Shards"""
	shard_id, output_dir, suffixes, mmsi_column, time_column, chunk_size = args

	temp_files = []
	current_chunk = []
	chunk_index = 0
	fieldnames = None

	for suffix in suffixes:
		filepath = output_dir / f"shard_{shard_id}_{suffix}.csv"
		if not filepath.exists():
			continue

		with open(filepath, "r", newline="", encoding="utf-8") as f:
			reader = csv.DictReader(f)
			if fieldnames is None:
				fieldnames = reader.fieldnames

			for row in reader:
				current_chunk.append(row)

				if len(current_chunk) >= chunk_size:
					current_chunk.sort(key=lambda x: (x.get(mmsi_column, ""), x.get(time_column, "")))

					temp_filepath = output_dir / f"temp_shard_{shard_id}_chunk_{chunk_index}.csv"
					with open(temp_filepath, "w", newline="", encoding="utf-8") as tf:
						writer = csv.DictWriter(tf, fieldnames=fieldnames)
						writer.writeheader()
						writer.writerows(current_chunk)

					temp_files.append(temp_filepath)
					chunk_index += 1
					current_chunk = []

	if current_chunk:
		current_chunk.sort(key=lambda x: (x.get(mmsi_column, ""), x.get(time_column, "")))
		temp_filepath = output_dir / f"temp_shard_{shard_id}_chunk_{chunk_index}.csv"
		with open(temp_filepath, "w", newline="", encoding="utf-8") as tf:
			writer = csv.DictWriter(tf, fieldnames=fieldnames)
			writer.writeheader()
			writer.writerows(current_chunk)
		temp_files.append(temp_filepath)

	if not temp_files:
		return 0

	final_filepath = output_dir / f"final_shard_{shard_id}.csv"

	file_handles = [open(tf, "r", newline="", encoding="utf-8") for tf in temp_files]
	readers = [csv.DictReader(fh) for fh in file_handles]

	with open(final_filepath, "w", newline="", encoding="utf-8") as final_f:
		writer = csv.DictWriter(final_f, fieldnames=fieldnames)
		writer.writeheader()

		merged_stream = heapq.merge(*readers, key=lambda x: (x.get(mmsi_column, ""), x.get(time_column, "")))

		total_rows = 0
		for row in merged_stream:
			writer.writerow(row)
			total_rows += 1

	for fh in file_handles:
		fh.close()
	for tf in temp_files:
		tf.unlink()

	for suffix in suffixes:
		old_filepath = output_dir / f"shard_{shard_id}_{suffix}.csv"
		if old_filepath.exists():
			old_filepath.unlink()

	return total_rows


if __name__ == "__main__":
	import argparse

	parser = argparse.ArgumentParser()
	parser.add_argument("--cores", type=int, default=mp.cpu_count() - 1)
	parser.add_argument("--chunk_size", type=int, default=100000)
	args = parser.parse_args()

	num_cores = args.cores
	chunk_size = args.chunk_size

	BASE_DIR = Path(__file__).resolve().parent
	DATA_DIR = BASE_DIR / "data"
	OUTPUT_DIR = BASE_DIR / "output"

	input_files = [
		DATA_DIR / "aisdk-2024-03-01.csv",
		DATA_DIR / "aisdk-2024-03-02.csv",
	]
	output_file = OUTPUT_DIR / "aisdk-combined-clean.csv"

	mmsi_column = "MMSI"
	delimiter = ","
	time_column = "# Timestamp"
	lon_column = "Longitude"
	lat_column = "Latitude"

	print(f"System has {mp.cpu_count()} cores. Setting num_cores to {num_cores}.")

	print(f"Starting Phase 1: Filtering and Sharding ({num_cores} processes)")
 
	start_time1 = time.perf_counter()
	shard_args = [
		(input_files[0], OUTPUT_DIR, "day1", num_cores, mmsi_column, lon_column, lat_column),
		(input_files[1], OUTPUT_DIR, "day2", num_cores, mmsi_column, lon_column, lat_column)
	]
 
	with mp.Pool(processes=min(2, num_cores)) as pool:
		results = pool.map(shard_file, shard_args)

	total_processed = sum(r[0] for r in results)
	total_kept = sum(r[1] for r in results)

	end_time1 = time.perf_counter()

	execution_time1 = end_time1 - start_time1
	print(f"Phase 1 Complete. Processed {total_processed:,} rows. Kept {total_kept:,} rows. Took {execution_time1:.2f} seconds.")

	print(f"\nStarting Phase 2: Merging and Sorting ({num_cores} processes)...")
	start_time2 = time.perf_counter()


	merge_args = [
		(i, OUTPUT_DIR, ["day1", "day2"], mmsi_column, time_column, chunk_size)
		for i in range(num_cores)
	]

	with mp.Pool(processes=num_cores) as pool:
		final_counts = pool.map(merge_and_sort_shard, merge_args)

	end_time2 = time.perf_counter()

	execution_time2 = end_time2 - start_time2

	execution_time = end_time2 - start_time1

	print(f"Phase 2 Complete. {num_cores} final partitioned files generated in {OUTPUT_DIR}. Took {execution_time2:.2f} seconds.")
	print(f"Overall execution time: {execution_time:.2f} seconds.")
