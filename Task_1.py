import csv
import multiprocessing as mp
import re
from itertools import islice


SEQUENCE_INVALID = {"123456789", "987654321"}
MMSI_REGEX = re.compile(r"^\d{9}$")


def is_valid_mmsi(value):
	mmsi = str(value).strip()

	if not MMSI_REGEX.fullmatch(mmsi):
		return False

	if mmsi == mmsi[0] * 9:
		return False

	if mmsi in SEQUENCE_INVALID:
		return False

	return True


def process_batch(args):
	rows, mmsi_column = args
	valid_rows = [row for row in rows if is_valid_mmsi(row.get(mmsi_column, ""))]
	return valid_rows, len(rows), len(valid_rows)


def batched_dict_rows(reader, batch_size):
	while True:
		batch = list(islice(reader, batch_size))
		if not batch:
			break
		yield batch


def clean_and_combine_csvs(input_files, output_file, mmsi_column = "MMSI", delimiter = ",",
                           batch_size = 100_000, processes = None):
	if not input_files:
		raise ValueError("No input files were provided.")

	if processes is None:
		processes = max(1, mp.cpu_count() - 1)

	total_rows = 0
	kept_rows = 0
	combined_fieldnames = None

	with open(output_file, "w", newline="", encoding="utf-8") as outfile:
		writer = None

		for input_file in input_files:
			with open(input_file, "r", newline="", encoding="utf-8") as infile:
				reader = csv.DictReader(infile, delimiter=delimiter)

				if not reader.fieldnames:
					raise ValueError(f"Input file '{input_file}' has no header row.")

				if mmsi_column not in reader.fieldnames:
					raise ValueError(
						f"Column '{mmsi_column}' not found in '{input_file}'. "
						f"Available columns: {reader.fieldnames}"
					)

				if combined_fieldnames is None:
					combined_fieldnames = reader.fieldnames
					writer = csv.DictWriter(outfile, fieldnames=combined_fieldnames, delimiter=delimiter)
					writer.writeheader()
				elif reader.fieldnames != combined_fieldnames:
					raise ValueError(
						f"Header mismatch in '{input_file}'. "
						f"Expected: {combined_fieldnames}, got: {reader.fieldnames}"
					)

				tasks = ((batch, mmsi_column) for batch in batched_dict_rows(reader, batch_size))

				with mp.Pool(processes=processes) as pool:
					for valid_rows, batch_total, batch_kept in pool.imap(process_batch, tasks, chunksize=1):
						writer.writerows(valid_rows)
						total_rows += batch_total
						kept_rows += batch_kept

	removed_rows = total_rows - kept_rows
	print(f"Finished. Files processed: {len(input_files)}")
	print(f"Finished. Total rows: {total_rows:,}")
	print(f"Kept rows: {kept_rows:,}")
	print(f"Removed rows: {removed_rows:,}")


if __name__ == "__main__":
	# Read, clean MMSI values, and combine into one file
	path = r"C:/Users/avark/Downloads/data/"
 
	input_files = [
		rf"{path}aisdk-2024-03-01.csv",
		rf"{path}aisdk-2024-03-02.csv",
	]
	output_file = rf"{path}aisdk-combined-clean.csv"
	mmsi_column = "MMSI"
	delimiter = ","
	batch_size = 100_000
	processes = None

	clean_and_combine_csvs(
		input_files=input_files,
		output_file=output_file,
		mmsi_column=mmsi_column,
		delimiter=delimiter,
		batch_size=batch_size,
		processes=processes,
	)