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


def clean_large_csv(input_file, output_file, mmsi_column = "MMSI", delimiter = ",",
                    batch_size = 100_000, processes = None):
	if processes is None:
		processes = max(1, mp.cpu_count() - 1)

	total_rows = 0
	kept_rows = 0

	with open(input_file, "r", newline="", encoding="utf-8") as infile, open(
		output_file, "w", newline="", encoding="utf-8"
	) as outfile:
		reader = csv.DictReader(infile, delimiter=delimiter)

		if not reader.fieldnames:
			raise ValueError("Input file has no header row.")

		if mmsi_column not in reader.fieldnames:
			raise ValueError(
				f"Column '{mmsi_column}' not found. Available columns: {reader.fieldnames}"
			)

		writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=delimiter)
		writer.writeheader()

		tasks = ((batch, mmsi_column) for batch in batched_dict_rows(reader, batch_size))

		with mp.Pool(processes=processes) as pool:
			for valid_rows, batch_total, batch_kept in pool.imap(process_batch, tasks, chunksize=1):
				writer.writerows(valid_rows)
				total_rows += batch_total
				kept_rows += batch_kept

	removed_rows = total_rows - kept_rows
	print(f"Finished. Total rows: {total_rows:,}")
	print(f"Kept rows: {kept_rows:,}")
	print(f"Removed rows: {removed_rows:,}")


if __name__ == "__main__":
    # Read and clean MMSI values
	input_file = r"C:/Users/avark/Downloads/aisdk-2024-03-01/aisdk-2024-03-01.csv"
	output_file = r"C:/Users/avark/Downloads/aisdk-2024-03-01/output_clean.csv"
	mmsi_column = "MMSI"
	delimiter = ","
	batch_size = 100_000
	processes = None

	clean_large_csv(
		input_file=input_file,
		output_file=output_file,
		mmsi_column=mmsi_column,
		delimiter=delimiter,
		batch_size=batch_size,
		processes=processes,
	)