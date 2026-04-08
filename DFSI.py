import csv
import time
from pathlib import Path
from collections import defaultdict


def calculate_dfsi():
    start_time = time.time()

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = BASE_DIR / "output"

    file_a = OUTPUT_DIR / "anomaly_A_results.csv"
    file_c = OUTPUT_DIR / "anomaly_C_results.csv"
    file_d = OUTPUT_DIR / "anomaly_D_results.csv"

    mmsi_data = defaultdict(lambda: {'max_gap': 0.0, 'c_count': 0, 'total_dist': 0.0})

    if file_a.exists():
        with open(file_a, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = row["MMSI"]
                gap_hours = float(row.get("Gap_Hours", 0))
                # Update with the MAXIMUM gap found for this ship
                mmsi_data[mmsi]['max_gap'] = max(mmsi_data[mmsi]['max_gap'], gap_hours)

    if file_c.exists():
        with open(file_c, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = row["MMSI"]
                # Count the NUMBER of illicit draft changes
                mmsi_data[mmsi]['c_count'] += 1

    if file_d.exists():
        with open(file_d, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = row["MMSI"]
                dist = float(row.get("Distance_NM", 0))
                # Sum the TOTAL impossible distance jumped
                mmsi_data[mmsi]['total_dist'] += dist

    if not mmsi_data:
        print("No anomaly data found to process!")
        return

    print("Calculating final DFSI scores.")
    final_results = []

    # Iterate through the dictionary and apply the formula
    for mmsi, metrics in mmsi_data.items():
        max_gap = metrics['max_gap']
        c_count = metrics['c_count']
        total_dist = metrics['total_dist']

        dfsi = (max_gap / 2.0) + (total_dist / 10.0) + (c_count * 15.0)

        final_results.append({
            "MMSI": mmsi,
            "DFSI_Score": round(dfsi, 2),
            "Max_Gap_Hours": round(max_gap, 2),
            "Total_Impossible_Distance_NM": round(total_dist, 2),
            "Draft_Changes_Count": c_count
        })

    # Sort Results
    final_results.sort(key=lambda x: x["DFSI_Score"], reverse=True)

    # Write Final Output to CSV
    final_output_path = OUTPUT_DIR / "FINAL_DFSI_RANKINGS.csv"
    with open(final_output_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = final_results[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(final_results)

    execution_time = time.time() - start_time

    print("\n========================================================")
    print("FINAL RESULTS: TOP 5 SHADOW FLEET SUSPECTS")
    print("========================================================")

    for i, ship in enumerate(final_results[:5], 1):
        print(f"{i}. MMSI: {ship['MMSI']} | DFSI Score: {ship['DFSI_Score']} "
              f"(Gaps: {ship['Max_Gap_Hours']}h, Jumps: {ship['Total_Impossible_Distance_NM']}nm, Draft Changes: {ship['Draft_Changes_Count']})")

    print(f"Total Execution Time: {execution_time:.6f} seconds")


if __name__ == "__main__":
    calculate_dfsi()