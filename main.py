import subprocess
import time
import sys


CORES = 10  # Number of CPU cores to allocate to the workers
CHUNK_SIZE = 100_000  # Max rows to hold in RAM during sorting phases

SCRIPTS = [
    "Split_Filter_Sort.py", # Initial splitting and sorting
    "Anomaly_A.py",  # Gaps
    "Anomaly_B_loitering_search.py",  # Loitering Extractor
    "Anomaly_B.py",  # Sweep-line Spatial Join
    "Anomaly_C.py",  # Draft Changes
    "Anomaly_D.py",  # Teleportation
    "DFSI.py"  # Final math (single core)
]

if __name__ == "__main__":
    print(f"INITIATING ORCHESTRATOR: {CORES} Cores | {CHUNK_SIZE} Chunk Size\n")
    master_start = time.perf_counter()

    for script in SCRIPTS:
        command = [
            sys.executable, script,
            "--cores", str(CORES),
            "--chunk_size", str(CHUNK_SIZE)
        ]

        result = subprocess.run(command)

        # Safety check: If a script crashes, stop the whole pipeline
        if result.returncode != 0:
            print(f"\n {script} crashed.")
            sys.exit(1)

        print("\n")

    master_end = time.perf_counter()
    print(f"Total pipeline run time: {master_end - master_start:.2f} seconds.")