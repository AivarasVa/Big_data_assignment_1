import subprocess
import time
import sys

# ==========================================
# ⚙️ GLOBAL PIPELINE PARAMETERS
# ==========================================
CORES = 10  # Number of CPU cores to allocate to the workers
CHUNK_SIZE = 100000  # Max rows to hold in RAM during sorting phases

# List your scripts in the exact order they need to run
SCRIPTS = [
    "Split_Filter_Sort.py",  # Your phase 1 sharding script
    "Anomaly_A.py",  # Gaps
    "Anomaly_B_loitering_search.py",  # Loitering Extractor
    "Anomaly_B.py",  # Sweep-line Spatial Join
    "Anomaly_C.py",  # Draft Changes
    "Anomaly_D.py",  # Teleportation
    "DFSI.py"  # Final math (single core)
]

if __name__ == "__main__":
    print(f"🚀 INITIATING ORCHESTRATOR: {CORES} Cores | {CHUNK_SIZE} Chunk Size\n")
    master_start = time.perf_counter()

    for script in SCRIPTS:
        print(f"=====================================================")
        print(f"▶️ LAUNCHING: {script}")
        print(f"=====================================================")

        # Build the command line instruction
        # e.g., python Task_2_Anomaly_A.py --cores 4 --chunk_size 100000
        command = [
            sys.executable, script,
            "--cores", str(CORES),
            "--chunk_size", str(CHUNK_SIZE)
        ]

        # Run the script and wait for it to finish.
        # Output will print directly to your console in real-time.
        result = subprocess.run(command)

        # Safety check: If a script crashes, stop the whole pipeline
        if result.returncode != 0:
            print(f"\n❌ FATAL ERROR: {script} crashed. Halting pipeline.")
            sys.exit(1)

        print("\n")  # Add a little space between script outputs

    master_end = time.perf_counter()
    print(f"🎉 ENTIRE PIPELINE COMPLETE! Total Time: {master_end - master_start:.2f} seconds.")