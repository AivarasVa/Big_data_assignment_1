# AIS Maritime Anomaly Detection System

A high-performance big data analysis pipeline for detecting suspicious vessel behavior patterns in AIS (Automatic Identification System) maritime tracking data.

## 📋 Overview

This system processes large-scale AIS datasets to identify vessels exhibiting anomalous behavior that may indicate illegal activities such as smuggling, sanctions evasion, or identity spoofing. The pipeline employs parallel processing and memory-efficient algorithms to handle datasets containing millions of position reports.

## 🎯 Detected Anomalies

### Anomaly A: Communication Gaps
Detects vessels that go silent (stop transmitting AIS signals) for extended periods (>4 hours) while moving significant distances (>1 NM), which may indicate deliberate signal deactivation.

### Anomaly B: Loitering Behavior
Identifies vessels that remain stationary or move within a small area for prolonged periods, potentially indicating:
- Illegal cargo transfers at sea
- Waiting for smuggling operations
- Surveillance activities

### Anomaly C: Draft Changes During Blackouts
Detects suspicious draft (water displacement) changes (>5%) during communication gaps (>2 hours), suggesting cargo loading/unloading while AIS is disabled.

### Anomaly D: Teleportation / MMSI Cloning
Identifies impossible speeds (>60 knots) between consecutive position reports, indicating:
- MMSI (Maritime Mobile Service Identity) spoofing
- Multiple vessels using the same identifier
- GPS coordinate manipulation

## 🏗️ Architecture

The pipeline consists of modular, sequential stages:

```
Data Input → Split/Filter/Sort → Anomaly Detection → DFSI Calculation → Results
```

### Pipeline Components

1. **Split_Filter_Sort.py**: Data preprocessing and sharding
   - Validates MMSI identifiers and coordinates
   - Filters invalid/test data
   - Shards data by MMSI for parallel processing
   - Sorts data chronologically per vessel

2. **Anomaly Detection Scripts** (A, B, C, D):
   - Process shards in parallel using multiprocessing
   - Stream vessel histories using memory-efficient iterators
   - Output suspicious events to CSV files

3. **DFSI.py**: Deviation From Safe Intervals scoring
   - Aggregates anomaly metrics per vessel
   - Calculates composite suspicion scores
   - Ranks vessels by risk level

## 🚀 Usage

### Quick Start

```bash
python main.py
```

The orchestrator will automatically execute all pipeline stages in sequence.

### Configuration

Edit `main.py` to adjust performance parameters:

```python
CORES = 10          # CPU cores to allocate
CHUNK_SIZE = 100000 # Rows per memory chunk
```

### Requirements

- Python 3.7+
- pandas
- Standard library modules (csv, multiprocessing, itertools, pathlib, etc.)

### Input Data Format

Place AIS CSV files in the `data/` directory with the following columns:
- MMSI (vessel identifier)
- Latitude / Longitude
- Timestamp (format: `dd/mm/yyyy HH:MM:SS`)
- Draught (for Anomaly C)
- Status (vessel operational status)

## 📁 Project Structure

```
Big_data_assignment_1/
├── main.py                          # Pipeline orchestrator
├── Split_Filter_Sort.py             # Data preprocessing
├── Anomaly_A.py                     # Gap detection
├── Anomaly_B_loitering_search.py    # Loitering event extraction
├── Anomaly_B.py                     # Loitering spatial analysis
├── Anomaly_C.py                     # Draft change detection
├── Anomaly_D.py                     # Teleportation detection
├── DFSI.py                          # Final scoring system
├── Plot.py                          # Visualization utilities
├── plot_transfer.py                 # Transfer plotting
├── data/                            # Input AIS datasets
│   ├── aisdk-2024-03-01.csv
│   └── aisdk-2024-03-02.csv
└── output/                          # Generated shards and results
    ├── final_shard_*.csv            # Processed vessel data
    ├── anomaly_A_results.csv
    ├── anomaly_B_results.csv
    ├── anomaly_C_results.csv
    ├── anomaly_D_results.csv
    └── dfsi_scores.csv              # Final vessel rankings
```

## ⚡ Performance Features

- **Parallel Processing**: Utilizes multiprocessing to distribute analysis across CPU cores
- **Memory Efficiency**: Streams data using itertools.groupby to process one vessel at a time
- **External Merge Sort**: Handles files larger than RAM using disk-based sorting
- **Incremental Processing**: Pipeline stages can be run independently for debugging

## 📊 Output

Results are stored in the `output/` directory:

- **anomaly_[A-D]_results.csv**: Individual anomaly detections with timestamps, coordinates, and metrics
- **dfsi_scores.csv**: Ranked list of suspicious vessels with composite risk scores

## 🔧 Technical Details

### Maritime Distance Calculation
Uses the Haversine formula with Earth radius = 3440.065 NM for accurate great-circle distances.

### Data Validation
- MMSI must be exactly 9 digits
- Rejects test/invalid identifiers (111111111, 123456789, etc.)
- Filters coordinates outside valid ranges (±90° lat, ±180° lon)
- Removes zero coordinates and missing data

### Thresholds
All detection thresholds are configurable in individual anomaly scripts:
- Gap threshold: 4 hours
- Movement threshold: 1 NM
- Speed threshold: 60 knots
- Draft change: 5%
- Loitering duration: Configurable per analysis

## 📝 License

Academic project for Big Data Analysis coursework.

## 👥 Authors

Master's Degree Program - Big Data Analysis (Semester 2)