import csv
import zlib
import folium
from pathlib import Path
import multiprocessing as mp

# Plots the vessel path over an interactive map


def plot_complete_vessel_history(target_mmsi, output_dir, CORES):
    # 1. Determine which shard to open
    mmsi_str = str(target_mmsi).strip()
    num_shards = CORES
    mmsi_hash = zlib.crc32(mmsi_str.encode("utf-8"))
    shard_id = mmsi_hash % num_shards

    shard_path = Path(output_dir) / f"final_shard_{shard_id}.csv"

    if not shard_path.exists():
        print(f"Error: Shard {shard_id} not found at {shard_path}")
        return

    # 2. Extract EVERY point for this MMSI
    # We assume Phase 2 already sorted these by timestamp
    points = []
    timestamps = []

    print(f"Reading all points for MMSI {mmsi_str} from Shard {shard_id}...")

    with open(shard_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("MMSI") == mmsi_str:
                try:
                    lat = float(row['Latitude'])
                    lon = float(row['Longitude'])
                    time = row.get('# Timestamp', 'N/A')
                    points.append((lat, lon))
                    timestamps.append(time)
                except (ValueError, KeyError):
                    continue

    if not points:
        print(f"No data found for MMSI {mmsi_str}.")
        return

    print(f"Found {len(points):,} total positions. Generating map...")

    # 3. Create Map (Centered on the vessel's average location)
    center_lat = sum(p[0] for p in points) / len(points)
    center_lon = sum(p[1] for p in points) / len(points)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles='CartoDB positron')

    # 4. Draw the continuous path (The Line)
    folium.PolyLine(
        points,
        color="#2980b9",
        weight=2,
        opacity=0.8,
        tooltip=f"Full Track: {mmsi_str}"
    ).add_to(m)

    # 5. Add individual points as tiny circles
    # This lets you see the "density" of the pings
    for i, coords in enumerate(points):
        folium.CircleMarker(
            location=coords,
            radius=2,
            color="#e74c3c",
            fill=True,
            fill_color="#e74c3c",
            fill_opacity=0.6,
            popup=f"Time: {timestamps[i]}<br>Lat: {coords[0]}<br>Lon: {coords[1]}"
        ).add_to(m)

    # 6. Mark Start and End with distinct icons
    folium.Marker(points[0], popup="First Recorded Position", icon=folium.Icon(color='green')).add_to(m)
    folium.Marker(points[-1], popup="Last Recorded Position", icon=folium.Icon(color='red')).add_to(m)

    # 7. Save
    output_name = f"full_history_mmsi_{mmsi_str}.html"
    m.save(output_name)
    print(f"Success! Created {output_name}")


if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent
    DATA_OUTPUT = BASE_DIR / "output"
    CORES = 10

    # ENTER THE MMSI YOU WANT TO ANALYZE
    TARGET = "212376000"

    plot_complete_vessel_history(TARGET, DATA_OUTPUT, CORES)