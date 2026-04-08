import csv
import zlib
import folium
from pathlib import Path
import multiprocessing as mp


def get_vessel_data(mmsi_str, output_dir):
    """Helper function to find and extract all points for a single MMSI."""
    num_shards = max(1, mp.cpu_count() - 1)
    mmsi_hash = zlib.crc32(mmsi_str.encode("utf-8"))
    shard_id = mmsi_hash % num_shards

    shard_path = Path(output_dir) / f"final_shard_{shard_id}.csv"
    points = []
    timestamps = []

    if not shard_path.exists():
        print(f"Error: Shard {shard_id} not found at {shard_path}")
        return points, timestamps

    print(f"Reading points for MMSI {mmsi_str} from Shard {shard_id}.")

    with open(shard_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("MMSI") == mmsi_str:
                lat = float(row['Latitude'])
                lon = float(row['Longitude'])
                time = row.get('# Timestamp', 'N/A')
                points.append((lat, lon))
                timestamps.append(time)

    return points, timestamps


def plot_two_vessels(mmsi_1, mmsi_2, output_dir):
    """Plots two vessels on the same map with distinct colors."""
    mmsi_1_str = str(mmsi_1).strip()
    mmsi_2_str = str(mmsi_2).strip()

    # Get data for both ships
    points_1, times_1 = get_vessel_data(mmsi_1_str, output_dir)
    points_2, times_2 = get_vessel_data(mmsi_2_str, output_dir)

    if not points_1 and not points_2:
        print("No data found for either vessel.")
        return

    # Find the geographic center combining BOTH ships' coordinates
    all_points = points_1 + points_2
    center_lat = sum(p[0] for p in all_points) / len(all_points)
    center_lon = sum(p[1] for p in all_points) / len(all_points)

    m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles='CartoDB positron')

    # Helper function to draw a ship's path to keep code clean
    def draw_ship(points, timestamps, mmsi, line_color, point_color, start_color, end_color):
        if not points:
            return

        # Draw the continuous path
        folium.PolyLine(
            points,
            color=line_color,
            weight=2,
            opacity=0.8,
            tooltip=f"Track: {mmsi}"
        ).add_to(m)

        # Draw individual pings
        for i, coords in enumerate(points):
            folium.CircleMarker(
                location=coords,
                radius=2,
                color=point_color,
                fill=True,
                fill_color=point_color,
                fill_opacity=0.6,
                popup=f"<b>MMSI:</b> {mmsi}<br><b>Time:</b> {timestamps[i]}<br><b>Lat:</b> {coords[0]}<br><b>Lon:</b> {coords[1]}"
            ).add_to(m)

        # Mark Start and End points
        folium.Marker(points[0], popup=f"{mmsi} Start", icon=folium.Icon(color=start_color)).add_to(m)
        folium.Marker(points[-1], popup=f"{mmsi} End", icon=folium.Icon(color=end_color)).add_to(m)

    # Draw Ship 1 (Blue Line, Red Dots)
    draw_ship(points_1, times_1, mmsi_1_str,
              line_color="#2980b9", point_color="#e74c3c",
              start_color="green", end_color="red")

    # Draw Ship 2 (Orange Line, Purple Dots)
    draw_ship(points_2, times_2, mmsi_2_str,
              line_color="#e67e22", point_color="#8e44ad",
              start_color="lightgreen", end_color="darkred")

    # Save the interactive map
    output_name = OUTPUT_DIR / f"anomaly_B_transfer_{mmsi_1_str}_and_{mmsi_2_str}.html"
    m.save(output_name)
    print(f"\nSuccess! Created {output_name.name}")


if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_DIR = BASE_DIR / "output"

    SHIP_1_MMSI = "219028635"
    SHIP_2_MMSI = "219005347"

    plot_two_vessels(SHIP_1_MMSI, SHIP_2_MMSI, OUTPUT_DIR)