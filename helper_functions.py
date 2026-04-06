import math
from datetime import datetime


def calculate_maritime_distance(lat1, lon1, lat2, lon2):
    """Calculates the great-circle distance in Nautical Miles."""
    radius_nm = 3440.065
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_nm * c


def parse_timestamp(time_str):
    """Converts the AIS timestamp string to a datetime object."""
    try:
        return datetime.strptime(time_str.strip(), "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None