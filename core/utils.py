from concurrent.futures import ThreadPoolExecutor
from math import radians, sin, cos, sqrt, atan2
import geohash2
from core.constants import PROVIDER_MAP


def calculate_distance(lat1, lng1, lat2, lng2):
    if lat2 is None or lng2 is None:
        return 999999
    R = 6371000 #earth radius
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lat = radians(lat2 - lat1)
    delta_lng = radians(lng2 - lng1)
    a = sin(delta_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lng / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return int(distance)


def run_parallel(tasks):
    def safe_call(f):
        try:
            return f()
        except Exception:
            return []
    with ThreadPoolExecutor() as executor:
        return list(executor.map(safe_call, tasks))


def get_geohash(lat, lng):
    return geohash2.encode(lat, lng, precision=6)


def score(b):
    distance = b.get("distance", 0)
    rating = b.get("rating", 0)
    proximity_score = max(0,100 - (distance / 100))
    rating_score = rating * 20
    category_score = 100
    final_score= (proximity_score * 0.40) +(rating_score * 0.35) +(category_score * 0.25)
    return final_score


def get_providers(category: str):
    return PROVIDER_MAP.get(category, PROVIDER_MAP["generic"])
