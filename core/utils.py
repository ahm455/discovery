from concurrent.futures import ThreadPoolExecutor
from math import radians, sin, cos, sqrt, atan2
import geohash2
import requests
from datetime import datetime, timedelta
from core.constants import PROVIDER_MAP
from django.core.cache import cache


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

def reverse_geocode(lat, lng):
    url = "https://nominatim.openstreetmap.org/reverse"

    params = {
        "lat": lat,
        "lon": lng,
        "format": "json",
        "addressdetails": 1
    }

    headers = {
        "User-Agent": "Discovery"
    }

    try:
        res = requests.get(url, params=params, headers=headers, timeout=5)

        if res.status_code != 200:
            return None

        data = res.json()
        address = data.get("address", {})

        return (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("state_district")
            or address.get("state")
        )

    except requests.RequestException:
        return None


def get_next_available_date(days_ahead=1):
    target_date = datetime.utcnow() + timedelta(days=days_ahead)
    return target_date.strftime("%Y-%m-%d")

def geocode_address(address: str):
    if not address:
        return None

    cache_key = f"geocode:{address.lower().strip()}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    url = "https://nominatim.openstreetmap.org/search"

    params = {
        "q": address,
        "format": "json",
        "limit": 1
    }

    headers = {
        "User-Agent": "Discovery"
    }

    try:
        res = requests.get(url, params=params, headers=headers, timeout=5)

        if res.status_code != 200:
            return None

        data = res.json()

        if not data:
            return None

        result = data[0]

        lat = result.get("lat")
        lng = result.get("lon")

        if not lat or not lng:
            return None

        coords = (float(lat), float(lng))

        cache.set(cache_key, coords, timeout=60 * 60 * 24 * 7)

        return coords

    except requests.RequestException:
        return None