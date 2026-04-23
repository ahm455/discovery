from concurrent.futures import ThreadPoolExecutor
import time
import requests
from django.conf import settings
from .constants import PROVIDER_MAP
import geohash2
from django.core.cache import cache


def run_parallel(tasks):
    with ThreadPoolExecutor() as executor:
        return list(executor.map(lambda f: f(), tasks))

def get_geohash(lat, lng):
    return geohash2.encode(lat, lng, precision=6)


def score(b):
    distance = b.get("distance", 0)
    rating = b.get("rating", 0)

    proximity_score = (100 - (distance / 100))
    rating_score = rating * 20
    category_score = 100

    return (
        proximity_score * 0.40 +
        rating_score * 0.35 +
        category_score * 0.25
    )

def get_providers(category: str):
    return PROVIDER_MAP.get(category, PROVIDER_MAP["generic"])


def fetch_yelp(lat, lng, radius_km, term="restaurants"):
    geohash = geohash2.encode(lat, lng, precision=6)

    cache_key = f"disc:provider:cache:yelp:{geohash}:{term}"

    cached = cache.get(cache_key)

    if cached is not None:
        return cached

    headers = {
        "Authorization": f"Bearer {settings.YELP_API_KEY}"
    }

    params = {
        "latitude": lat,
        "longitude": lng,
        "radius": int(radius_km * 1000),
        "term": term,
    }
    YELP_URL = "https://api.yelp.com/v3/businesses/search"

    res = requests.get(
        YELP_URL,
        headers=headers,
        params=params,
        timeout=5
    )
    if res.status_code != 200:
        return []

    data = res.json().get("businesses", [])

    cache.set(
        cache_key,
        data,
        timeout=settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS
    )

    return data

def fetch_opentable(lat, lng, radius_km):
    print("fetching opentable")
    return[]

def fetch_tripadvisor(lat, lng, radius_km):
    print("fetching tripadvisor")
    return []

def fetch_eventbrite(lat, lng, radius_km):
    print("fetching eventbrite")
    return []

def fetch_viator(lat, lng, radius_km):
    print("fetching viator")
    return []

def fetch_luma(lat, lng, radius_km):
    print("fetching luma")
    return []

def fetch_partyful(lat, lng, radius_km):
    print("fetching partyful")
    return []

def fetch_kayak(lat, lng, radius_km):
    print("fetching kayak")
    return []

def build_provider_tasks(category, lat, lng, radius_km):
    providers = get_providers(category)

    tasks = []

    if "yelp" in providers:
        tasks.append(lambda: fetch_yelp(lat,lng,radius_km))

    if "tripadvisor" in providers:
        tasks.append(lambda: fetch_tripadvisor(lat, lng, radius_km))

    if "opentable" in providers:
        tasks.append(lambda: fetch_opentable(lat, lng, radius_km))

    if "eventbrite" in providers:
        tasks.append(lambda: fetch_eventbrite(lat, lng, radius_km))

    if "viator" in providers:
        tasks.append(lambda: fetch_viator(lat, lng, radius_km))

    if "luma" in providers:
        tasks.append(lambda: fetch_luma(lat, lng, radius_km))

    if "partiful" in providers:
        tasks.append(lambda: fetch_partyful(lat, lng, radius_km))

    if "kayak" in providers:
        tasks.append(lambda: fetch_kayak(lat, lng, radius_km))

    return tasks

