import uuid
from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor
import requests
from django.conf import settings
from .constants import PROVIDER_MAP
import geohash2
from django.core.cache import cache


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


def fetch_yelp(lat, lng, radius_km, term="restaurants"):

    geohash = get_geohash(lat, lng)
    cache_key = f"disc:provider:cache:yelp:{geohash}:{term}:{radius_km}"
    cached = cache.get(cache_key)

    if cached is not None:
        return cached

    headers = {"Authorization": f"Bearer {settings.YELP_API_KEY}"}

    params = {"latitude": lat,"longitude": lng,"radius": int(radius_km * 1000),"term": term,}

    YELP_URL = "https://api.yelp.com/v3/businesses/search"

    res = requests.get(YELP_URL,headers=headers,params=params,timeout=5)

    if res.status_code != 200:
        return []

    data = res.json().get("businesses", [])

    for b in data:
        b["provider"] = "yelp"

    cache.set(cache_key,data,timeout=settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS)

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

def fetch_partiful(lat, lng, radius_km):
    print("fetching partiful")
    return [] #no api available

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
        tasks.append(lambda: fetch_partiful(lat, lng, radius_km))

    if "kayak" in providers:
        tasks.append(lambda: fetch_kayak(lat, lng, radius_km))

    return tasks

def build_post(lat, lng, label, top):

    suggestions = []

    for b in top:
        if not b.get("image_url"):
            continue

        suggestions.append({
            "provider": b.get("provider"),
            "external_id": f"{b.get('provider')}_{b['id']}",
            "title": b.get("name"),
            "category": b.get("categories"),
            "rating": b.get("rating"),
            "price_range": b.get("price"),
            "distance_m": int(b.get("distance", 0)),
            "image_url": b.get("image_url"),
            "url": b.get("url"),
        })

    if not suggestions:
        return []
    now = datetime.now()

    return [{
        "id": f"disc_{uuid.uuid4()}",
        "type": "discovery",
        "created_at": now,
        "author": "system_user_musey",
        "location": {
            "lat": lat,
            "lng": lng,
            "label": label
        },
        "headline": "Some great spots near you right now 📍",
        "image_url": suggestions[0]["image_url"],
        "suggestions": suggestions,
        "expires_at": (now + timedelta(hours=settings.DISCOVERY_POST_EXPIRY_HOURS)).isoformat()
    }]

def deduplicate_businesses(businesses):
    deduped = {}

    for b in businesses:
        if not b.get("name"):
            continue

        key = joining_name_address(b)

        if key not in deduped:
            deduped[key] = b
        else:
            if b.get("rating", 0) > deduped[key].get("rating", 0):
                deduped[key] = b

    return list(deduped.values())

def joining_name_address(b):
    name = b.get("name", "").lower().strip()

    for word in ["restaurant", "cafe", "branch"]:
        name = name.replace(word, "")

    address_list = b.get("location", {}).get("display_address", [])
    address = " ".join(address_list).lower().strip()

    return name + "|" + address
