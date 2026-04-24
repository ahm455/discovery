from django.conf import settings
from .tasks import *


def build_discovery(payload):
    if not settings.DISCOVERY_ENABLED:
        return []

    category = payload.get("category")
    loc = payload["location"]
    lat = loc["lat"]
    lng = loc["lng"]
    label = loc.get("label", "")

    radius_km = payload.get("radius_km",settings.DISCOVERY_RADIUS_DEFAULT_KM)

    #Looks up which providers to call based on the category
    tasks = build_provider_tasks(category=category,lat=lat,lng=lng,radius_km=radius_km,)

    #Calls each provider's API in parallel (not sequential — we want speed)
    results = run_parallel(tasks)

    businesses = [b for group in results for b in group]

    # Deduplicates across providers (same place showing up on Yelp and Tripadvisor = keep one)
    unique = deduplicate_businesses(businesses)

    #Re-ranks by: proximity (40%) + time fit (35%) + category match (25%)
    ranked = sorted(unique, key=score, reverse=True)

    #Takes the top 5
    top = ranked[:settings.DISCOVERY_MAX_SUGGESTIONS]

    #build required post
    return build_post(lat, lng, label, top)