import uuid
from datetime import datetime, timedelta
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

    radius_km = payload.get(
        "radius_km",
        settings.DISCOVERY_RADIUS_DEFAULT_KM
    )
#Looks up which providers to call based on the category
    tasks = build_provider_tasks(
    category=category,
    lat=lat,
    lng=lng,
    radius_km=radius_km,
)
    #Calls each provider's API in parallel (not sequential — we want speed)
    results = run_parallel(tasks)


    businesses = [b for group in results for b in group]

# Deduplicates across providers (same place showing up on Yelp and Tripadvisor = keep one)
    deduped = {}

    for business in businesses:
        key = joining_name_address(business)

        if key not in deduped:
            deduped[key] = business
        else:
            if business.get("rating", 0) > deduped[key].get("rating", 0):
                deduped[key] = business

    unique = list(deduped.values())



    #Re-ranks by: proximity (40%) + time fit (35%) + category match (25%)
    ranked = sorted(unique, key=score, reverse=True)

    #Takes the top 5
    top = ranked[:settings.DISCOVERY_MAX_SUGGESTIONS]

    suggestions = []

    for b in top:
        if not b.get("image_url"):
            continue

        suggestions.append({
            "provider": b.get("provider"),
            "external_id": f"{b.get('provider')}_{b['id']}",
            "title": b.get("name"),
            "category":b.get("categories"),
            "rating": b.get("rating"),
            "price_range":b.get("price"),
            "distance_m": int(b.get("distance", 0)),
            "image_url": b.get("image_url"),
            "url": b.get("url"),
        })

    if not suggestions:
        return []


    now = datetime.now()
    expires = now + timedelta(
        hours=settings.DISCOVERY_POST_EXPIRY_HOURS
    )

    return [{
        "id": f"disc_{uuid.uuid4()}",
        "type": "discovery",
        "created_at": now.isoformat(),
        "author": "system_user_musey",
        "location": {
            "lat": lat,
            "lng": lng,
            "label": label
        },
        "headline": "Some great spots near you right now 📍",
        "image_url": suggestions[0]["image_url"],
        "suggestions": suggestions,
        "expires_at": expires.isoformat()
    }]