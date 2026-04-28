import uuid
from datetime import timedelta, datetime
from .utils import *
import requests
from django.conf import settings
from django.core.cache import cache

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


def fetch_opentable(lat,lng,radius_km,time: str = "19:00",party_size:int= 2):

    if not settings.OPENTABLE_API_KEY:
        return []

    geohash = get_geohash(lat, lng)
    date = get_next_available_date()
    cache_key = f"opentable:{geohash}:{radius_km}:{date}:{time}:{party_size}"

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    city_cache_key = f"city:{geohash}"
    city_name = cache.get(city_cache_key)

    if not city_name:
        city_name = reverse_geocode(lat, lng)
        if city_name:
            cache.set(city_cache_key, city_name, 600000)

    if not city_name:
        return []

    url = "https://platform.opentable.com/restaurants/search"

    headers = {
        "Authorization": f"Bearer {settings.OPENTABLE_API_KEY}",
        "Accept": "application/json",
        "X-Platform-Version": "1.0"
    }

    params = {
        "city": city_name,
        "date": date,
        "time": time,
        "party_size": party_size,
        "lat": lat,
        "lng": lng,
        "radius": radius_km,
        "limit": 50
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            return []

        data = response.json()

        restaurants = data.get("restaurants", data.get("data", []))
        results = []

        for restaurant in restaurants:
            name = restaurant.get("name")
            if not name:
                continue


            rest_lat = restaurant.get("latitude") or restaurant.get("lat")
            rest_lng = restaurant.get("longitude") or restaurant.get("lng")


            if rest_lat is None or rest_lng is None:
                address = restaurant.get("address", {}).get("street_address", "")
                if address:
                    coords = geocode_address(address)
                    if coords:
                        rest_lat, rest_lng = coords
                    else:
                        continue
                else:
                    continue

            distance = calculate_distance(lat, lng, rest_lat, rest_lng)


            if distance > radius_km * 1000:
                continue


            rating = restaurant.get("rating")
            try:
                rating = float(rating) if rating else None
            except (ValueError, TypeError):
                rating = None

            image_url = (
                    restaurant.get("image_url") or
                    restaurant.get("hero_image_url") or
                    restaurant.get("photo_url") or
                    restaurant.get("image"))

            price = restaurant.get("price_range") or restaurant.get("price_level")
            if isinstance(price, dict):
                price = price.get("display")


            address_obj = restaurant.get("address", {})
            address_parts = [
                address_obj.get("street_address", ""),
                address_obj.get("locality", ""),
                address_obj.get("region", ""),
                address_obj.get("postal_code", "")
            ]
            display_address = ", ".join([p for p in address_parts if p])

            result = {
                "provider": "opentable",
                "id": str(restaurant.get("id", restaurant.get("restaurant_id", name))),
                "name": name,
                "rating": rating,
                "price": price,
                "image_url": image_url,
                "url": restaurant.get("url") or restaurant.get("reservation_url"),
                "distance": int(distance),
                "lat": rest_lat,
                "lng": rest_lng,
                "categories": restaurant.get("categories", ["restaurant"]),
                "location": {
                    "display_address": [display_address] if display_address else [restaurant.get("address_string", "")]
                },
                "availability": restaurant.get("is_bookable", True),
                "cuisine": restaurant.get("cuisine_type") or restaurant.get("cuisine")
            }

            results.append(result)

        cache.set(cache_key, results, settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS)

        return results

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return []


def fetch_tripadvisor(lat, lng, radius_km):
    geohash = get_geohash(lat, lng)
    cache_key = f"disc:provider:cache:tripadvisor:{geohash}:{radius_km}"

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    base_url = "https://api.content.tripadvisor.com/api/v1/location/nearby_search"

    params = {
        "latLong": f"{lat},{lng}",
        "category": "restaurants",
        "radius": int(radius_km * 1000),
        "radiusUnit": "m",
        "key": settings.TRIPADVISOR_API_KEY,
    }

    try:
        response = requests.get(base_url, params=params, timeout=5)

        if response.status_code != 200:
            return []

        data = response.json()
        results = []

        for item in data.get("data", []):
            location_id = item.get("location_id")
            name = item.get("name")

            if not location_id or not name:
                continue

            detail_url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/details"
            detail_params = {"key": settings.TRIPADVISOR_API_KEY}

            try:
                detail_response = requests.get(detail_url, params=detail_params, timeout=3)

                if detail_response.status_code != 200:
                    continue

                detail = detail_response.json()

                item_lat = detail.get("latitude")
                item_lng = detail.get("longitude")

                if not item_lat or not item_lng:
                    continue

                distance = calculate_distance(lat, lng, item_lat, item_lng)

                if distance > radius_km * 1000:
                    continue

                rating = detail.get("rating")
                rating = float(rating) if rating else None

                price_level = detail.get("price_level")
                if isinstance(price_level, int):
                    price = "$" * price_level
                elif price_level:
                    price = str(price_level)
                else:
                    price = None

                photos = detail.get("photos", [])
                image_url = None
                if photos:
                    image_url = photos[0].get("images", {}).get("large", {}).get("url")

                address_obj = detail.get("address", {})
                display_address = []

                if address_obj.get("address_string"):
                    display_address = [address_obj.get("address_string")]
                else:
                    parts = [
                        address_obj.get("street1"),
                        address_obj.get("locality"),
                        address_obj.get("country"),
                    ]
                    display_address = [p for p in parts if p]

                results.append({
                    "provider": "tripadvisor",
                    "id": str(location_id),
                    "name": name,
                    "rating": rating,
                    "price": price,
                    "image_url": image_url,
                    "url": detail.get(
                        "web_url",
                        f"https://www.tripadvisor.com/Restaurant_Review-g{location_id}"
                    ),
                    "distance": distance,
                    "lat": item_lat,
                    "lng": item_lng,
                    "categories": ["restaurant"],
                    "location": {
                        "display_address": display_address
                    }
                })

            except requests.RequestException:
                continue

        cache.set(cache_key, results, timeout=settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS)
        return results

    except requests.RequestException:
        return []

def fetch_eventbrite(lat, lng, radius_km):

    geohash = get_geohash(lat, lng)
    cache_key = f"disc:provider:cache:eventbrite:{geohash}:{radius_km}"
    cached = cache.get(cache_key)

    if cached is not None:
        return cached

    url = "https://www.eventbriteapi.com/v3/events/search/"
    params = {
        "location.latitude": lat,
        "location.longitude": lng,
        "location.within": f"{radius_km}km",
        "expand": "venue",
    }
    headers = {"Authorization": f"Bearer {settings.EVENTBRITE_TOKEN}"}

    try:
        res = requests.get(url,headers=headers,params=params,timeout=5)


        if res.status_code != 200:
            return []

        data = res.json()
        events = data.get("events", [])

        results=[]
        for event in events:

            venue = event.get("venue", {})
            venue_lat = venue.get("latitude")
            venue_lng = venue.get("longitude")

            if not venue_lat or not venue_lng:
                continue


            image_url = None
            logo = event.get("logo")
            if logo:
                image_url = logo.get("url")

            if not image_url:
                continue

            distance = calculate_distance(lat, lng, venue_lat, venue_lng)

            if distance > radius_km * 1000:
                continue

            name_obj = event.get("name", {})
            name = name_obj.get("text") if isinstance(name_obj, dict) else str(name_obj)

            results.append({
                "provider": "eventbrite",
                "id": event.get("id"),
                "name": name,
                "rating": None,  # Eventbrite has no rating
                "price": None,  # Eventbrite has no price
                "image_url": image_url,
                "url": event.get("url"),
                "distance": distance,
                "lat": venue_lat,
                "lng": venue_lng,
                "categories": ["event"],
                "location": {
                    "display_address": [
                        venue.get("address", {}).get("localized_address_display", "")
                    ]
                }
            })
        cache.set(cache_key, results, timeout=settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS)
        return results

    except requests.RequestException:
        return []

def fetch_viator(lat, lng, radius_km):
    geohash = get_geohash(lat, lng)
    cache_key = f"disc:provider:cache:viator:{geohash}:{radius_km}"

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    headers = {"exp-api-key": settings.VIATOR_API_KEY}


    dest_url = "https://api.viator.com/partner/destinations/nearby"
    dest_params = {
        "lat": lat,
        "lng": lng,
        "radius": radius_km
    }

    try:
        dest_response = requests.get(dest_url, headers=headers, params=dest_params, timeout=5)
        if dest_response.status_code != 200:
            return []

        destinations = dest_response.json().get("destinations", [])
        if not destinations:
            return []

        destination_id = destinations[0].get("id")

    except requests.RequestException:
        return []


    search_url = "https://api.viator.com/partner/search/products"

    search_payload = {
        "filtering": {"destination": str(destination_id)},
        "sorting": {"sort": "TRAVELER_RATING", "order": "DESCENDING"},
        "pagination": {"start": 1, "count": 10},
        "currency": "USD"
    }

    try:
        search_response = requests.post(
            search_url,
            headers=headers,
            json=search_payload,
            timeout=5
        )

        if search_response.status_code != 200:
            return []

        products = search_response.json().get("products", [])

    except requests.RequestException:
        return []

    results = []

    for product in products[:10]:
        product_code = product.get("productCode")
        if not product_code:
            continue

        detail_cache_key = f"viator:product:{product_code}"
        detail = cache.get(detail_cache_key)

        if detail is None:
            detail_url = f"https://api.viator.com/partner/product/{product_code}"

            try:
                detail_response = requests.get(detail_url, headers=headers, timeout=5)
                if detail_response.status_code != 200:
                    continue

                detail = detail_response.json()
                cache.set(detail_cache_key, detail, timeout=86400)  # 24h cache

            except requests.RequestException:
                continue

        images = detail.get("images", [])
        if not images:
            continue

        location = detail.get("location", {})
        dest_lat = location.get("latitude")
        dest_lng = location.get("longitude")


        if not dest_lat or not dest_lng:
            continue

        distance = calculate_distance(lat, lng, dest_lat, dest_lng)


        if distance > radius_km * 1000:
            continue

        rating = 0
        reviews = detail.get("reviews")
        if reviews:
            rating = reviews.get("averageRating")
        from_price = detail.get("fromPrice", {})
        price_amount = from_price.get("amount")

        results.append({
            "provider": "viator",
            "id": product_code,
            "name": detail.get("title"),
            "rating": float(rating) if rating else None,
            "price": f"${price_amount}" if price_amount else None,
            "image_url": images[0].get("url"),
            "url": detail.get("productUrl") or f"https://www.viator.com/tours/{product_code}",
            "distance": distance,
            "lat": dest_lat,
            "lng": dest_lng,
            "categories": [
                detail.get("category", {}).get("name", "tour")
            ],
            "location": {
                "display_address": [
                    location.get("address", ""),
                    location.get("city", ""),
                    location.get("country", "")
                ]
            }
        })

    cache.set(cache_key, results, timeout=settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS)
    return results

def fetch_luma(lat, lng, radius_km):
    geohash = get_geohash(lat, lng)
    cache_key = f"disc:provider:cache:luma:{geohash}:{radius_km}"
    cached = cache.get(cache_key)

    if cached is not None:
        return cached

    BASE_URL = "https://public-api.luma.com/v1/calendar/list-events"

    params = {"lat": lat,"lng": lng,"radius": radius_km,}

    headers = {"Authorization": f"Bearer {settings.LUMA_API_KEY}"}

    try:
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=5)
    except requests.RequestException:
        return []

    if response.status_code != 200:
        return []

    data = response.json()

    results = []

    for event in data.get("events", []):
        location = event.get("location", {})
        event_lat = location.get("lat")
        event_lng = location.get("lng")

        if not event_lat or not event_lng:
            continue

        image_url = None
        cover = event.get("cover_image")
        if cover:
            image_url = cover.get("original_url")

        if not image_url:
            continue

        distance = calculate_distance(lat, lng, event_lat, event_lng)

        results.append({
            "provider": "luma",
            "id": event.get("id"),
            "name": event.get("title"),
            "rating": None,  # Luma has no rating
            "price": None,  # Luma has no price
            "image_url": image_url,
            "url": event.get("url"),
            "distance": distance,
            "lat": event_lat,
            "lng": event_lng,
            "categories": [event.get("category", "event")],
            "location": {
                "display_address": [
                    location.get("address", ""),
                    location.get("city", ""),
                    location.get("country", "")
                ]
            }
        })

    cache.set(cache_key, results, timeout=settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS)
    return results

def fetch_partiful(lat, lng, radius_km): # no api available
    print("fetching partiful")
    return []

def fetch_kayak(lat, lng, radius_km):
    print("fetching kayak")

    geohash = get_geohash(lat, lng)
    destination= reverse_geocode(lat, lng)
    if not destination:
        return []
    checkin=datetime.now().strftime("%Y-%m-%d")
    checkout=(datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")
    rooms = "1|2"

    cache_key = f"disc:provider:cache:kayak:{geohash}:{destination}:{checkin}:{checkout}:{radius_km}"

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    BASE_URL = "https://sandbox-en-us.kayakaffiliates.com/api/3.0/hotels"

    user_track_id = str(uuid.uuid4())

    params = {
        "apiKey": settings.KAYAK_API_KEY,
        "userTrackId": user_track_id,
        "destination": destination,
        "checkin": checkin,
        "checkout": checkout,
        "rooms": rooms,
        "pageSize": 50,
        "pageIndex": 0,
        "summaryOnly": False,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)

        if response.status_code != 200:
            return []

        data = response.json()

        results = []

        for hotel in data.get("results", []):
            name = hotel.get("name")
            if not name:
                continue

            lat_h = hotel.get("latitude")
            lng_h = hotel.get("longitude")

            if lat_h is None or lng_h is None:
                continue

            distance = calculate_distance(lat, lng, lat_h, lng_h)

            if distance > radius_km * 1000:
                continue

            image = None
            images = hotel.get("images", [])
            if images:
                image = images[0].get("large") or images[0].get("small")

            rating = hotel.get("guestRating")

            price = hotel.get("lowestRate")

            results.append({
                "provider": "kayak",
                "id": str(hotel.get("id")),
                "name": name,
                "rating": rating,
                "price": price,
                "image_url": image,
                "url": hotel.get("href"),
                "distance": int(distance),
                "lat": lat_h,
                "lng": lng_h,
                "categories": ["hotel"],
                "location": {
                    "display_address": [
                        hotel.get("address", "")
                    ]
                }
            })

        cache.set(cache_key, results, timeout=settings.DISCOVERY_PROVIDER_CACHE_TTL_SECONDS)
        return results

    except requests.RequestException as e:
        print(f"Kayak request failed: {e}")
        return []

def build_provider_tasks(category, lat, lng, radius_km):
    providers = get_providers(category)
    tasks = []

    if "yelp" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_yelp(l, n, r))

    if "tripadvisor" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_tripadvisor(l, n, r))

    if "opentable" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_opentable(l, n, r))

    if "eventbrite" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_eventbrite(l, n, r))

    if "viator" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_viator(l, n, r))

    if "luma" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_luma(l, n, r))

    if "partiful" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_partiful(l, n, r))

    if "kayak" in providers:
        tasks.append(lambda l=lat, n=lng, r=radius_km: fetch_kayak(l, n, r))

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
            "rating": b.get("rating",None),
            "price_range": b.get("price",None),
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
