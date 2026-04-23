import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .service import build_discovery

@csrf_exempt
def discovery_view(request):
    if request.method != "POST":
        return JsonResponse([], safe=False)

    try:
        payload = json.loads(request.body)
    except:
        return JsonResponse([], safe=False)

    return JsonResponse(build_discovery(payload), safe=False)