import json

from django.http import HttpRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import services


@csrf_exempt  # an API endpoint for the demo; use real auth in a real app
@require_POST
def place_order(request: HttpRequest) -> JsonResponse:
    body = json.loads(request.body)
    order = services.place_order(body["email"], int(body["total_cents"]))
    return JsonResponse({"id": order.pk}, status=201)
