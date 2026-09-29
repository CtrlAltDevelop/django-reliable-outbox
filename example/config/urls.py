from django.contrib import admin
from django.urls import path
from shop.views import place_order

urlpatterns = [
    path("admin/", admin.site.urls),
    path("orders/", place_order),
]
