from django.urls import path, include
from .views import discovery_view

urlpatterns = [
    path("", discovery_view)

]