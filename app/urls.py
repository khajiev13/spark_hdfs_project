from django.contrib import admin
from django.urls import path, include
from django.conf.urls.static import static
from django.conf import settings

from app.views import hello_view, weather_render , holidays_render, season_render

urlpatterns = [
    path('', hello_view, name='main'),
    path('main', hello_view, name='main'),
    path('weather/<data>', weather_render, name='Roma'),
    path('holidays', holidays_render, name='hello'),
    path('seasons/<data>', season_render, name='hello'),
    # ... other URL patterns for your project
]
