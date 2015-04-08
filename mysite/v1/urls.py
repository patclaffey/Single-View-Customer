from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^za/$', views.svc_za, name='za'),
    url(r'^(?P<country_code>\w+)/count/$', views.svc_country, name='country'),
]