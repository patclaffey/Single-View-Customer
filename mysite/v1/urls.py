from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^(?P<country_code>\w+)/count/$', views.svc_country, name='country'),
    url(r'^(?P<country_code>\w+)/msisdn/(?P<msisdn_id>\w+)/$',views.svc_country_msisdn, name='msisdn'),
    url(r'^(?P<country_code>\w+)/profile/(?P<profile_id>\d+)/$', views.svc_country_profile, name='profile'),
    url(r'^(?P<country_code>\w+)/summary/$', views.svc_country_summary, name='country_summary'),
]
