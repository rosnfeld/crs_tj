from django.conf.urls import patterns, url

from tj import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^queries$', views.queries_home, name='queries_home'),
    url(r'^query_combos$', views.query_combos_home, name='query_combos_home'),
)
