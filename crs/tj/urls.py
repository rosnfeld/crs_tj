from django.conf.urls import patterns, url

from tj import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^queries$', views.queries_home, name='queries_home'),
    url(r'^query/create$', views.query_create, name='query_create'),
    url(r'^query/process$', views.query_process, name='query_process'),
    url(r'^query/(?P<query_id>\d+)/edit$', views.query_edit, name='query_edit'),
    url(r'^query_combos$', views.query_combos_home, name='query_combos_home'),
)
