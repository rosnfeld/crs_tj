from django.conf.urls import patterns, url

from tj import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^queries$', views.queries_home, name='queries_home'),
    url(r'^query/create$', views.query_create, name='query_create'),
    url(r'^query/post$', views.query_post, name='query_post'),
    url(r'^query/(?P<query_id>\d+)/edit$', views.query_edit, name='query_edit'),
    url(r'^query/(?P<query_id>\d+)/run.json', views.query_run_json, name='query_run_json'),
    url(r'^query_combos$', views.query_combos_home, name='query_combos_home'),
)
