from django.conf.urls import patterns, url

from tj import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='home'),

    url(r'^query/?$', views.queries_home, name='queries_home'),
    url(r'^query/create$', views.query_create, name='query_create'),
    url(r'^query/post$', views.query_post, name='query_post'),
    url(r'^query/(?P<query_id>\d+)/edit$', views.query_edit, name='query_edit'),
    url(r'^query/(?P<query_id>\d+)/update$', views.query_update, name='query_update'),
    url(r'^query/(?P<query_id>\d+)/run_json', views.query_run_json, name='query_run_json'),
    url(r'^query/(?P<query_id>\d+)/export_csv', views.query_export_csv, name='query_export_csv'),
    url(r'^query/(?P<query_id>\d+)/delete', views.query_delete, name='query_delete'),

    url(r'^combo/?$', views.combos_home, name='combos_home'),
    url(r'^combo/create$', views.combo_create, name='combo_create'),
    url(r'^combo/post$', views.combo_post, name='combo_post'),
    url(r'^combo/(?P<combo_id>\d+)/edit$', views.combo_edit, name='combo_edit'),
    url(r'^combo/(?P<combo_id>\d+)/update$', views.combo_update, name='combo_update'),
    url(r'^combo/(?P<combo_id>\d+)/run_json', views.combo_run_json, name='combo_run_json'),
    url(r'^combo/(?P<combo_id>\d+)/export_csv', views.combo_export_csv, name='combo_export_csv'),
    url(r'^combo/(?P<combo_id>\d+)/delete', views.combo_delete, name='combo_delete'),
)
