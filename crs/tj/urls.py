from django.conf.urls import patterns, url

from tj import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='home'),

    url(r'^query/?$', views.queries_home, name='queries_home'),
    url(r'^query/create$', views.query_create, name='query_create'),
    url(r'^query/(?P<query_id>\d+)/edit$', views.query_edit, name='query_edit'),
    url(r'^query/(?P<query_id>\d+)/update_text$', views.query_update_text, name='query_update_text'),
    url(r'^query/(?P<query_id>\d+)/exclusions', views.query_get_exclusions, name='query_get_exclusions'),
    url(r'^query/(?P<query_id>\d+)/add_exclusion', views.query_add_exclusion, name='query_add_exclusion'),
    url(r'^query/(?P<query_id>\d+)/remove_exclusion', views.query_remove_exclusion, name='query_remove_exclusion'),
    url(r'^query/(?P<query_id>\d+)/results', views.query_results, name='query_results'),
    url(r'^query/(?P<query_id>\d+)/export_csv', views.query_export_csv, name='query_export_csv'),
    url(r'^query/(?P<query_id>\d+)/delete', views.query_delete, name='query_delete'),
    url(r'^query/filter_box/(?P<filter_type>\w+)', views.filter_box, name='filter_box'),

    url(r'^combo/?$', views.combos_home, name='combos_home'),
    url(r'^combo/create$', views.combo_create, name='combo_create'),
    url(r'^combo/(?P<combo_id>\d+)/edit$', views.combo_edit, name='combo_edit'),
    url(r'^combo/(?P<combo_id>\d+)/update$', views.combo_update, name='combo_update'),
    url(r'^combo/(?P<combo_id>\d+)/run_json', views.combo_run_json, name='combo_run_json'),
    url(r'^combo/(?P<combo_id>\d+)/export_csv', views.combo_export_csv, name='combo_export_csv'),
    url(r'^combo/(?P<combo_id>\d+)/delete', views.combo_delete, name='combo_delete'),
)
