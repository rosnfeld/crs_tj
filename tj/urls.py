from django.conf.urls import patterns, url

from tj import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='home'),

    url(r'^login$', 'django.contrib.auth.views.login', {'template_name': 'tj/login.html'}, name='login'),
    url(r'^logout$', 'django.contrib.auth.views.logout_then_login', {'login_url': 'login'}, name='logout'),

    url(r'^query/build$', views.query_build, name='query_build'),
    url(r'^query/results$', views.query_results, name='query_results'),
    url(r'^query/commit_analysis$', views.commit_analysis, name='query_commit_analysis'),

    url(r'^review_analysis$', views.review_analysis, name='review_analysis'),
    url(r'^review_tj_dataset$', views.review_tj_dataset, name='review_tj_dataset'),
    url(r'^review_tj_dataset_results$', views.review_tj_dataset_results, name='review_tj_dataset_results'),
    url(r'^review_excluded$', views.review_excluded, name='review_excluded'),
    url(r'^review_excluded_results$', views.review_excluded_results, name='review_excluded_results'),
    url(r'^review_uncategorized$', views.review_uncategorized, name='review_uncategorized'),
    url(r'^review_uncategorized_results$', views.review_uncategorized_results, name='review_uncategorized_results'),
    url(r'^review_unincluded$', views.review_unincluded, name='review_unincluded'),
    url(r'^review_unincluded_results$', views.review_unincluded_results, name='review_unincluded_results'),
    url(r'^export_csv$', views.export_csv, name='export_csv'),
)
