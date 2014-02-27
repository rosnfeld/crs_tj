from django.core.urlresolvers import reverse
from django.http import HttpResponse
from django.shortcuts import render
from tj.models import CODE_FILTER_TYPES
import db_layer
import paginator

import json
import datetime


def index(request):
    return render(request, 'tj/index.html')


# consider using a parameters object rather than a map to better manage this complexity?
def construct_query_builder_context(title, results_view, parents):
    """
    Build the base context object for the query_builder template
    """
    code_filter_map = {}
    for filter_type in CODE_FILTER_TYPES:
        code_filter_map[filter_type] = db_layer.get_all_name_code_pairs(filter_type)

    return {'title': title,
            'results_url': reverse(results_view),
            'parents': [(name, reverse(view)) for name, view in parents],
            'year_filter_rows': db_layer.get_years_as_filter_rows(),
            # pass the types explicitly as list, as order is important
            'code_filter_types': CODE_FILTER_TYPES, 'code_filter_map': code_filter_map}


def query_build(request):
    context = construct_query_builder_context(
        title='Query Unanalyzed Data', results_view='query_results', parents=[('Home', 'home')])

    return render(request, 'tj/query_builder.html', context)


def show_results(request, page):
    inclusions = db_layer.get_all_inclusion_rows()
    categories = db_layer.get_all_category_rows()

    return render(request, 'tj/query_results.html',
                  {'page': page, 'inclusions': inclusions, 'categories': categories})


def query_results(request):
    payload = request.read()
    json_payload = json.loads(payload)

    query = db_layer.QueryParams(json_payload['search_terms'])

    for filter_type, codes in json_payload['code_filters'].iteritems():
        for code in codes:
            query.add_code_filter(filter_type, code)

    for year in json_payload['years']:
        query.add_year_filter(year)

    possible_row_count = db_layer.get_count_of_matching_rows_for_query(query)
    result_rows = db_layer.get_matching_rows_for_query(query)

    pandas_paginator = paginator.PandasPaginator(result_rows, db_layer.ROW_LIMIT)
    pandas_paginator.num_items = possible_row_count  # explicitly set this (a bit of a hack)
    page = pandas_paginator.get_page(0)
    page.has_next_page = False  # explicitly disable this

    return show_results(request, page)


def commit_analysis(request):
    if not request.user.is_authenticated():
        return HttpResponse('Unauthorized', status=401)

    payload = request.read()
    json_payload = json.loads(payload)

    db_layer.update_inclusions(json_payload['inclusionActions'])
    db_layer.update_categories(json_payload['categoryActions'])

    return HttpResponse('OK')


def export_csv(request):
    dataframe = db_layer.get_tj_dataset_rows()
    csv_text = db_layer.convert_to_csv_string_for_export(dataframe)

    timestamp_string = datetime.datetime.utcnow().isoformat()

    response = HttpResponse(csv_text, content_type="text/csv")
    response['Content-Disposition'] = 'attachment; filename="CRS_TJ_{timestamp}.csv"'.format(timestamp=timestamp_string)

    return response


def review_analysis(request):
    # TODO a data table with marginals... maybe use pandas crosstab?
    # Count by inclusion, category
    # USD disbursed defl by inclusion, category

    # TODO also include nulls in this categorization?

    return render(request, 'tj/review_analysis.html', {})


def show_review(request, title, results_view):
    context = construct_query_builder_context(
        title=title, results_view=results_view, parents=[('Home', 'home'), ('Review Analyzed Rows', 'review_analysis')])

    context['submit_on_load'] = True

    context['custom_filter_types'] = ('inclusion', 'category')

    context['custom_filter_map'] = {
        'inclusion': db_layer.get_all_inclusion_rows(as_filter=True),
        'category': db_layer.get_all_category_rows(as_filter=True)
    }

    return render(request, 'tj/query_builder.html', context)


def review_tj_dataset(request):
    return show_review(request, title='Review TJ Dataset', results_view='review_tj_dataset_results')


def review_excluded(request):
    return show_review(request, title='Review Excluded Results', results_view='review_excluded_results')


def review_uncategorized(request):
    return show_review(request, title='Review Uncategorized Results', results_view='review_uncategorized_results')


def review_unincluded(request):
    return show_review(request, title='Review Unincluded Results', results_view='review_unincluded_results')


def review_results(request, results_function):
    payload = request.read()
    json_payload = json.loads(payload)

    query = db_layer.QueryParams(json_payload['search_terms'])

    for filter_type, codes in json_payload['code_filters'].iteritems():
        for code in codes:
            query.add_code_filter(filter_type, code)

    for filter_type, codes in json_payload['custom_filters'].iteritems():
        for code in codes:
            query.add_custom_column_filter(filter_type, code)

    for year in json_payload['years']:
        query.add_year_filter(year)

    results = results_function(query)

    pandas_paginator = paginator.PandasPaginator(results, db_layer.ROW_LIMIT)

    page_number = int(json_payload['page_number'])
    page = pandas_paginator.get_page(page_number)

    return show_results(request, page)


def review_tj_dataset_results(request):
    return review_results(request, db_layer.get_tj_dataset_rows)


def review_excluded_results(request):
    return review_results(request, db_layer.get_excluded_rows)


def review_uncategorized_results(request):
    return review_results(request, db_layer.get_included_but_uncategorized_rows)


def review_unincluded_results(request):
    return review_results(request, db_layer.get_categorized_but_no_inclusion_decision_rows)
