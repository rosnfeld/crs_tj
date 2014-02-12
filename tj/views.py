from django.core.urlresolvers import reverse
from django.http import HttpResponse
from django.shortcuts import render
from tj.models import CODE_FILTER_TYPES

import db_layer
import json
import datetime


def index(request):
    return render(request, 'tj/index.html')


def query_build(request):
    code_filter_map = {}
    for filter_type in CODE_FILTER_TYPES:
        code_filter_map[filter_type] = db_layer.get_all_name_code_pairs(filter_type)

    return render(request, 'tj/query_builder.html',
                  # pass the types explicitly as list, as order is important
                  {'code_filter_types': CODE_FILTER_TYPES, 'code_filter_map': code_filter_map})


def show_results(request, result_rows, row_limit, possible_row_count=None):
    inclusions = db_layer.get_all_inclusion_rows()
    categories = db_layer.get_all_category_rows()

    return render(request, 'tj/query_results.html',
                  {'rows': result_rows, 'row_limit': row_limit, 'possible_row_count': possible_row_count,
                   'inclusions': inclusions, 'categories': categories})


def query_results(request):
    payload = request.read()
    json_payload = json.loads(payload)

    query = db_layer.QueryParams(json_payload['search_terms'])

    for filter_type, codes in json_payload['code_filters'].iteritems():
        for code in codes:
            query.add_code_filter(filter_type, code)

    possible_row_count = db_layer.get_count_of_matching_rows_for_query(query)
    result_rows = db_layer.get_matching_rows_for_query(query)

    return show_results(request, result_rows, db_layer.ROW_LIMIT, possible_row_count)


def commit_analysis(request):
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


def review_tj_dataset(request):
    return render(request, 'tj/review_dataset.html',
                  {'title': 'Review TJ Dataset', 'results_url': reverse('review_tj_dataset_results')})


def review_tj_dataset_results(request):
    return show_results(request, db_layer.get_tj_dataset_rows(), db_layer.NO_ROW_LIMIT)


def review_excluded(request):
    return render(request, 'tj/review_dataset.html',
                  {'title': 'Review Excluded Results', 'results_url': reverse('review_excluded_results')})


def review_excluded_results(request):
    return show_results(request, db_layer.get_excluded_rows(), db_layer.NO_ROW_LIMIT)


def review_uncategorized(request):
    return render(request, 'tj/review_dataset.html',
                  {'title': 'Review Uncategorized Results', 'results_url': reverse('review_uncategorized_results')})


def review_uncategorized_results(request):
    return show_results(request, db_layer.get_included_but_uncategorized_rows(), db_layer.NO_ROW_LIMIT)


def review_unincluded(request):
    return render(request, 'tj/review_dataset.html',
                  {'title': 'Review Unincluded Results', 'results_url': reverse('review_unincluded_results')})


def review_unincluded_results(request):
    return show_results(request, db_layer.get_categorized_but_no_inclusion_decision_rows(),
                        db_layer.NO_ROW_LIMIT)
