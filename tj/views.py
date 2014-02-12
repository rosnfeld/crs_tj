from django.core.urlresolvers import reverse
from django.http import HttpResponse, HttpResponseRedirect, HttpResponseBadRequest
from django.shortcuts import render, get_object_or_404
from tj.models import Query, QueryCombination, CODE_FILTER_TYPES

import query_processor
import json
import datetime


def index(request):
    return render(request, 'tj/index.html')


def queries_home(request):
    queries = Query.objects.all()
    return render(request, 'tj/queries.html', {'queries': queries})


def combos_home(request):
    query_combos = QueryCombination.objects.all()
    return render(request, 'tj/combos.html', {'combos': query_combos})


def query_create(request):
    # TODO enforce POST?
    query = Query(text='new query')
    query.save()

    return HttpResponseRedirect(reverse('query_edit', args=(query.id,)))


def query_edit(request, query_id):
    query = get_object_or_404(Query, pk=query_id)
    # TODO would like to filter by year here also but it doesn't fit the pattern
    return render(request, 'tj/query_edit.html', {'query': query, 'code_filter_types': CODE_FILTER_TYPES})


def query_update_text(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    try:
        query_text = request.POST['query_text']
    except KeyError:
        return HttpResponseBadRequest('Bad form data')

    if not query_text:
        return HttpResponseBadRequest('No text entered')

    query.text = query_text
    query.save()

    return HttpResponse('OK')


def query_get_exclusions_json(request, query_id):
    query = get_object_or_404(Query, pk=query_id)
    row_ids = [exclusion.pandas_row_id for exclusion in query.manualexclusion_set.all()]
    return HttpResponse(json.dumps(row_ids), content_type="application/json")


def query_add_exclusion(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    try:
        row_id = int(request.POST['row_id'])
    except KeyError:
        return HttpResponseBadRequest('Bad form data')

    query.manualexclusion_set.create(pandas_row_id=row_id)

    return HttpResponse('OK')


def query_remove_exclusion(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    try:
        row_id = int(request.POST['row_id'])
    except KeyError:
        return HttpResponseBadRequest('Bad form data')

    existing_exclusions = query.manualexclusion_set.all()
    for exclusion in existing_exclusions:
        if exclusion.pandas_row_id == row_id:
            exclusion.delete()

    return HttpResponse('OK')


def query_results(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    rows = query_processor.get_matching_rows_for_query(query)

    return render(request, 'tj/query_results.html', {'rows': rows})


def query_export_csv(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    dataframe = query_processor.get_matching_rows_for_query(query)
    csv_text = query_processor.convert_to_csv_string_for_export(dataframe)

    response = HttpResponse(csv_text, content_type="text/csv")
    response['Content-Disposition'] = 'attachment; filename="%s.csv"' % query.text

    return response


def query_delete(request, query_id):
    query = get_object_or_404(Query, pk=query_id)
    query.delete()
    return HttpResponseRedirect(reverse('queries_home'))


def query_get_filters_json(request, query_id):
    query = get_object_or_404(Query, pk=query_id)
    filter_codes = {}

    for filter_type in CODE_FILTER_TYPES:
        filter_codes[filter_type] = []

    for code_filter in query.codefilter_set.all():
        filter_codes[code_filter.filter_type].append(code_filter.code)

    return HttpResponse(json.dumps(filter_codes), content_type="application/json")


def query_filter_edit(request, query_id, filter_type):
    query = get_object_or_404(Query, pk=query_id)
    filter_rows = query_processor.get_all_name_code_pairs(filter_type)

    return render(request, 'tj/filter_edit.html',
                  {'query': query, 'filter_type': filter_type, 'filter_rows': filter_rows})


def query_filter_update(request, query_id, filter_type):
    query = get_object_or_404(Query, pk=query_id)

    try:
        codes = [int(code) for code in request.POST.getlist('codes[]')]
    except KeyError:
        return HttpResponseBadRequest('Bad form data')

    existing_filters_of_this_type = query.codefilter_set.all().filter(filter_type__exact=filter_type)
    for code_filter in existing_filters_of_this_type:
        code_filter.delete()

    for code in codes:
        query.codefilter_set.create(filter_type=filter_type, code=code)

    return HttpResponse('OK')


def combo_create(request):
    # TODO enforce POST?
    combo = QueryCombination(name='Unnamed Query Combination')
    combo.save()

    return HttpResponseRedirect(reverse('combo_edit', args=(combo.id,)))


def combo_edit(request, combo_id):
    combo = get_object_or_404(QueryCombination, pk=combo_id)
    possible_queries = Query.objects.all()
    return render(request, 'tj/combo_edit.html', {'combo': combo, 'possible_queries': possible_queries})


def combo_update(request, combo_id):
    combo = get_object_or_404(QueryCombination, pk=combo_id)

    try:
        combo_name = request.POST['combo_name']
        query_ids = [int(id) for id in request.POST.getlist('query_ids[]')]
    except KeyError:
        return HttpResponseBadRequest('Bad form data')  # could be JSON?

    if not combo_name:
        return HttpResponseBadRequest('No name entered')

    combo.name = combo_name
    combo.save()

    # slightly inefficient, but easiest to just wipe the queries and start over
    existing_queries = combo.queries.all()
    for query in existing_queries:
        combo.queries.remove(query)

    for query_id in query_ids:
        combo.queries.add(query_id)

    return HttpResponseRedirect(reverse('combo_edit', args=(combo.id,)))


def combo_run_json(request, combo_id):
    combo = get_object_or_404(QueryCombination, pk=combo_id)

    dataframe = query_processor.get_matching_rows_for_combo(combo)

    json_text = dataframe.to_json(orient='index')

    return HttpResponse(json_text, content_type="application/json")


def combo_export_csv(request, combo_id):
    combo = get_object_or_404(QueryCombination, pk=combo_id)

    dataframe = query_processor.get_matching_rows_for_combo(combo)
    csv_text = query_processor.convert_to_csv_string_for_export(dataframe)

    response = HttpResponse(csv_text, content_type="text/csv")
    response['Content-Disposition'] = 'attachment; filename="%s.csv"' % combo.name

    return response


def combo_delete(request, combo_id):
    combo = get_object_or_404(QueryCombination, pk=combo_id)
    combo.delete()
    return HttpResponseRedirect(reverse('combos_home'))


def query_build(request):
    code_filter_map = {}
    for filter_type in CODE_FILTER_TYPES:
        code_filter_map[filter_type] = query_processor.get_all_name_code_pairs(filter_type)

    return render(request, 'tj/query_builder.html',
                  # pass the types explicitly as list, as order is important
                  {'code_filter_types': CODE_FILTER_TYPES, 'code_filter_map': code_filter_map})


def show_results(request, result_rows, row_limit, possible_row_count=None):
    inclusions = query_processor.get_all_inclusion_rows()
    categories = query_processor.get_all_category_rows()

    return render(request, 'tj/query_results.html',
                  {'rows': result_rows, 'row_limit': row_limit, 'possible_row_count': possible_row_count,
                   'inclusions': inclusions, 'categories': categories})


def query_results_new(request):
    payload = request.read()
    json_payload = json.loads(payload)

    query = query_processor.QueryParams(json_payload['search_terms'])

    for filter_type, codes in json_payload['code_filters'].iteritems():
        for code in codes:
            query.add_code_filter(filter_type, code)

    possible_row_count = query_processor.get_count_of_matching_rows_for_query(query.search_terms, query.code_filters)
    result_rows = query_processor.get_matching_rows_for_query_new(query)

    return show_results(request, result_rows, query_processor.ROW_LIMIT, possible_row_count)


def commit_analysis(request):
    payload = request.read()
    json_payload = json.loads(payload)

    # TODO this argues for a better name than "query processor" - really it's now more of a db_access_layer
    query_processor.updateInclusions(json_payload['inclusionActions'])
    query_processor.updateCategories(json_payload['categoryActions'])

    return HttpResponse('OK')


def export_csv(request):
    dataframe = query_processor.get_tj_dataset_rows()
    csv_text = query_processor.convert_to_csv_string_for_export(dataframe)

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
    return show_results(request, query_processor.get_tj_dataset_rows(), query_processor.NO_ROW_LIMIT)


def review_excluded(request):
    return render(request, 'tj/review_dataset.html',
                  {'title': 'Review Excluded Results', 'results_url': reverse('review_excluded_results')})


def review_excluded_results(request):
    return show_results(request, query_processor.get_excluded_rows(), query_processor.NO_ROW_LIMIT)


def review_uncategorized(request):
    return render(request, 'tj/review_dataset.html',
                  {'title': 'Review Uncategorized Results', 'results_url': reverse('review_uncategorized_results')})


def review_uncategorized_results(request):
    return show_results(request, query_processor.get_included_but_uncategorized_rows(), query_processor.NO_ROW_LIMIT)


def review_unincluded(request):
    return render(request, 'tj/review_dataset.html',
                  {'title': 'Review Unincluded Results', 'results_url': reverse('review_unincluded_results')})


def review_unincluded_results(request):
    return show_results(request, query_processor.get_categorized_but_no_inclusion_decision_rows(),
                        query_processor.NO_ROW_LIMIT)
