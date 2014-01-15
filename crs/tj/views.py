from django.core.urlresolvers import reverse
from django.http import HttpResponse, HttpResponseRedirect, HttpResponseBadRequest
from django.shortcuts import render, get_object_or_404
from tj.models import Query, QueryCombination

import query_processor


def index(request):
    return render(request, 'tj/index.html')


def queries_home(request):
    queries = Query.objects.all()
    return render(request, 'tj/queries.html', {'queries': queries})


def combos_home(request):
    query_combos = QueryCombination.objects.all()
    return render(request, 'tj/combos.html', {'combos': query_combos})


def query_create(request):
    return render(request, 'tj/query_create.html')


def query_post(request):
    try:
        query_text = request.POST['query_text']
    except KeyError:
        return render(request, 'tj/query_create.html', {'error_message': 'Bad form data'})

    if not query_text:
        return render(request, 'tj/query_create.html', {'error_message': 'No text entered'})

    query = Query(text=query_text)
    query.save()

    # Always return an HttpResponseRedirect after successfully dealing with POST data.
    # This prevents data from being posted twice if a user hits the Back button.
    return HttpResponseRedirect(reverse('query_edit', args=(query.id,)))


def query_edit(request, query_id):
    query = get_object_or_404(Query, pk=query_id)
    return render(request, 'tj/query_edit.html', {'query': query})


def query_update(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    try:
        query_text = request.POST['query_text']
        manual_exclusion_ids = [int(id) for id in request.POST.getlist('manual_exclusion_ids[]')]
    except KeyError:
        return HttpResponseBadRequest('Bad form data')  # could be JSON?

    if not query_text:
        return HttpResponseBadRequest('No text entered')

    query.text = query_text
    query.save()

    # slightly inefficient, but easiest is to just wipe the existing exclusions and start fresh
    existing_exclusions = query.manualexclusion_set.all()
    for exclusion in existing_exclusions:
        exclusion.delete()

    for manual_exclusion_id in manual_exclusion_ids:
        query.manualexclusion_set.create(pandas_row_id=manual_exclusion_id)

    return HttpResponseRedirect(reverse('query_edit', args=(query.id,)))


def query_run_json(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    dataframe = query_processor.find_rows_matching_query_text(query.text)

    json_text = dataframe.to_json(orient='index')

    return HttpResponse(json_text, content_type="application/json")


def combo_create(request):
    return render(request, 'tj/combo_create.html')


def combo_post(request):
    try:
        combo_name = request.POST['combo_name']
    except KeyError:
        return render(request, 'tj/combo_create.html', {'error_message': 'Bad form data'})

    if not combo_name:
        return render(request, 'tj/combo_create.html', {'error_message': 'No name entered'})

    combo = QueryCombination(name=combo_name)
    combo.save()

    return HttpResponseRedirect(reverse('combo_edit', args=(combo.id,)))


def combo_edit(request, combo_id):
    combo = get_object_or_404(QueryCombination, pk=combo_id)
    return render(request, 'tj/combo_edit.html', {'combo': combo})
