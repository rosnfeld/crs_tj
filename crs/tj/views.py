from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.core.urlresolvers import reverse
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, get_object_or_404
from tj.models import Query, QueryCombination

import query_processor


def index(request):
    return render(request, 'tj/index.html')


def queries_home(request):
    queries = Query.objects.all()
    return render(request, 'tj/queries.html', {'queries': queries})


def query_combos_home(request):
    query_combos = QueryCombination.objects.all()
    return render(request, 'tj/query_combos.html', {'query_combos': query_combos})


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

    dataframe = query_processor.find_rows_matching_query_text(query.text)
    row_list = [row for i, row in dataframe.iterrows()]

    paginator = Paginator(row_list, 10)

    page_number = request.GET.get('page')

    try:
        rows = paginator.page(page_number)
    except PageNotAnInteger:
        # If page is not an integer, deliver first page.
        rows = paginator.page(1)
    except EmptyPage:
        # If page is out of range (e.g. 9999), deliver last page of results.
        rows = paginator.page(paginator.num_pages)

    return render(request, 'tj/query_edit.html', {'query': query, 'rows': rows})


def query_update(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    try:
        query_text = request.POST['query_text']
    except KeyError:
        return render(request, 'tj/query_create.html', {'error_message': 'Bad form data'})

    if not query_text:
        return render(request, 'tj/query_create.html', {'error_message': 'No text entered'})

    query.text = query_text
    query.save()

    return HttpResponseRedirect(reverse('query_edit', args=(query.id,)))


def query_run_json(request, query_id):
    query = get_object_or_404(Query, pk=query_id)

    dataframe = query_processor.find_rows_matching_query_text(query.text)

    json_text = dataframe.to_json(orient='index')

    return HttpResponse(json_text, content_type="application/json")
