from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse
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


def query_process(request):
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

    rows = query_processor.find_rows_matching_query_text(query.text)

    return render(request, 'tj/query_edit.html', {'query': query, 'rows': rows})
