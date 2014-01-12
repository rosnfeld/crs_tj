from django.shortcuts import render
from tj.models import Query, QueryUnion


def index(request):
    return render(request, 'tj/index.html')


def queries_home(request):
    queries = Query.objects.all()
    return render(request, 'tj/queries.html', {'queries': queries})


def query_combos_home(request):
    query_combos = QueryUnion.objects.all()
    return render(request, 'tj/query_combos.html', {'query_combos': query_combos})
