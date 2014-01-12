from django.contrib import admin
from tj.models import Query, QueryCombination, ManualExclusion

# Register your models here.
admin.site.register(Query)
admin.site.register(QueryCombination)
admin.site.register(ManualExclusion)
