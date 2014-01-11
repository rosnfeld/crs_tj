from django.contrib import admin
from tj.models import Query, QueryUnion, ManualExclusion

# Register your models here.
admin.site.register(Query)
admin.site.register(QueryUnion)
admin.site.register(ManualExclusion)
