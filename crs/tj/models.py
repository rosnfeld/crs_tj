from django.db import models


class Query(models.Model):
    """
    Currently just models some search text we want persisted.
    Will be fleshed out with more fields in due course.
    """
    text = models.CharField(max_length=64)

    def __unicode__(self):
        return self.text


class ManualExclusion(models.Model):
    """
    Models a manual exclusion of a row returned by a Query
    """
    pandas_row_id = models.PositiveIntegerField()
    query = models.ForeignKey(Query)

    def __unicode__(self):
        return unicode(self.pandas_row_id)


class QueryCombination(models.Model):
    """
    Models the combination of several Queries into a "mega-query".
    Note that the Query results, pre-manual exclusion, are unioned together,
    and then the manual exclusions also get unioned together and applied,
    so it's not exactly the "straight" union of Query results.
    """
    queries = models.ManyToManyField(Query)
    name = models.CharField(max_length=64)

    def __unicode__(self):
        return self.name
