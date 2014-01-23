from django.db import models


class Query(models.Model):
    """
    Models various search criteria
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


# would be nice to have a proper enum in a static "code table",
# but I think this is the django way of doing things
CODE_FILTER_TYPES = ['recipient', 'donor', 'agency', 'channel', 'sector', 'purpose']
CODE_FILTER_CHOICES = tuple((x, x) for x in CODE_FILTER_TYPES)


class CodeFilter(models.Model):
    """
    Models a given {donor,recipient,sector,...} code that could be required by a Query
    """
    filter_type = models.CharField(max_length=16, choices=CODE_FILTER_CHOICES)
    code = models.PositiveIntegerField()
    query = models.ForeignKey(Query)

    def __unicode__(self):
        return unicode(self.filter_type) + u'_' + unicode(self.code)


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
