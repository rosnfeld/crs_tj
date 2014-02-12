from django.db import models

# no longer using Django ORM, but may choose to again at some point

# would be nice to have a proper enum in a static "code table",
# but I think this is the django way of doing things
CODE_FILTER_TYPES = ['recipient', 'donor', 'agency', 'channel', 'sector', 'purpose']
CODE_FILTER_CHOICES = tuple((x, x) for x in CODE_FILTER_TYPES)
