# -*- coding: utf-8 -*-
"""data_model.py: Django """

from __future__ import unicode_literals
from __future__ import print_function

import logging

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import models

__author__ = 'Steven Klass'
__date__ = '1/12/17 11:38'
__credits__ = ['Steven Klass', ]

log = logging.getLogger(__name__)


class AnalyticsModelTracker(models.Model):
    """Tracks a general model processing"""
    content_type = models.ForeignKey(ContentType)
    last_updated = models.DateTimeField()
    state = models.SmallIntegerField(choices=[(1, 'Ready'), (2, 'In-Process')])
    last_updated = models.DateTimeField()

class AnalyticsChanges(models.Model):

    # Enable generic foreign key to other models
    content_type = models.ForeignKey(ContentType)
    object_id = models.PositiveIntegerField(db_index=True)
    content_object = GenericForeignKey('content_type', 'object_id')
    last_updated = models.DateTimeField()
