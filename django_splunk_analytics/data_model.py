# -*- coding: utf-8 -*-
"""data_model.py: Django """

from __future__ import unicode_literals
from __future__ import print_function

import argparse
import decimal
import json
import logging

import datetime
import os
import re

import sys
from collections import OrderedDict

from django.contrib.admin.options import get_content_type_for_model
from django.utils.timezone import now
from django_splunk_analytics.utils import SplunkRequest

try:
    from .models import AnalyticsChanges, AnalyticsModelTracker
except ValueError:
    sys.path.append(os.path.abspath("."))
    from django_splunk_analytics.models import AnalyticsChanges, AnalyticsModelTracker

    __author__ = 'Steven Klass'
__date__ = '1/12/17 11:38'
__credits__ = ['Steven Klass', ]

log = logging.getLogger(__name__)

# Things we are not doing yet
#  - Accounting for FK changes - Community.subdivision_set.all() -
#

SPLUNK_PREFERRED_DATETIME = "%Y-%m-%d %H:%M:%S:%f"
INTS = re.compile(r"^-?[0-9]+$")
NUMS = re.compile(r"^[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$")


def splunk_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError

class HistoricalAnalyticsCollector(object):
    model = None
    fields = ('pk',)
    field_map = OrderedDict()
    field_methods = ['get_historical_attributes']
    simple_history_attribute_name = 'history'
    splunk_timestamp_field = 'historical_last_change_date'

    search_quantifiers = None

    def __init__(self, reset=False, max_count=None):
        self.last_look = None
        self.locked = False
        self.add_pks = []
        self.update_pks = []
        self.delete_pks = []
        self._existing_record_ids = []
        self.splunk_ready = False
        self.skip_locks = True
        self.reset = reset
        self.output_file = None
        self.max_count = max_count

        err_msg = "Missing attribute %r on model" % self.simple_history_attribute_name
        assert hasattr(self.model, self.simple_history_attribute_name), err_msg

    def get_queryset(self):
        return self.historical_model.all()

    @property
    def historical_model(self):
        return getattr(self.model, self.simple_history_attribute_name)

    @property
    def content_type(self):
        return get_content_type_for_model(self.model)

    @property
    def verbose_name(self):
        return self.model._meta.verbose_name.title()

    def lock(self):
        prior_date = now() - datetime.timedelta(days=365 * 100)
        try:
            self.last_look = AnalyticsModelTracker.objects.get(
                content_type=self.content_type)
        except AnalyticsModelTracker.DoesNotExist:
            self.last_look = AnalyticsModelTracker.objects.create(
                content_type=self.content_type, last_updated=prior_date, state=0)
        else:
            if self.last_look.state == 0:
                if not self.skip_locks:
                    raise RuntimeError("Already processing")
            if self.reset:
                log.warning("Resetting {}".format(self.verbose_name))
                AnalyticsChanges.objects.filter(content_type=self.content_type).delete()
                self.last_look.last_updated = prior_date
            self.last_look.state = 0
            self.last_look.save()

        self.locked = True

    def unlock(self):
        """Sets our last look and open the db"""
        last_change = AnalyticsChanges.objects.filter(content_type=self.content_type).order_by('last_updated').last()
        self.last_look.state = 0
        self.last_look.last_updated = last_change.last_updated
        self.last_look.save()
        self.locked = False

    def get_historical_change_delete_pks(self):
        """This will collect a list of changes since we last looked at the data"""
        assert self.locked, "You need to lock the db first"
        historical_changes = self.get_queryset().filter(history_date__gt=self.last_look.last_updated)
        deletes = historical_changes.filter(history_type="-")
        historical_deletes = list(set(deletes.values_list('id', flat=True)))

        changes = historical_changes.exclude(history_type="-").exclude(id__in=historical_deletes)
        historical_changes = list(set(changes.values_list('id', flat=True)))
        log.debug("%s Identified %d changes and %d deletes", self.verbose_name ,len(historical_changes), len(historical_deletes))
        return historical_changes, historical_deletes

    def get_accounted_pks(self):
        return AnalyticsChanges.objects.filter(content_type=self.content_type).values_list('object_id', flat=True)

    @property
    def splunk(self):
        if not self.splunk_ready:
            self.splunk_req = SplunkRequest()
            self.splunk_ready = True
        return self.splunk_req

    @property
    def search_quantifier(self):
        data = ""
        if self.search_quantifiers:
            data = self.search_quantifier
        return data + " model={}".format(self.model._meta.model_name)

    def delete_items(self, delete_pks):
        if not len(delete_pks):
            return
        ids = " OR ".join(["id={}".format(x) for x in delete_pks])
        delete_query = "{} ({}) | delete".format(self.search_quantifier, ids)
        search_id = self.splunk.create_search(delete_query)
        return self.splunk.get_search_status(search_id, wait_for_results=True)

    def get_actions(self):
        assert self.locked, "You need to lock the db first"

        historical_change_pks, historical_delete_pks = self.get_historical_change_delete_pks()
        accounted_pks = self.get_accounted_pks()

        delete_pks = list(set(accounted_pks).intersection(set(historical_delete_pks)))
        update_pks = list(set(accounted_pks).intersection(set(historical_change_pks)))
        add_pks = list(set(historical_change_pks) - set(self.update_pks))

        log.info("%s identified %d add actions, %d update actions and %d delete actions",
                  self.verbose_name, len(add_pks), len(update_pks), len(delete_pks))

        return (add_pks, update_pks, delete_pks)

    def get_base_values(self, pks):
        """This is the main method for getting the values - a list of dictionaries"""
        fields = self.fields
        if 'pk' not in fields:
            fields = tuple(['pk'] + list(fields))
        # Preserve the order we want.
        return [OrderedDict([(k, x[k]) for k in fields]) for x in self.model.objects.filter(id__in=pks).values(*fields)]

    def get_values(self, add_pks):

        values = self.get_base_values(add_pks)
        field_method_results = self.get_field_methods(add_pks)
        results = []
        for item in values:
            field_values = field_method_results.get(item.get('pk'), {})
            item.update(field_values)
            results.append(item)
        return results

    def add_items(self, add_pks):

        for item in self.get_values(add_pks):
            result = self.dump_result(item)
            change, create = AnalyticsChanges.objects.get_or_create(
                content_type=self.content_type,
                object_id=item.get('pk'), defaults={'last_updated': item['historical_last_change_date']})
            if not create:
                change.last_updated = item['historical_last_change_date']
                change.save()
            if self.output_file:
                with open(self.output_file, "w") as outfile:
                    outfile.write("{}\n".format(result))
            else:
                print("{}".format(result))

    def get_historical_attributes(self, pks):
        results = {}
        data = self.historical_model.filter(id__in=pks).values_list('id', 'history_date', 'history_type')
        for pk, hist_date, hist_type in data:
            if pk not in results:
                results[pk] = OrderedDict([('historical_create_date', hist_date),
                                           ('historical_last_change_date', hist_date),
                                           ('historical_total_changes', 0),
                                           ('historical_delta_days', 0)])
            results[pk]['historical_total_changes'] += 1
            if hist_date < results[pk]['historical_create_date']:
                results[pk]['historical_create_date'] = hist_date
            if hist_date > results[pk]['historical_last_change_date']:
                results[pk]['historical_last_change_date'] = hist_date
        for k, v in results.items():
            last = v['historical_last_change_date']
            create = v['historical_create_date']
            delta_days = ((((last - create).total_seconds() / 60.0) / 60.0) / 24.0)
            results[k]['historical_delta_days'] = delta_days
            results[k]['historical_average_days'] = delta_days / float(v['historical_total_changes'])
        return results

    def dump_result(self, item):

        data = OrderedDict([('timestamp', item.get(self.splunk_timestamp_field)), ('pk', item.get('pk'))])

        def clean_value(value):
            if isinstance(value, basestring):
                if value.startswith("00"):
                    pass
                elif INTS.search(value):
                    value = int(value)
                elif NUMS.search(value):
                    value = float(value)
                elif not len(value):
                    value = None
            elif isinstance(value, (list, tuple)):
                if not len(value):
                    value = None
                else:
                    value = [clean_value(v) for v in value]
            return value

        for _field, value in item.items():
            field = self.field_map.get(_field, _field)
            value = clean_value(value)
            if value is not None:
                data[field] = value

        return json.dumps(data, default=splunk_default, sort_keys=False)

    def get_field_methods(self, add_pks):

        results = {}
        for method_name in self.field_methods:
            method = getattr(self, method_name)
            data_dict = method(add_pks)
            for k, v in data_dict.items():
                if k not in results:
                    results[k] = OrderedDict()
                results[k].update(v)
        return results

    def analyze(self):

        try:
            self.lock()
        except RuntimeError as err:
            log.info("Unable to lock! - %r", err)
            return err

        log.debug("Getting actions")
        (add_pks, update_pks, delete_pks) = self.get_actions()

        delete_pks = update_pks + delete_pks
        delete_pks = delete_pks[:self.max_count] if self.max_count else delete_pks
        self.delete_items(delete_pks)

        add_pks = add_pks + update_pks
        add_pks = add_pks[:self.max_count] if self.max_count else add_pks
        self.add_items(add_pks)

        self.unlock()


import django
django.setup()
from apps.community.models import Community
class CommunityCollector(HistoricalAnalyticsCollector):
    model = Community


def main(args):
    """Main - $<description>$"""
    logging.basicConfig(
        level=logging.INFO, datefmt="%H:%M:%S", stream=sys.stdout,
        format="%(asctime)s %(levelname)s [%(filename)s] (%(name)s) %(message)s")

    args.verbose = 4 if args.verbose > 4 else args.verbose
    loglevel = 50 - args.verbose * 10
    log.setLevel(loglevel)

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', args.settings)

    log.debug("Starting to process community..")
    collector = CommunityCollector()
    collector.analyze()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="$<description>$")
    parser.add_argument('-v', dest='verbose', help="How verbose of the output",
                        action='append_const', const=1, default=[1, 2, 3, 4, 5])
    parser.add_argument('-y', dest='settings', help="Django Settings", action='store')
    parser.add_argument("-n", dest='dry_run', help="Dry Run", action="store_true")
    sys.exit(main(parser.parse_args()))
