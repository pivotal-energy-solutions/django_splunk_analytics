# -*- coding: utf-8 -*-
"""utils.py: Django """

from __future__ import unicode_literals
from __future__ import print_function

import argparse
import decimal
import json
import logging
import os
import pprint
import re
import urllib
from collections import OrderedDict
import time
import datetime
import requests
import sys

__author__ = 'Steven Klass'
__date__ = '1/12/17 13:34'
__credits__ = ['Steven Klass', ]

log = logging.getLogger(__name__)

SPLUNK_PREFERRED_DATETIME = "%Y-%m-%d %H:%M:%S:%f"
INTS = re.compile(r"^-?[0-9]+$")
NUMS = re.compile(r"^[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$")

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


class SplunkAuthenticationException(Exception):
    def __init__(self, value, *args, **kwargs):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SplunkError(Exception):
    def __init__(self, value, *args, **kwargs):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SplunkRequest(object):

    def __init__(self, *args, **kwargs):
        self.username = kwargs.get('username', 'admin')
        self.password = kwargs.get('password', 'changeme')
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', '8089')
        self.session_key = kwargs.get('splunk_session_key')
        self.session = None
        self.base_url = 'https://{host}:{port}'.format(host=self.host, port=self.port)

    def connect(self, **kwargs):
        if self.session_key:
            return

        if self.session:
            return self.session

        self.session = requests.Session()
        try:
            url = '{base_url}/services/auth/login?output_mode=json'.format(base_url=self.base_url)
            request = self.session.post(
                url, data={'username': self.username, 'password': self.password},
                auth=(self.username, self.password), verify=False)
            if request.status_code != 200:
                raise SplunkAuthenticationException(
                    "Authorization error ({status_code}) connecting to {url}".format(
                        status_code=request.status_code, url=url))
            self.session_key = request.json().get('sessionKey')
            self.headers = {'Authorization': 'Splunk {session_key}'.format(**self.__dict__),
                            'content-type': 'application/json'}
        except:
            log.error("Issue connecting to %(base_url)s with %(username)s:%(password)s", self.__dict__)
            raise
        log.debug("Successfully logged in and have session key %(session_key)s", self.__dict__)

    def create_search(self, search_query):
        """Create a basic search"""
        self.connect()
        if not search_query.startswith('search'):
            search_query = 'search {search_query}'.format(search_query=search_query)
        request = self.session.post(
            '{base_url}/services/search/jobs?output_mode=json'.format(base_url=self.base_url),
            headers=self.headers, data={'search': search_query}, verify=False)

        data = request.json()
        if data.get('messages') and data.get('messages')[0].get('type') == 'FATAL':
            log.error('FATAL response received: {text}'.format(
                text=data.get('messages')[0].get('text')))
        log.debug("Created search on {search} and id = {sid}".format(search=search_query, **data))
        return data.get('sid')

    def get_search_status(self, search_id, wait_for_results=True):

        self.connect()
        url = '{base_url}/services/search/jobs/{search_id}/results?output_mode=json'
        start = None
        while True:
            request = self.session.get(url.format(base_url=self.base_url, search_id=search_id),
                                   headers=self.headers, verify=False)
            if not wait_for_results:
                break
            if not start or (datetime.datetime.now() - start).seconds > 5:
                log.debug("Waiting on results")
                start = datetime.datetime.now()
            if request.status_code == 200:
                break
            time.sleep(.5)
        return request.json(), request.status_code

    def get_normalized_data(self, content):

        data = OrderedDict()
        keys = content.keys()
        keys.sort()

        for date_key in ['time', 'date', 'timestamp']:
            if date_key in keys:
                keys.pop(keys.index(date_key))
                keys = [date_key] + keys

        for key in keys:
            value = content.get(key)
            if isinstance(value, basestring):
                if value.startswith("00"):
                    pass
                elif INTS.search(value):
                    value = int(value)
                elif NUMS.search(value):
                    value = float(value)
            data[key] = value
        return data



def main(args):
    """Main - $<description>$"""
    logging.basicConfig(
        level=logging.INFO, datefmt="%H:%M:%S", stream=sys.stdout,
        format="%(asctime)s %(levelname)s [%(filename)s] (%(name)s) %(message)s")

    args.verbose = 4 if args.verbose > 4 else args.verbose
    loglevel = 50 - args.verbose * 10
    log.setLevel(loglevel)

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', args.settings)

    splunk = SplunkRequest()
    search_id = splunk.create_search("source=icm_data| top limit=20 user")
    results = splunk.get_search_status(search_id)

    pprint.pprint(results)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="$<description>$")
    parser.add_argument('-v', dest='verbose', help="How verbose of the output",
                        action='append_const', const=1, default=[1, 2, 3])
    parser.add_argument('-y', dest='settings', help="Django Settings", action='store')
    parser.add_argument("-n", dest='dry_run', help="Dry Run", action="store_true")
    sys.exit(main(parser.parse_args()))
