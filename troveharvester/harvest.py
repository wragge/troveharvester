import re
import json
try:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
except ImportError:
    from urllib2 import urlopen, Request, HTTPError
from .utilities import retry
import codecs


class ServerError(Exception):
    pass


class TroveHarvester:
    """
    A basic harvester for downloading records via the Trove API.
    You'll want to subclass this and, at a minimum, overwrite
    process_results() to actually do something with the stuff
    you're harvesting.

    """
    trove_api = None
    query = None
    harvested = 0
    number = 20
    maximum = 0

    def __init__(self, trove_api, **kwargs):
        self.trove_api = trove_api
        query = kwargs.get('query', None)
        if query:
            self.query = self._clean_query(query)
        self.harvested = int(kwargs.get('start', 0))
        self.number = int(kwargs.get('number', 20))
        maximum = kwargs.get('max')
        if maximum:
            self.maximum = int(maximum)
            self.max_set = True
        else:
            self.maximum = self.harvested + 1
            self.max_set = False

    def _clean_query(self, query):
        """Remove s and n values just in case."""
        query = re.sub(r'&s=\d+', '', query)
        query = re.sub(r'&n=\d+', '', query)
        return query

    def log_query(self):
        """Do something with details of query -- ie log date"""
        pass

    @retry(ServerError, tries=10, delay=1)
    def _get_url(self, url):
        ''' Try to retrieve the supplied url.'''
        req = Request(url)
        try:
            response = urlopen(req)
        except HTTPError as e:
            if e.code == 503 or e.code == 504 or e.code == 500:
                raise ServerError("The server didn't respond")
            else:
                raise
        else:
            return response

    def harvest(self):
        number = self.number
        query_url = '{}&n={}&key={}'.format(
            self.query,
            self.number,
            self.trove_api.api_key
        )
        while (number == self.number) and (self.harvested < self.maximum):
            current_url = '{}&s={}'.format(
                query_url,
                self.harvested
            )
            print(current_url)
            response = self._get_url(current_url)
            try:
                reader = codecs.getreader('utf-8')  # For Python 3
                results = json.load(reader(response))
            except (AttributeError, ValueError):
                # Log errors?
                pass
            else:
                zones = results['response']['zone']
                self.process_results(zones)
                number = self.get_highest_n(zones)
                if not self.max_set:
                    self.maximum = self.get_highest_total(zones)

    def get_highest_n(self, zones):
        n = 0
        for zone in zones:
            new_n = int(zone['records']['n'])
            if new_n > n:
                n = new_n
        return n

    def get_highest_total(self, zones):
        total = 0
        for zone in zones:
            new_total = int(zone['records']['total'])
            if new_total > total:
                total = new_total
        return total

    def process_results(self, results):
        """
        Do something with each set of results.
        Needs to update self.harvested
        """
        self.harvested += self.number
