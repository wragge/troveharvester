import re
import json
from .utilities import retry
import codecs
import requests
from tqdm import tqdm
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout


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
    next_start = '*'

    def __init__(self, trove_api, **kwargs):
        self.trove_api = trove_api
        query = kwargs.get('query', None)
        if query:
            self.query = self._clean_query(query)
        self.harvested = int(kwargs.get('harvested', 0))
        self.number = int(kwargs.get('number', 100))
        #self.next_start = kwargs.get('next_start')
        max_results = kwargs.get('max')
        if max_results:
            self.maximum = max_results
        else:
            self._get_total()

    def _get_total(self):
        query_url = '{}&n={}&key={}'.format(
            self.query,
            0,
            self.trove_api.api_key
        )
        response = self._get_url(query_url)
        try:
            results = response.json()
        except (AttributeError, ValueError):
            print('No results!')
        else:
            self.maximum = int(results['response']['zone'][0]['records']['total'])

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
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except HTTPError:
            raise ServerError('The server couldn\'t fulfill the request. Error code: {}.'.format(response.status_code))
        except ConnectionError:
            raise ServerError('We failed to reach a server.')
        except Timeout:
            raise ServerError('The server took too long to respond.')
        return response

    def harvest(self):
        number = self.number
        query_url = '{}&n={}&key={}'.format(
            self.query,
            self.number,
            self.trove_api.api_key
        )
        with tqdm(total=self.maximum) as pbar:
            while self.next_start and (self.harvested < self.maximum):
                current_url = '{}&s={}'.format(
                    query_url,
                    self.next_start
                )
                # print(current_url)
                response = self._get_url(current_url)
                try:
                    # reader = codecs.getreader('utf-8')  # For Python 3
                    # results = json.load(reader(response))
                    results = response.json()
                except (AttributeError, ValueError):
                    # Log errors?
                    pass
                else:
                    records = results['response']['zone'][0]['records']
                    try:
                        self.next_start = records['nextStart']
                    except KeyError:
                        self.next_start = None
                    self.process_results(records)
                    pbar.update(int(records['n']))

    def process_results(self, results):
        """
        Do something with each set of results.
        Needs to update self.harvested
        """
        self.harvested += self.number
