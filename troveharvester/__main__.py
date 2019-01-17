"""
TroveHarvester - A tool for harvesting digitised newspaper articles from Trove

Written in 2016 by Tim Sherratt tim@discontents.com.au

To the extent possible under law, the author(s) have dedicated all copyright and related and neighboring rights to this software to the public domain worldwide. This software is distributed without any warranty.

You should have received a copy of the CC0 Public Domain Dedication along with this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
"""

import time
import argparse
import os
import datetime
import arrow
from tqdm import tqdm
import json
from pprint import pprint
import re
import unicodecsv as csv
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
try:
    from urllib.parse import urlparse, parse_qsl
except ImportError:
    from urlparse import urlparse, parse_qsl

s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))
s.mount('https://', HTTPAdapter(max_retries=retries))

FIELDS = [
    'article_id',
    'title',
    'newspaper_id',
    'newspaper_title',
    'page',
    'date',
    'category',
    'words',
    'illustrated',
    'corrections',
    'url',
    'page_url'
]


class Harvester:
    zoom = 3
    api_url = 'https://api.trove.nla.gov.au/v2/result'

    def __init__(self, **kwargs):
        self.data_dir = kwargs.get('data_dir')
        self.csv_file = os.path.join(self.data_dir, 'results.csv')
        self.pdf = kwargs.get('pdf', False)
        self.text = kwargs.get('text', False)
        self.api_key = kwargs.get('key')
        self.query_params = kwargs.get('query_params', None)
        self.harvested = int(kwargs.get('harvested', 0))
        self.number = int(kwargs.get('number', 100))
        self.start = kwargs.get('start', '*')
        max_results = kwargs.get('max')
        if max_results:
            self.maximum = max_results
        else:
            self._get_total()

    def _get_total(self):
        params = self.query_params.copy()
        params['n'] = 0
        response = s.get(self.api_url, params=params)
        try:
            results = response.json()
        except (AttributeError, ValueError):
            print('No results!')
        else:
            self.maximum = int(results['response']['zone'][0]['records']['total'])

    def log_query(self):
        """Do something with details of query -- ie log date"""
        pass

    def harvest(self):
        number = self.number
        params = self.query_params.copy()
        params['n'] = self.number
        with tqdm(total=self.maximum, unit='article') as pbar:
            while self.start and (self.harvested < self.maximum):
                params['s'] = self.start
                # print(current_url)
                response = s.get(self.api_url, params=params)
                try:
                    results = response.json()
                except (AttributeError, ValueError):
                    # Log errors?
                    pass
                else:
                    records = results['response']['zone'][0]['records']
                    self.process_results(records, pbar)

    def update_meta(self, start):
        meta = get_metadata(self.data_dir)
        if meta:
            meta['start'] = start
        with open(os.path.join(self.data_dir, 'metadata.json'), 'w') as meta_file:
            json.dump(meta, meta_file, indent=4)

    def prepare_row(self, article):
        row = {}
        row['article_id'] = article['id']
        # Seems some articles don't have headings -- added 10 May 2018
        try:
            row['title'] = article['heading']
        except KeyError:
            row['title'] = ''
        row['newspaper_id'] = article['title']['id']
        row['newspaper_title'] = article['title']['value']
        row['page'] = article['pageSequence']
        row['date'] = article['date']
        row['category'] = article.get('category')
        row['words'] = article.get('wordCount')
        row['illustrated'] = article.get('illustrated')
        row['corrections'] = article.get('correctionCount')
        row['url'] = article.get('identifier')
        if 'trovePageUrl' in article:
            page_id = re.search(r'page\/(\d+)', article['trovePageUrl']).group(1)
            row['page_url'] = 'http://trove.nla.gov.au/newspaper/page/{}'.format(page_id)
        else:
            row['page_url'] = None
        return row

    def make_filename(self, article):
        date = article['date']
        date = date.replace('-', '')
        newspaper_id = article['title']['id']
        article_id = article['id']
        return '{}-{}-{}'.format(date, newspaper_id, article_id)

    def ping_pdf(self, ping_url):
        ready = False
        # req = Request(ping_url)
        try:
            # urlopen(req)
            response = s.get(ping_url, timeout=30)
            response.raise_for_status()
        except HTTPError:
            if response.status_code == 423:
                ready = False
            else:
                raise
        else:
            ready = True
        return ready

    def get_pdf_url(self, article_id, zoom=3):
        pdf_url = None
        prep_url = 'https://trove.nla.gov.au/newspaper/rendition/nla.news-article{}/level/{}/prep'.format(article_id, zoom)
        response = s.get(prep_url)
        prep_id = response.text
        ping_url = 'https://trove.nla.gov.au/newspaper/rendition/nla.news-article{}.{}.ping?followup={}'.format(article_id, zoom, prep_id)
        tries = 0
        ready = False
        time.sleep(2)  # Give some time to generate pdf
        while ready is False and tries < 2:
            ready = self.ping_pdf(ping_url)
            if not ready:
                tries += 1
                time.sleep(5)
        if ready:
            pdf_url = 'https://trove.nla.gov.au/newspaper/rendition/nla.news-article{}.{}.pdf?followup={}'.format(article_id, zoom, prep_id)
        return pdf_url

    def process_results(self, records, pbar):
        '''
        Processes a page full of results.
        Saves pdf for each result.
        '''
        try:
            articles = records['article']
            with open(self.csv_file, 'ab') as csv_file:
                writer = csv.DictWriter(csv_file, FIELDS, encoding='utf-8')
                if self.harvested == 0:
                    writer.writeheader()
                for article in articles:
                    article_id = article['id']
                    row = self.prepare_row(article)
                    writer.writerow(row)
                    if self.pdf:
                        pdf_url = self.get_pdf_url(article_id)
                        if pdf_url:
                            pdf_filename = self.make_filename(article)
                            pdf_file = os.path.join(self.data_dir, 'pdf', '{}.pdf'.format(pdf_filename))
                            response = s.get(pdf_url, stream=True)
                            with open(pdf_file, 'wb') as pf:
                                for chunk in response.iter_content(chunk_size=128):
                                    pf.write(chunk)
                    if self.text:
                        text = article.get('articleText')
                        if text:
                            text_filename = self.make_filename(article)
                            text = re.sub('<[^<]+?>', '', text)
                            text = re.sub("\s\s+", " ", text)
                            text_file = os.path.join(self.data_dir, 'text', '{}.txt'.format(text_filename))
                            with open(text_file, 'wb') as text_output:
                                text_output.write(text.encode('utf-8'))
                    pbar.update(1)
            time.sleep(0.5)
            self.harvested += int(records['n'])
            try:
                self.start = records['nextStart']
            except KeyError:
                self.start = None
            self.update_meta(self.start)
            # print('Harvested: {}'.format(self.harvested))
        except KeyError:
            raise


def format_date(date, start=False):
    if date != '*':
        date_obj = arrow.get(date)
        if start:
            date_obj = date_obj.shift(days=-1)
        date = '{}Z'.format(date_obj.format('YYYY-MM-DDT00:00:00'))
    return date


def prepare_query(query, text, api_key):
    if 'api.trove.nla.gov.au' in query:
        if text and 'articleText' not in query:
            query += '&include=articleText'
        return query
    else:
        safe = ['q', 'l-category', 'l-title', 'l-decade', 'l-year', 'l-month', 'l-state']  # Note l-month doesn't work in API -- returns 0 results
        new_params = {}
        dates = {}
        keywords = []
        parsed_url = urlparse(query)
        params = parse_qsl(parsed_url.query)
        for key, value in params:
            if key in safe:
                if key in new_params:
                    try:
                        new_params[key].append(value)
                    except AttributeError:
                        old_value = new_params[key]
                        new_params[key] = [old_value, value]
                else:
                    new_params[key] = value
            elif key == 'l-word':
                if '<100 Words' in value:
                    new_params[key] = '0'
                elif '100 - 1000 Words' in value:
                    new_params[key] = '1'
                elif '1000+ Words' in value:
                    new_params[key] = '3'
            elif key == 'l-advstate':
                if 'l-state' in new_params:
                    try:
                        new_params['l-state'].append(value)
                    except AttributeError:
                        old_value = new_params['l-state']
                        new_params['l-state'] = [old_value, value]
                else:
                    new_params['l-state'] = value
            elif key == 'l-illustrated':
                if value == 'true':
                    new_params[key] = 'y'
            elif key == 'l-advcategory':
                new_params['l-category'] = value
            elif key == 'l-advtitle':
                if 'l-title' in new_params:
                    try:
                        new_params['l-title'].append(value)
                    except AttributeError:
                        old_value = new_params['l-title']
                        new_params['l-title'] = [old_value, value]
                else:
                    new_params['l-title'] = value
            elif key == 'dateFrom':
                dates['from'] = value
            elif key == 'dateTo':
                dates['to'] = value
            elif key == 'exactPhrase':
                keywords.append('"{}"'.format(value))
            elif key == 'notWords':
                keywords.append('NOT ({})'.format(' OR '.join(value.split())))
            elif key == 'anyWords':
                keywords.append('({})'.format(' OR '.join(value.split())))
        if keywords:
            if 'q' in new_params:
                new_params['q'] += ' AND {}'.format(' AND '.join(keywords))
            else:
                new_params['q'] = ' AND '.join(keywords)
        if dates:
            if 'from' not in dates:
                dates['from'] = '*'
            if 'to' not in dates:
                dates['to'] = '*'
            date_query = 'date:[{} TO {}]'.format(format_date(dates['from'], True), format_date(dates['to']))
            if 'q' in new_params:
                new_params['q'] += ' {}'.format(date_query)
            else:
                new_params['q'] = date_query
        if 'q' not in new_params:
            new_params['q'] = ' '
        new_params['key'] = api_key
        new_params['encoding'] = 'json'
        new_params['zone'] = 'newspaper'
        new_params['reclevel'] = 'full'
        if text:
            new_params['include'] = 'articleText'
        # return '{}?{}'.format('https://api.trove.nla.gov.au/v2/result', urlencode(new_params, doseq=True))
        return new_params


def make_dir(dir):
    try:
        os.makedirs(dir)
    except OSError:
        if not os.path.isdir(dir):
            raise


def save_meta(args, data_dir, harvest):
    meta = {}
    meta['query'] = args.query
    meta['key'] = args.key
    meta['max'] = args.max
    meta['pdf'] = args.pdf
    meta['text'] = args.text
    meta['harvest'] = harvest
    meta['date_started'] = datetime.datetime.now().isoformat()
    meta['start'] = '*'
    with open(os.path.join(data_dir, 'metadata.json'), 'w') as meta_file:
        json.dump(meta, meta_file, indent=4)


def get_harvest(args):
    if args.harvest:
        harvest = args.harvest
    else:
        harvests = sorted(os.listdir(os.path.join(os.getcwd(), 'data')))
        harvest = harvests[-1]
    return harvest


def get_metadata(data_dir):
    try:
        with open(os.path.join(data_dir, 'metadata.json'), 'r') as meta_file:
            meta = json.load(meta_file)
    except IOError:
        print('No harvest!')
        meta = None
    return meta


def get_results(data_dir):
    results = {}
    try:
        with open(os.path.join(data_dir, 'results.csv'), 'rb') as csv_file:
                reader = csv.reader(csv_file, delimiter=',', encoding='utf-8')
                rows = list(reader)
                results['num_rows'] = len(rows) - 1
                results['last_row'] = rows[-1]
    except IOError:
        results['num_rows'] = 0
        results['last_row'] = None
    return results


def report_harvest(args):
    harvest = get_harvest(args)
    data_dir = os.path.join(os.getcwd(), 'data', harvest)
    meta = get_metadata(data_dir)
    if meta:
        results = get_results(data_dir)
        print('')
        print('HARVEST METADATA')
        print('================')
        print('Last harvest started: {}'.format(meta['date_started']))
        print('Harvest id: {}'.format(meta['harvest']))
        print('API key: {}'.format(meta['key']))
        print('Query: {}'.format(meta['query']))
        print('Max results: {}'.format(meta['max']))
        print('Include PDFs: {}'.format(meta['pdf']))
        print('Include text: {}'.format(meta['text']))
        print('')
        print('HARVEST PROGRESS')
        print('================')
        print('Articles harvested: {}'.format(results['num_rows']))
        print('Last article harvested:')
        print('')
        pprint(results['last_row'], indent=2)


def restart_harvest(args):
    harvest = get_harvest(args)
    data_dir = os.path.join(os.getcwd(), 'data', harvest)
    meta = get_metadata(data_dir)
    if meta:
        if meta['next_start']:
            start_harvest(data_dir=data_dir, key=meta['key'], query=meta['query'], pdf=meta['pdf'], text=meta['text'], start=meta['start'], max=meta['max'])
        else:
            print('Harvest completed')


def prepare_harvest(args):
    if args.action == 'report':
        report_harvest(args)
    elif args.action == 'restart':
        restart_harvest(args)
    else:
        harvest = str(int(time.time()))  # Get rid of fractions
        data_dir = os.path.join(os.getcwd(), 'data', harvest)
        make_dir(data_dir)
        save_meta(args, data_dir, harvest)
        if args.pdf:
            make_dir(os.path.join(data_dir, 'pdf'))
        if args.text:
            make_dir(os.path.join(data_dir, 'text'))
        start_harvest(data_dir=data_dir, key=args.key, query=args.query, pdf=args.pdf, text=args.text, start='*', max=args.max)


def start_harvest(data_dir, key, query, pdf, text, start, max):
    params = prepare_query(query, text, key)
    harvester = Harvester(query_params=params, data_dir=data_dir, pdf=pdf, text=text, start=start, max=max)
    harvester.harvest()


def main():
    parser = argparse.ArgumentParser(prog="troveharvester")
    subparsers = parser.add_subparsers(dest='action')
    parser_start = subparsers.add_parser('start', help='Start a new harvest')
    parser_start.add_argument('query', help='The url of the search you want to harvest')
    parser_start.add_argument('key', help='Your Trove API key')
    parser_restart = subparsers.add_parser('restart', help='Restart an unfinished harvest')
    parser_restart.add_argument('--harvest', help='Restart the harvest with this id (default is the most recent harvest)')
    parser_report = subparsers.add_parser('report', help='Report on a harvest')
    parser_report.add_argument('--harvest', help='Report on the harvest with this id (default is the most recent harvest)')
    parser_start.add_argument('--max', type=int, default=0, help='Maximum number of results to return')
    parser_start.add_argument('--pdf', action="store_true", help='Save PDFs of articles')
    parser_start.add_argument('--text', action="store_true", help='Save text contents of articles')
    args = parser.parse_args()
    prepare_harvest(args)


if __name__ == "__main__":
    main()
