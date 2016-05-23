"""
TroveHarvester - A tool for harvesting digitised newspaper articles from Trove

Written in 2016 by Tim Sherratt tim@discontents.com.au

To the extent possible under law, the author(s) have dedicated all copyright and related and neighboring rights to this software to the public domain worldwide. This software is distributed without any warranty.

You should have received a copy of the CC0 Public Domain Dedication along with this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
"""

from . import trove
from .harvest import TroveHarvester, ServerError
# import urllib
import time
import argparse
import os
import datetime
import json
from pprint import pprint
# import urlparse
# from urllib2 import urlopen, Request, HTTPError, URLError
# from urllib import urlencode
import re
from .utilities import retry
import unicodecsv as csv

try:
    from urllib.request import urlopen, Request, urlretrieve
    from urllib.parse import urlparse, parse_qsl, urlencode
    from urllib.error import HTTPError, URLError
except ImportError:
    from urlparse import urlparse, parse_qsl
    from urllib2 import urlopen, Request, HTTPError, URLError
    from urllib import urlencode, urlretrieve

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
    'url'
]

STATES = {
    'Victoria': 'vic',
    'New South Wales': 'nsw',
    'South Australia': 'sa',
    'Queensland': 'qld',
    'Tasmania': 'tas',
    'Western Australia': 'wa',
    'ACT': 'act',
    'Northern Territory': 'nt',
    'National': 'national'
}


class Harvester(TroveHarvester):
    zoom = 3
    pdf = False
    text = False

    def __init__(self, trove_api, **kwargs):
        self.data_dir = kwargs.get('data_dir')
        self.csv_file = os.path.join(self.data_dir, 'results.csv')
        self.pdf = kwargs.get('pdf')
        self.text = kwargs.get('text')
        TroveHarvester.__init__(self, trove_api, **kwargs)

    def prepare_row(self, article):
        row = {}
        row['article_id'] = article['id']
        row['title'] = article['heading']
        row['newspaper_id'] = article['title']['id']
        row['newspaper_title'] = article['title']['value']
        row['page'] = article['pageSequence']
        row['date'] = article['date']
        row['category'] = article.get('category')
        row['words'] = article.get('wordCount')
        row['illustrated'] = article.get('illustrated')
        row['corrections'] = article.get('correctionCount')
        row['url'] = article.get('identifier')
        return row

    def make_filename(self, article):
        date = article['date']
        date = date.replace('-', '')
        newspaper_id = article['title']['id']
        article_id = article['id']
        return '{}-{}-{}'.format(date, newspaper_id, article_id)

    @retry(ServerError, tries=10, delay=1)
    def ping_pdf(self, ping_url):
        ready = False
        req = Request(ping_url)
        try:
            urlopen(req)
        except HTTPError as e:
            if e.code == 423:
                ready = False
            else:
                raise ServerError("The server didn't respond")
        else:
            ready = True
        return ready

    def get_pdf_url(self, article_id, zoom=3):
        pdf_url = None
        prep_url = 'http://trove.nla.gov.au/newspaper/rendition/nla.news-article{}/level/{}/prep'.format(article_id, zoom)
        response = get_url(prep_url)
        prep_id = response.read().decode()
        ping_url = 'http://trove.nla.gov.au/newspaper/rendition/nla.news-article{}.{}.ping?followup={}'.format(article_id, zoom, prep_id)
        tries = 0
        ready = False
        time.sleep(2)  # Give some time to generate pdf
        while ready is False and tries < 2:
            ready = self.ping_pdf(ping_url)
            if not ready:
                tries += 1
                time.sleep(5)
        if ready:
            pdf_url = 'http://trove.nla.gov.au/newspaper/rendition/nla.news-article{}.{}.pdf?followup={}'.format(article_id, zoom, prep_id)
        return pdf_url

    def process_results(self, results):
        '''
        Processes a page full of results.
        Saves pdf for each result.
        '''
        try:
            articles = results[0]['records']['article']
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
                            urlretrieve(pdf_url, pdf_file)
                    if self.text:
                        text = article.get('articleText')
                        if text:
                            text_filename = self.make_filename(article)
                            text = re.sub('<[^<]+?>', '', text)
                            text = re.sub("\s\s+", " ", text)
                            text_file = os.path.join(self.data_dir, 'text', '{}.txt'.format(text_filename))
                            with open(text_file, 'wb') as text_output:
                                text_output.write(text.encode('utf-8'))
            time.sleep(0.5)
            self.harvested += self.get_highest_n(results)
            print('Harvested: {}'.format(self.harvested))
        except KeyError:
            pass


@retry((HTTPError, URLError), tries=10, delay=1)
def get_url(url):
        response = None
        req = Request(url)
        try:
            response = urlopen(req)
        except HTTPError as e:
            print('The server couldn\'t fulfill the request.')
            print('Error code: {}'.format(e.code))
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: {}'.format(e.reason))
        return response


def get_titles(value, key):
    title_ids = []
    state = STATES[value]
    url = 'http://api.trove.nla.gov.au/newspaper/titles?state={}&encoding=json&key={}'.format(state, key)
    # print(url)
    response = get_url(url)
    if response:
        data = json.load(response)
        titles = data['response']['records']['newspaper']
        for title in titles:
            title_ids.append(title['id'])
    return title_ids


def prepare_query(query, text, api_key):
    if 'api.trove.nla.gov.au' in query:
        if text and 'articleText' not in query:
            query += '&include=articleText'
        return query
    else:
        safe = ['q', 'l-category', 'l-title']
        new_params = {}
        dates = {}
        keywords = []
        parsed_url = urlparse(query)
        params = parse_qsl(parsed_url.query)
        for key, value in params:
            if key in safe:
                new_params[key] = value
            elif key == 'l-word':
                if '<100 Words' in value:
                    new_params[key] = '0'
                elif '100 - 1000 Words' in value:
                    new_params[key] = '1'
                elif '1000+ Words' in value:
                    new_params[key] = '3'
            elif key in ['l-state', 'l-advstate']:
                title_ids = get_titles(value, api_key)
                new_params['l-title'] = title_ids
            elif key == 'l-illustrated':
                if value == 'true':
                    new_params[key] = 'y'
            elif key == 'l-advcategory':
                new_params['l-category'] = value
            elif key == 'dateFrom':
                dates['from'] = value[:4]
            elif key == 'dateTo':
                dates['to'] = value[:4]
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
            if 'q' in new_params:
                new_params['q'] += ' date:[{} TO {}]'.format(dates['from'], dates['to'])
            else:
                new_params['q'] = 'date:[{} TO {}]'.format(dates['from'], dates['to'])
        if 'q' not in new_params:
            new_params['q'] = ' '
        new_params['encoding'] = 'json'
        new_params['zone'] = 'newspaper'
        new_params['reclevel'] = 'full'
        if text:
            new_params['include'] = 'articleText'
        return '{}?{}'.format('http://api.trove.nla.gov.au/result', urlencode(new_params, doseq=True))


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
        try:
            with open(os.path.join(data_dir, 'results.csv'), 'rb') as csv_file:
                reader = csv.reader(csv_file, delimiter=',', encoding='utf-8')
                rows = list(reader)
            if len(rows) > 1:
                start = len(rows) - 2
                # Remove the last row in the CSV just in case there was a problem
                rows = rows[:-1]
                with open(os.path.join(data_dir, 'results.csv'), 'wb') as csv_file:
                    writer = csv.writer(csv_file, delimiter=',', encoding='utf-8')
                    for row in rows:
                        writer.writerow(row)
            else:
                start = 0
        except IOError:
            # Nothing's been harvested
            start = 0
        start_harvest(data_dir=data_dir, key=meta['key'], query=meta['query'], pdf=meta['pdf'], text=meta['text'], start=start, max=meta['max'])


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
        start_harvest(data_dir=data_dir, key=args.key, query=args.query, pdf=args.pdf, text=args.text, start=0, max=args.max)


def start_harvest(data_dir, key, query, pdf, text, start, max):
    api_query = prepare_query(query, text, key)
    trove_api = trove.Trove(key)
    harvester = Harvester(trove_api, query=api_query, data_dir=data_dir, pdf=pdf, text=text, start=start, max=max)
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

