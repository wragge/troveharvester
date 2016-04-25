try:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError
except ImportError:
    from urllib2 import urlopen, Request, HTTPError, URLError
import json
import re
import time


class Trove:
    api_key = ''

    def __init__(self, api_key):
        self.api_key = api_key

    def get_item(self, item_id=None, item_type=None, item_url=None):
        item = None
        if item_url:
            item_details = re.search(r'(work|article|list)\/(\d+)', item_url)
            item_id = item_details.group(2)
            item_type = item_details.group(1)
        if item_id and item_type:
            if item_type == 'work':
                item = TroveWork(self.api_key, item_id)
            elif item_type == 'article':
                item = TroveArticle(self.api_key, item_id)
            elif item_type == 'list':
                item = TroveList(self.api_key, item_id)
            elif item_type == 'contributor':
                item = TroveContributor(self.api_key, item_id)
            else:
                print('I don\'t recognise that item type!')
        else:
            print('I need more item details!')
        return item


class TroveItem:
    '''
    Base class -- don't use directly.
    '''
    api_key = ''
    item_id = ''
    api_url = ''
    item_type = ''
    record = {}
    title_field = ''

    def __init__(self, api_key, item_id):
        self.api_key = api_key
        self.item_id = item_id
        url = self.api_url.format(item_id, self.api_key)
        response = self.get_url(url)
        if response:
            data = json.load(response)
            self.record = data[self.item_type]

    def _check_if_list(self, value):
        '''
        Sometimes values are in lists, and sometimes not.
        This makes everything into a list.
        '''
        if not isinstance(value, list):
            value = [value]
        return value

    def _check_if_dict(self, value):
        '''
        Checks to see if a field contains a dict.
        If so, looks for and returns the 'value' value.
        '''
        if isinstance(value, dict):
            if 'value' in value:
                value = value['value']
            else:
                value = list(value.values())[0]
        try:
            value = str(value.encode('utf8'))
        except AttributeError:
            value = str(value)
        return value

    def _prepare_value(self, value):
        new_values = []
        values = self._check_if_list(value)
        for val in values:
            new_values.append(self._check_if_dict(val))
        return new_values

    def list_fields(self):
        '''
        Gets a list of the field names or the record.
        '''
        return list(self.record.keys())

    def get_record(self):
        '''
        Gets the record object.
        '''
        return self.record

    def get_title(self):
        if self.title_field:
            title = self.get_field(self.title_field)[0]
        else:
            print('Title field is not set!')
            title = None
        return title

    def get_field(self, field):
        try:
            value = self.record[field]
        except KeyError:
            print('No such field!')
            value = None
        else:
            value = self._prepare_value(value)
        return value

    def get_url(self, url):
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


class TroveArticle(TroveItem):
    '''
    Newspaper articles
    '''
    api_url = 'http://api.trove.nla.gov.au/newspaper/{}?reclevel=full&include=articletext&encoding=json&key={}'
    item_type = 'article'
    title_field = 'heading'

    def ping_pdf(self, ping_url):
        ready = False
        req = Request(ping_url)
        try:
            urlopen(req)
        except HTTPError as e:
            if e.code == 423:
                ready = False
            else:
                raise
        else:
            ready = True
        return ready

    def get_pdf_url(self, zoom=3):
        pdf_url = None
        prep_url = 'http://trove.nla.gov.au/newspaper/rendition/nla.news-article{}/level/{}/prep'.format(self.item_id, zoom)
        response = self.get_url(prep_url)
        prep_id = response.read()
        ping_url = 'http://trove.nla.gov.au/newspaper/rendition/nla.news-article{}.{}.ping?followup={}'.format(self.item_id, zoom, prep_id)
        tries = 0
        ready = False
        time.sleep(2)  # Give some time to generate pdf
        while ready is False and tries < 2:
            ready = self.ping_pdf(ping_url)
            tries += 1
            time.sleep(5)
        if ready:
            pdf_url = 'http://trove.nla.gov.au/newspaper/rendition/nla.news-article{}.{}.pdf?followup={}'.format(self.item_id, zoom, prep_id)
        return pdf_url

    def get_text(self):
        text = self.record['articleText']
        text = re.sub('<[^<]+?>', '', text)
        text = re.sub("\s\s+", " ", text)
        return text


class TroveWork(TroveItem):
    '''
    Books, articles, pictures, maps, archives, sound & video
    '''
    api_url = 'http://api.trove.nla.gov.au/work/{}?reclevel=full&include=workVersions&include=tags&encoding=json&key={}'
    item_type = 'work'
    title_field = 'title'

    def _get_version_record_metadata(self, record):
        if 'metadata' in record:
            if 'record' in record['metadata']:
                records = self._check_if_list(record['metadata']['record'])
            elif 'dc' in record['metadata']:
                records = self._check_if_list(record['metadata']['dc'])
        else:
            records = self._check_if_list(record)
        return records

    def get_version(self, version_id):
        version = None
        for ver in self._check_if_list(self.record['version']):
            if ver['id'] == version_id:
                version = ver
                break
        return version

    def get_version_records(self, version=None, version_id=None):
        '''
        Version metadata is sometimes buried.
        '''
        new_records = []
        if version_id and not version:
            version = self.get_version(version_id)
        if version:
            records = self._check_if_list(version['record'])
            for record in records:
                records = self._get_version_record_metadata(record)
                new_records.extend(records)
        return new_records

    def get_first_version_records(self):
        versions = self._check_if_list(self.record['version'])
        records = self.get_version_records(version=versions[0])
        return records

    def get_field_first_version(self, field):
        value = None
        records = self.get_first_version_records()
        for record in records:
            if field in record:
                value = self._prepare_value(record[field])
                break
        return value

    def get_urls(self, record=None):
        urls = {}
        if not record:
            record = self.record
        if 'identifier' in record:
            identifiers = self._check_if_list(record['identifier'])
            for identifier in identifiers:
                try:
                    if ('type' not in identifier) or ('type' in identifier and identifier['type'] == 'url'):
                        try:
                            urls[identifier['linktype']] = identifier['value']
                        except TypeError:  # ignore non-dictionaries
                            pass
                except TypeError:  # ignore non-dictionaries
                    pass
        if 'identifier.url.mediumresolution' in record:
            urls['mediumresolution'] = record['identifier.url.mediumresolution']
        # There can be extra urls (like medium res) in versions.
        # So if there's only one version we'll grab them all from there as well.
        if 'versionCount' in record and record['versionCount'] == 1:
            versions = self._check_if_list(record['version'])
            urls.update(self.get_version_urls(versions[0]))
        return urls

    def get_version_urls(self, version=None, version_id=None):
        urls = {}
        if version_id and not version:
            version = self.get_version(version_id)
        records = self.get_version_records(version)
        for record in records:
            urls.update(self.get_urls(record))
            urls.update(self.get_related_urls(record))
        return urls

    def get_related_urls(self, record):
        urls = {}
        if 'relation' in record:
            for relation in self._check_if_list(record['relation']):
                if re.match(r'http.+\.pdf$', relation):
                    urls.update({'fulltext': relation})
        return urls

    def get_pdf_url(self, record=None):
        if not record:
            record = self.record
        urls = self.get_urls(record)
        related_urls = self.get_related_urls(record)
        if 'fulltext' in urls and re.search(r'\.pdf$', urls['fulltext']):
            pdf_url = urls['fulltext']
        elif related_urls:
            pdf_url = related_urls['fulltext']
        else:
            pdf_url = None
        return pdf_url

    def _prepare_value(self, value):
        new_values = []
        values = self._check_if_list(value)
        for val in values:
            new_values.append(self._check_if_dict(val))
        return new_values

    def get_repository(self):
        repository = None
        nuc = None
        versions = self._check_if_list(self.record['version'])
        records = self._check_if_list(versions[0]['record'])
        for record in records:
            if 'metadataSource' in record:
                source = record['metadataSource']
                try:
                    if 'type' in source and source['type'] == 'nuc':
                        nuc = source['value']
                    else:
                        repository = source
                except TypeError:
                    repository = source
                break
        return {'repository': repository, 'nuc': nuc}

    def _get_record_tags(self, all_tags, record):
        tags = self._check_if_list(record['tag'])
        for tag in tags:
            if tag['value'] not in all_tags:
                all_tags.append(tag['value'])
        return all_tags

    def get_all_tags(self):
        '''
        Gets all tags from work and versions.
        '''
        all_tags = []
        if 'tag' in self.record:
            all_tags = self._get_record_tags(all_tags, self.record)
        if 'version' in self.record:
            versions = self._check_if_list(self.record['version'])
            for version in versions:
                if 'tag' in version:
                    all_tags = self._get_record_tags(all_tags, version)
        return all_tags

    def get_details(self):
        '''
        Try to provide a useful summary.
        All values are returned as lists.
        '''
        details = {}
        # Main work fields
        work_fields = [
            'id',
            'title',
            'troveUrl',
            'contributor',
            'issued',
            'type',
            'isPartOf',
            'language',
            'abstract',
            'subject'
        ]
        # Useful fields at version level
        version_fields = [
            'publisher',
        ]
        # Useful fields in the bibliographicCitation field
        citation_fields = [
            'edition',
            'pagination',
            'rights',
            'isPartOf'
        ]
        record = self.record
        for field in work_fields:
            value = self.get_field(field)
            if value:
                details[field] = value
        if record['versionCount'] == 1:
            for field in version_fields:
                value = self.get_field_first_version(field)
                if value:
                    details[field] = value
            version_records = self.get_first_version_records()
            for version in version_records:
                if 'bibliographicCitation' in version:
                    citations = self._check_if_list(version['bibliographicCitation'])
                    for citation in citations:
                        try:
                            if citation['type'] in citation_fields:
                                details[citation['type']] = self._prepare_value(citation)
                        except TypeError:
                            pass
        urls = self.get_urls()
        if urls:
            details['urls'] = urls
        pdf_url = self.get_pdf_url()
        if pdf_url:
            details['pdf_url'] = pdf_url
        details['source'] = 'Trove'
        return details


class TroveList(TroveItem):
    api_url = 'http://api.trove.nla.gov.au/list/{}/?encoding=json&reclevel=full&include=all&key={}'
    item_type = 'list'
    title_field = 'title'
    list_items = []

    def __init__(self, api_key, item_id):
        self.api_key = api_key
        self.item_id = item_id
        url = self.api_url.format(item_id, self.api_key)
        response = self.get_url(url)
        if response:
            data = json.load(response)
            self.record = data[self.item_type][0]
            self.list_items = self.record['listItem']


class TroveContributor(TroveItem):
    api_url = 'http://api.trove.nla.gov.au/contributor/{}?encoding=json&reclevel=full&key={}'
    item_type = 'contributor'
    title_field = 'name'

