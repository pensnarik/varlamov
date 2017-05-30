#!/usr/bin/env python

import os
import sys
import logging
import hashlib
import shutil

import requests
from lxml.html import fromstring
from lxml import etree
from PIL import Image

logger = logging.getLogger('history')

class CacheConsumer(object):

    def get_cache_path(self):
        return os.path.join(os.path.dirname(sys.argv[0]), 'cache')

    def __init__(self):
        logger.info('CacheConsumer() initialization, path = %s' % self.get_cache_path())
        if not os.path.isdir(self.get_cache_path()):
            os.mkdir(self.get_cache_path())

    def get_cached_filename(self, url):
        return os.path.join(self.get_cache_path(), hashlib.md5(url).hexdigest())

    def get_file_size(self, url):
        statinfo = os.stat(self.get_cached_filename(url))
        return statinfo.st_size

    def is_in_cache(self, url):
        return os.path.exists(self.get_cached_filename(url)) and self.get_file_size(url) > 0

    def get_document(self, url):
        logger.info('Getting document: %s' % url)

        if self.is_in_cache(url):
            return open(self.get_cached_filename(url)).read()
        else:
            result = requests.get(url)

            if result.status_code == 200:
                logger.info(result.encoding)
                f = open(self.get_cached_filename(url), 'w')
                f.write(result.content)
                f.close()
                return result.text
            elif result.status_code == 404:
                return None
            else:
                raise Exception('Could not get page %s: %s' % (url, result.status_code))

    def get_binary_file(self, url):
        logger.info('Getting binary file: %s' % url)

        if not self.is_in_cache(url):
            try:
                result = requests.get(url, stream=True, timeout=10)
            except requests.ConnectionError as e:
                logger.error('Could not get file %s: %s' % (url, str(e)))
                return None

            if result.status_code == 200:
                with open(self.get_cached_filename(url), 'wb') as f:
                    result.raw.decode_content = True
                    shutil.copyfileobj(result.raw, f)
            else:
                return None

        return open(self.get_cached_filename(url)).read()

class App(CacheConsumer):

    url_template = 'http://varlamov.ru/%(year)s/%(month)02d'

    def __init__(self):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO, stream=sys.stdout)
        logger.setLevel(logging.INFO)

        super(App, self).__init__()

    def process_image(self, url):
        data = self.get_binary_file(url)

        if data is None:
            return False

        try:
            image = Image.open(open(self.get_cached_filename(url)))
        except IOError:
            logger.error('Could not read image %s' % url)
            return False

        logger.info('Image size: %s x %s' % (image.size))

    def process_post(self, url):
        page = self.get_document(url)

        if page is None:
            return False

        html = fromstring(page)

        content = html.xpath('//div[@id="entrytext"]')

        if len(content) < 1:
            return False

        print (etree.tostring(content[0], pretty_print=True, encoding='unicode'))

        for img in content[0].xpath('.//img'):
            print('\t%s' % img.get('src'))
            self.process_image(img.get('src'))


    def run(self, argv):
        for year in range(2006, 2018):
            for month in range(1, 13):
                print('%s/%s' % (month, year))

                page = self.get_document(self.url_template % locals())
                if page is None:
                    continue

                html = fromstring(page)

                logger.info('Searching')

                for a_item in html.xpath('//a[@class="j-day-subject-link"]'):
                    logging.info('PROESSING POST')
                    logging.info('%s / %s' % (a_item.get('href'), a_item.text_content()))
                    self.process_post(a_item.get('href'))


if __name__ == '__main__':
    app = App()
    sys.exit(app.run(sys.argv))
