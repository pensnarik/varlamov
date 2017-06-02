#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import re
import sys
import logging
import hashlib
import shutil
import psycopg2
import argparse

import requests
from lxml.html import fromstring
from lxml import etree
from PIL import Image
from PIL.ExifTags import TAGS
from requests.packages.urllib3.exceptions import ReadTimeoutError
import exifread

logger = logging.getLogger('history')

class CacheConsumer(object):

    def get_cache_path(self):
        return os.path.join(os.path.dirname(sys.argv[0]), 'cache')

    def __init__(self):
        logger.info('CacheConsumer() initialization, path = %s' % self.get_cache_path())
        if not os.path.isdir(self.get_cache_path()):
            os.mkdir(self.get_cache_path())

        for i in range(0,16):
            if not os.path.isdir(os.path.join(self.get_cache_path(), '%x' % i)):
                os.mkdir(os.path.join(self.get_cache_path(), '%x' % i))

            for j in range(0,16):
                if not os.path.isdir(os.path.join(self.get_cache_path(), '%x' % i, '%x%x' % (i,j))):
                    os.mkdir(os.path.join(self.get_cache_path(), '%x' % i, '%x%x' % (i,j)))

    def get_cached_filename(self, url):
        hash = hashlib.md5(url).hexdigest()
        return os.path.join(self.get_cache_path(), hash[0], hash[0:2], hash)

    def get_cached_filename_compat(self, url):
        hash = hashlib.md5(url).hexdigest()
        return os.path.join(self.get_cache_path(), hash[0:2], hash)

    def get_file_size(self, url):
        statinfo = os.stat(self.get_cached_filename(url))
        return statinfo.st_size

    def is_in_cache(self, url):
        if os.path.exists(self.get_cached_filename_compat(url)):
            os.rename(self.get_cached_filename_compat(url), self.get_cached_filename(url))
            return self.get_file_size(url) > 0

        return os.path.exists(self.get_cached_filename(url)) and self.get_file_size(url) > 0

    def is_in_cache_error(self, url):
        return os.path.exists('%s.error' % self.get_cached_filename(url))

    def save_error_in_cache(self, url, error='ERROR'):
        with open('%s.error' % self.get_cached_filename(url), 'wt') as f:
            f.write(error)

    def get_file(self, url, stream=False, force_download=False):
        logger.info('Getting file: %s' % url)

        if self.is_in_cache_error(url):
            return None

        if self.is_in_cache(url) and force_download == False:
            return open(self.get_cached_filename(url)).read()
        else:
            try:
                result = requests.get(url, stream=stream, timeout=10)
            except Exception as e:
                logger.error('Could not get file %s: %s' % (url, str(e)))
                self.save_error_in_cache(url, 'DownloadException')
                return None

            if result.status_code == 200:
                if stream == True:
                    try:
                        with open(self.get_cached_filename(url), 'wb') as f:
                            result.raw.decode_content = True
                            shutil.copyfileobj(result.raw, f)
                        return open(self.get_cached_filename(url)).read()
                    except ReadTimeoutError as e:
                        logger.error('Exception while reading data: %s' % str(e))
                        os.remove(self.get_cached_filename(url))
                        self.save_error_in_cache('ReadTimeoutError')
                        return None
                else:
                    logger.info(result.encoding)
                    f = open(self.get_cached_filename(url), 'w')
                    f.write(result.content)
                    f.close()
                    return result.text
            else:
                self.save_error_in_cache('InvalidHTTPStatusCode: %s' % result.status_code)
                return None

    def get_document(self, url, force_download=False):
        return self.get_file(url, stream=False, force_download=force_download)

    def get_binary_file(self, url, force_download=False):
        return self.get_file(url, stream=True, force_download=force_download)

class App(CacheConsumer):

    url_template = 'http://varlamov.ru/%(year)s/%(month)02d'

    def __init__(self):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO, stream=sys.stdout)
        logger.setLevel(logging.INFO)

        super(App, self).__init__()

        parser = argparse.ArgumentParser(description='Generate SQL statemets to create '
                                                     'attribute tables.')
        parser.add_argument('--post', type=str, help='Post to parse')
        parser.add_argument('--from-year', type=int, help='Year to start parse from', default=2006)
        parser.add_argument('--from-month', type=int, help='Month to start parse from', default=1)
        parser.add_argument('--update', action='store_true', default=False, help='Do not use cache to construct post list')
        self.args = parser.parse_args()


        self.conn = psycopg2.connect('postgresql://postgres@127.0.0.1:20000/database')
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def process_image(self, post_id, url):
        def extract_tag(tags, tag_name):
            if tags.get(tag_name) is None or str(tags.get(tag_name)).strip() == '':
                return None
            return str(tags.get(tag_name))

        image_id = self.get_image_id(post_id, url)

        # Если информация об изображении есть в базе данных,
        # будем считать, что изображение обработано, - это ускоряет
        # обработку призапуске после неожиданной остановки
        if image_id is not None:
            return True

        data = self.get_binary_file(url)

        if data is None:
            return False

        fp = open(self.get_cached_filename(url), 'rb')

        try:
            image = Image.open(fp)
        except IOError:
            logger.error('Could not read image %s' % url)
            return False

        logger.info('Image size: %s x %s' % (image.size))

        fp.seek(0)
        try:
            tags = exifread.process_file(fp)
        except (UnicodeEncodeError, TypeError) as e:
            logger.error('Could not extract EXIF tags: %s' % str(e))
            tags = {}

        # logger.info('%s: %s' % (self.get_cached_filename(url),tags.keys()))
        # logger.info('%s' % tags.get('EXIF FocalLength'))
        # logger.info('%s' % tags.get('EXIF ExposureTime'))
        # logger.info('%s' % tags.get('EXIF DateTimeOriginal'))

        self.save_image({'post_id': post_id, 'url': url,
                         'width': image.size[0], 'height': image.size[1],
                         'file_size': self.get_file_size(url),
                         'exif_camera_model': extract_tag(tags, 'Image Model'),
                         'exif_focal_length': extract_tag(tags, 'EXIF FocalLength'),
                         'exif_exposure_time': extract_tag(tags, 'EXIF ExposureTime'),
                         'exif_date_time': extract_tag(tags, 'EXIF DateTimeOriginal'),
                         'exif_aperture_value': extract_tag(tags, 'EXIF FNumber'),
                         'exif_iso': extract_tag(tags, 'EXIF ISOSpeedRatings')})

    def get_date(self, date_as_string):
        month_mapping = {u'января': 1, u'февраля': 2, u'марта': 3, u'апреля': 4, u'мая': 5,
                         u'июня': 6, u'июля': 7, u'августа': 8, u'сентября': 9, u'октября': 10,
                         u'ноября': 11, u'декабря': 12}

        if date_as_string is None:
            return None
        if re.match('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', date_as_string):
            return date_as_string
        if ',' in date_as_string:
            # Попробуем интерпретировать дату как русскую дату
            date_parts = date_as_string.split(',')

            day_parts = date_parts[0].split(' ')

            if day_parts[1] not in month_mapping.keys():
                return None

            return '%s-%02d-%02dT%sZ' % (day_parts[2], month_mapping[day_parts[1]], int(day_parts[0]),
                                         date_parts[1].strip())
        else:
            return None

    def process_post(self, post):
        page = self.get_document(post['url'])

        if page is None:
            return False

        html = fromstring(page)

        content = html.xpath('//div[@id="entrytext"]')

        if len(content) < 1:
            return False

        # print (etree.tostring(content[0], pretty_print=True, encoding='unicode'))

        title = html.xpath('//title')

        if len(title) > 0:
            post['title'] = title[0].text_content()
        else:
            raise Exception('Could not find title')

        date_published = html.xpath('//time[@itemprop="datePublished"]')

        if len(date_published) > 0:
            post['date_published'] = date_published[0].text_content()
        else:
            date_published = html.xpath('//time[@itemprop="dateCreated"]')
            if len(date_published) > 0:
                post['date_published'] = date_published[0].text_content()
            else:
                post['date_published'] = None

        date_modified = html.xpath('//time[@itemprop="dateModified"]')

        if len(date_modified) > 0:
            post['date_modified'] = date_modified[0].text_content()
        else:
            post['date_modified'] = None

        post['date_modified'] = self.get_date(post['date_modified'])
        post['date_published'] = self.get_date(post['date_published'])

        post_id = self.save_post(post)

        for img in content[0].xpath('.//img'):
            url = img.get('src')

            if url is None or url.endswith('.ico') or url.endswith('.svg') or url.endswith('.gif'):
                continue

            try:
                url.decode('ascii')
            except UnicodeEncodeError:
                logger.error('Invalid ASCII symbol in URL, skipping')
                continue

            logger.info(url)
            self.process_image(post_id, url)

    def save_post(self, post):
        cursor = self.conn.cursor()

        query_check = '''
        select id from public.post where url = %(url)s
        '''

        query_insert = '''
        insert into public.post (url, title, date_published, date_modified)
        values (%(url)s, %(title)s, %(date_published)s, %(date_modified)s)
        returning id
        '''

        query_update = '''
        update post
           set date_modified = %(date_modified)s,
               date_published = %(date_published)s
         where id = %(id)s
        '''

        cursor.execute(query_check, post)
        result = cursor.fetchone()

        if result is None:
            cursor.execute(query_insert, post)
            result = cursor.fetchone()
            cursor.close()
            return result[0]
        else:
            post.update({'id': result[0]})
            cursor.execute(query_update, post)
            cursor.close()
            return result[0]

    def get_image_id(self, post_id, url):
        query_check = '''
        select id from public.image
         where post_id = %(post_id)s
           and url = %(url)s
        '''
        cursor = self.conn.cursor()
        cursor.execute(query_check, locals())
        result = cursor.fetchone()

        if result is None:
            cursor.close()
            return None
        else:
            cursor.close()
            return result[0]

    def save_image(self, image):
        cursor = self.conn.cursor()

        query_insert = '''
        insert into public.image (post_id, url, width, height, file_size,
                                  exif_camera_model, exif_focal_length,
                                  exif_exposure_time, exif_date_time, exif_aperture_value, exif_iso)
        values (%(post_id)s, %(url)s, %(width)s, %(height)s, %(file_size)s,
                %(exif_camera_model)s, %(exif_focal_length)s, %(exif_exposure_time)s,
                to_timestamp(%(exif_date_time)s, 'yyyy:mm:dd HH24:mi:ss'),
                %(exif_aperture_value)s, %(exif_iso)s)
        returning id
        '''

        image_id = self.get_image_id(image['post_id'], image['url'])

        if image_id is None:
            cursor.execute(query_insert, image)
            result = cursor.fetchone()
            cursor.close()
            return result[0]
        else:
            cursor.close()
            return result[0]

    def extract_posts_from_range(self):
        posts_count = 0

        for year in range(self.args.from_year, 2018):
            for month in range(self.args.from_month, 13):
                logger.info('%s/%s' % (month, year))

                page = self.get_document(self.url_template % locals(), force_download=self.args.update)
                if page is None:
                    continue

                html = fromstring(page)

                for a_item in html.xpath('//a[@class="j-day-subject-link"]'):
                    posts_count += 1
                    url = a_item.get('href')

                    try:
                        url.decode('ascii')
                    except UnicodeEncodeError:
                        logger.info('Non ASCII symbol in URL, skipping...')
                        continue

                    logging.info('PROCESSING POST %s' % posts_count)
                    logging.info('%s / %s' % (url, a_item.text_content()))

                    try:
                        self.process_post({'url': url})
                    except Exception as e:
                        logger.error('Could not parse post: %s' % str(e))

    def run(self, argv):

        if self.args.post is not None:
            self.process_post({'url': self.args.post})
        else:
            self.extract_posts_from_range()

        self.conn.close()


if __name__ == '__main__':
    app = App()
    sys.exit(app.run(sys.argv))
