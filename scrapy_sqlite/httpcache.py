from __future__ import print_function
import os
import gzip
import logging
from six.moves import cPickle as pickle
from time import time
from scrapy.http import Headers, Response
from scrapy.responsetypes import responsetypes
from scrapy.utils.request import request_fingerprint
from scrapy.utils.project import data_path

logger = logging.getLogger(__name__)

class SQLiteCacheStorage(object):

    def __init__(self, settings):
        import sqlite3
        self._sqlite3 = sqlite3
        self.cachedir = data_path(settings['HTTPCACHE_DIR'], createdir=True)
        self.expiration_secs = settings.getint('HTTPCACHE_EXPIRATION_SECS')
        self.conn = None
        self.table = 'httpcache'

    def open_spider(self, spider):
        dbpath = os.path.join(self.cachedir, '%s.sqlite3' % spider.name)
        self.table = '%s_requests' % spider.name
        self.conn = self._sqlite3.connect(dbpath)
        self.conn.execute("""CREATE TABLE ? IF NOT EXISTS (
                fingerprint TEXT UNIQUE,
                request BLOB,
                created INTEGER DEFAULT CURRENT_TIMESTAMP,
                response BLOB,
                downloaded INTEGER,
                state INTEGER)""", (self.table,))

        logger.debug("Using SQLite cache storage in %(cachepath)s" % {'cachepath': dbpath}, extra={'spider': spider})

    def close_spider(self, spider):
        self.conn.close()

    def retrieve_response(self, spider, request):
        data = self._read_data(spider, request)
        if data is None:
            return  # not cached
        url = data['url']
        status = data['status']
        headers = Headers(data['headers'])
        body = data['body']
        respcls = responsetypes.from_args(headers=headers, url=url)
        response = respcls(url=url, headers=headers, status=status, body=body)
        return response

    def store_response(self, spider, request, response):
        data = {
            'status': response.status,
            'url': response.url,
            'headers': dict(response.headers),
            'body': response.body,
        }
        c = self.conn.Cursor()
        c.execute('UPDATE ? SET response = ?, downloaded = CURRENT_TIMESTAMP WHERE fingerprint = ?', \
                (self.table, pickle.dumps(data, protocol=-1), request_fingerprint(request)))
        self.conn.commit()

    def _read_data(self, spider, request):
        fp = request_fingerprint(request)
        c = self.conn.Cursor()
        c.execute('SELECT rowid, downloaded, response FROM ? WHERE fingerprint = ?', \
                (self.table, fp))
        row = c.fetchone()
        if row:
            rowid, downloaded, response = row
        self.conn.commit()

        if 0 < self.expiration_secs < time() - downloaded:
            return  # expired

        return pickle.loads(response)

