from __future__ import print_function

__author__ = 'Filip Hanes'

import os
import gzip
from six.moves import cPickle as pickle
from time import time
from scrapy.http import Headers, Response
from scrapy.responsetypes import responsetypes
from scrapy.utils.request import request_fingerprint
from scrapy.utils.project import data_path
import sqlite3
from time import sleep
import scrapy_sqlite.connection as connection

import logging
logger = logging.getLogger(__name__)

class SQLiteCacheStorage(object):

    def __init__(self, settings):
        self.cachedir = data_path(settings['HTTPCACHE_DIR'], createdir=True)
        self.sqlite_database = settings['SQLITE_DATABASE']
        self.table = settings.get('SQLITE_REQUESTS_TABLE', connection.SQLITE_REQUESTS_TABLE)
        self.database = settings.get('SQLITE_DATABASE', connection.SQLITE_DATABASE)
        self.expiration_secs = settings.getint('HTTPCACHE_EXPIRATION_SECS')

        self.use_gzip = settings.getbool('HTTPCACHE_GZIP')
        if self.use_gzip:
            self._loads = self._gzip_loads
            self._dumps = self._gzip_dumps
        else:
            self._loads = self._pickle_loads
            self._dumps = self._pickle_dumps

        self.conn = None

    def open_spider(self, spider):
        self.table = self.table % {'spider': spider.name}
        self.database = self.database % {'spider': spider.name}
        self.conn = connection.from_crawler(spider.crawler)

        dbpath = self.database+":"+self.table
        #dbpath = os.path.join(self.cachedir, '%s.sqlite3' % spider.name)

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
        response_dump = self._dumps(data)
        fingerprint = request_fingerprint(request)
        c = self.conn.cursor()
        self.begin_immediate_transaction(c)
        c.execute( \
            'UPDATE "%s" SET response=?, downloaded=?, state=? WHERE fingerprint=?'%self.table, \
            (response_dump, int(time()), connection.DOWNLOADED, fingerprint))
        if c.rowcount < 1:
            c = self.conn.execute('''
                INSERT INTO "%s" (url, fingerprint, response, downloaded, state)
                        VALUES (?, ?, ?, ?, ?);
                ''' % self.table, (request.url, fingerprint, response_dump, int(time()), connection.DOWNLOADED))

        #logger.debug("cursor.rowcount = %s" % (c.rowcount,), extra={'spider': spider})
        self.conn.commit()

    def begin_immediate_transaction(self, c):
        canwait = 30
        while canwait:
            try:
                c.execute('BEGIN IMMEDIATE TRANSACTION')
                return
            except sqlite3.OperationalError:
                logger.info('Database locked, waiting 1s...')
                canwait -= 1
                sleep(1);
        logger.debug('Waiting for sqlite3 db lock timeout 30 occured')

    def _read_data(self, spider, request):
        fp = request_fingerprint(request)
        c = self.conn.execute('''SELECT rowid, response, downloaded FROM "%s"
            WHERE fingerprint = ? AND downloaded IS NOT NULL'''%self.table, \
            (fp,))
        row = c.fetchone()
        if row:
            rowid, response, downloaded = row

            if 0 < self.expiration_secs < time() - downloaded:
                return  # expired

            return self._loads(response)

    def _gzip_dumps(self, data):
        return gzip.compress(self._pickle_dumps(data))

    def _gzip_loads(self, data):
        return self._pickle_loads(gzip.decompress(data))

    def _pickle_dumps(self, data):
        return pickle.dumps(data, protocol=-1)

    def _pickle_loads(self, data):
        return pickle.loads(data)

