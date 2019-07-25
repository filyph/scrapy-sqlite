__author__ = 'Filip Hanes'

import scrapy_sqlite.connection as connection

from scrapy.utils.misc import load_object
from scrapy_sqlite.dupefilter import RFPDupeFilter
from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.request import request_fingerprint
import sqlite3
from time import sleep

try:
    import cPickle as pickle
except ImportError:
    import pickle

import logging
logger = logging.getLogger(__name__)

# default values
SCHEDULER_PERSIST = False
QUEUE_TABLE = 'http'
QUEUE_CLASS = 'scrapy_sqlite.queue.SpiderQueue'
DUPEFILTER_TABLE = '%(spider)s_dupefilter'
IDLE_BEFORE_CLOSE = 0

class Scheduler(object):
    """ A SQLite Scheduler for Scrapy.
    """

    def __init__(self, conn, persist, table, queue_cls, dupefilter_table, idle_before_close, stats, *args, **kwargs):
        self.conn = conn
        self.persist = persist
        self.table = table
        # queue is not used
        #self.queue_cls = queue_cls
        # dupefilter is not used because of UNIQUE index on column fingerprint
        #self.dupefilter_table = dupefilter_table
        self.idle_before_close = idle_before_close
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        persist = settings.get('SCHEDULER_PERSIST', SCHEDULER_PERSIST)
        table = settings.get('SCHEDULER_QUEUE_TABLE', QUEUE_TABLE)
        table = table % {'spider': crawler.spider.name}
        queue_cls = load_object(settings.get('SCHEDULER_QUEUE_CLASS', QUEUE_CLASS))
        dupefilter_table = settings.get('DUPEFILTER_TABLE', DUPEFILTER_TABLE)
        idle_before_close = settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', IDLE_BEFORE_CLOSE)

        conn = connection.from_crawler(crawler)
        instance = cls(conn, persist, table, queue_cls, dupefilter_table, idle_before_close, crawler.stats)
        return instance

    def open(self, spider):
        self.spider = spider

        if self.has_pending_requests():
            spider.log("Resuming crawl (%d requests scheduled)" % len(self))

    def close(self, reason):
        if not self.persist:
            # removes scheduled and downloading requests
            self.conn.execute('DELETE FROM "%s" WHERE state=? OR state=?' % self.table, \
                    (connection.SCHEDULED, connection.DOWNLOADING))
            # all downloaded requests sets as scheduled for next run
            self.conn.execute('UPDATE "%s" SET state=?' % self.table, \
                    (connection.SCHEDULED, ))

    def enqueue_request(self, request):
        request_dump = self._encode_request(request)
        fingerprint = request_fingerprint(request)

        # INSERT OR IGNORE acts as dupefilter, because column fingerprint is UNIQUE
        c = self.conn.cursor()
        self.begin_immediate_transaction(c)
        c.execute('''
            INSERT OR IGNORE INTO "%s"
                (url, fingerprint, priority, request, state) VALUES (?,?,?,?,?)''' % self.table,\
                (request.url, fingerprint, request.priority, request_dump, connection.SCHEDULED))

        # if request is not inserted (new) and we cannot filter it,
        # update state to be scheduled again
        if c.rowcount < 1 and request.dont_filter:
            c = self.conn.execute('UPDATE "%s" SET state = ? WHERE fingerprint=?' % self.table,\
                    (connection.SCHEDULED, fingerprint))
            if self.stats:
                self.stats.inc_value('scheduler/enqueued/sqlite', spider=self.spider)

        self.conn.commit()


    def begin_immediate_transaction(self, cursor):
        canwait = 30
        while canwait:
            try:
                cursor.execute('BEGIN IMMEDIATE TRANSACTION')
                return
            except sqlite3.OperationalError:
                logger.info('Database locked, waiting 1s...')
                canwait -= 1
                sleep(1);
        logger.debug('Waiting for sqlite3 db lock timeout 30 occured')

    def next_request(self):
        c = self.conn.cursor()
        self.begin_immediate_transaction(c)
        c.execute('''
                SELECT rowid, request, state, url, priority FROM "%s"
                WHERE state = ? ORDER BY priority DESC LIMIT 1''' % self.table, \
                (connection.SCHEDULED,))
        result = c.fetchone()
        if result:
            rowid, body, state, url, priority = result
            if body:
                c.execute('UPDATE "%s" SET state=? WHERE rowid=?'%self.table, \
                        (connection.DOWNLOADING, rowid))
                self.conn.commit()

                # decode request
                request_dict = pickle.loads(body)
                request_dict['url'] = url
                request_dict['priority'] = priority
                request = request_from_dict(request_dict, self.spider)

                if request and self.stats:
                    self.stats.inc_value('scheduler/dequeued/sqlite', spider=self.spider)
                return request

        self.conn.commit()

    def _encode_request(self, request):
        """Encode a request object"""
        request_dict = request_to_dict(request, self.spider)

        # url and priority are saved in table columns
        del request_dict['url']
        del request_dict['priority']

        return pickle.dumps(request_dict, protocol=-1)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""

    def has_pending_requests(self):
        c = self.conn.execute( \
            'SELECT * FROM "%s" WHERE state=? LIMIT 1'%self.table, \
            (connection.SCHEDULED,))
            # SELECT rowid FROM http WHERE state=1 LIMIT 1;
        return c.fetchone() is not None

    def __len__(self):
        """Return the number of scheduled requests"""
        c = self.conn.execute( \
            'SELECT COUNT(*) FROM "%s" WHERE state=?'%self.table, \
            (connection.SCHEDULED,))
        result = c.fetchone()
        if result:
            return int(result[0])

