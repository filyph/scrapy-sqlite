__author__ = 'Filip Hanes'

import connection

from scrapy.utils.misc import load_object
from scrapy_sqlite.dupefilter import RFPDupeFilter

# default values
SCHEDULER_PERSIST = False
QUEUE_TABLE = '%(spider)s_requests'
QUEUE_CLASS = 'scrapy_sqlite.queue.SpiderQueue'
DUPEFILTER_TABLE = '%(spider)s_dupefilter'
IDLE_BEFORE_CLOSE = 0


class Scheduler(object):
    """ A SQLite Scheduler for Scrapy.
    """

    def __init__(self, conn, persist, queue_table, queue_cls, dupefilter_table, idle_before_close, *args, **kwargs):
        self.conn = conn
        self.persists = persists
        self.queue_table = queue_table
        self.queue_cls = queue_cls
        self.dupefilter_table = dupefilter_table
        self.idle_before_close = idle_before_close
        self.stats = None

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        persist = settings.get('SCHEDULER_PERSIST', SCHEDULER_PERSIST)
        queue_table = settings.get('SCHEDULER_QUEUE_TABLE', QUEUE_TABLE)
        queue_cls = load_object(settings.get('SCHEDULER_QUEUE_CLASS', QUEUE_CLASS))
        dupefilter_table = settings.get('DUPEFILTER_TABLE', DUPEFILTER_TABLE)
        idle_before_close = settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', IDLE_BEFORE_CLOSE)
        conn = connection.from_settings(settings)
        return cls(conn, persist, queue_table, queue_cls, dupefilter_table, idle_before_close)

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls.from_settings(crawler.settings)
        instance.stats = crawler.stats
        return instance

    def open(self, spider):
        self.spider = spider
        self.queue = self.queue_cls(self.conn, spider, self.queue_table)
        self.df = RFPDupeFilter(self.conn, self.dupefilter_table % {'spider': spider.name})

        if self.idle_before_close < 0:
            self.idle_before_close = 0

        if len(self.queue):
            spider.log("Resuming crawl (%d requests scheduled)" % len(self.queue))

    def close(self, reason):
        if not self.persist:
            self.df.clear()
            self.queue.clear()

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            return
        if self.stats:
            self.stats.inc_value('scheduler/enqueued/sqlite', spider=self.spider)
        self.queue.push(request)

    def next_request(self):
        block_pop_timeout = self.idle_before_close
        request = self.queue.pop()
        if request and self.stats:
            self.stats.inc_value('scheduler/dequeued/sqlite', spider=self.spider)
        return request

    def has_pending_requests(self):
        return len(self) > 0
