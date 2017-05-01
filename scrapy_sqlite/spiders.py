__author__ = 'Filip Hanes'

import connection

from scrapy.spider import Spider
from scrapy import signals
from scrapy.exceptions import DontCloseSpider


class SQLiteMixin(object):
    """ A SQLite Mixin used to read URLs from a SQLite queue.
    """

    table = None

    def __init__(self):
        self.conn = None

    def setup_sqlite(self):
        """ Setup SQLite connection.

            Call this method after spider has set its crawler object.
        :return: None
        """

        if not self.table:
            self.table = '{}_start_urls'.format(self.name)

        self.conn = connection.from_settings(self.crawler.settings)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)

    def next_request(self):
        """ Provides a request to be scheduled.
        :return: Request object or None
        """

        self.conn.execute('BEGIN IMMEDIATE TRANSACTION'))
        self.conn.execute('SELECT rowid,url FROM ? LIMIT 1', (self.table)))
        rowid,url = c.fetchone()
        if url:
            self.conn.execute('DELETE FROM ? WHERE rowid = ?', (self.table, rowid))
            self.conn.commit()
            return self.make_requests_from_url(url)
        self.conn.commit()

    def schedule_next_request(self):
        """ Schedules a request, if exists.

        :return:
        """
        req = self.next_request()

        if req:
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """ Waits for request to be scheduled.

        :return: None
        """
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """ Avoid waiting for spider.
        :param args:
        :param kwargs:
        :return: None
        """
        self.schedule_next_request()


class SQLiteSpider(SQLiteMixin, Spider):
    """ Spider that reads urls from SQLite queue when idle.
    """

    def set_crawler(self, crawler):
        super(SQLiteSpider, self).set_crawler(crawler)
        self.setup_sqlite()
