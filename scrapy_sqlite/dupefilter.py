__author__ = 'Filip Hanes'

from time import time
import scrapy_sqlite.connection as connection

from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint


class RFPDupeFilter(BaseDupeFilter):
    """SQLite-based request duplication filter"""

    def __init__(self, conn, table):
        """Initialize duplication filter

        Parameters
        ----------
        conn : SQLite connection
        table : str
            Where to store fingerprints
        """
        self.conn = conn
        self.table = table

    @classmethod
    def from_crawler(cls, crawler):
        # create one-time table. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        conn = connection.from_crawler(crawler)
        table = "dupefilter_%s" % int(time())
        return cls(conn, table)

    def request_seen(self, request):
        fp = request_fingerprint(request)

        c = self.conn.execute('INSERT OR IGNORE INTO "%s" (fingerprint) VALUES (?)'%self.table, (fp,))
        self.conn.commit()

        return c.rowcount < 1

    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clear()

    def clear(self):
        """Clears fingerprints data"""
        self.conn.execute('DELETE FROM %s'%self.table)

