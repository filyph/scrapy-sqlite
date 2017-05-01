__author__ = 'roycehaynes'

from scrapy.dupefilter import BaseDupeFilter

import time
import connection

from scrapy.dupefilter import BaseDupeFilter
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
        self.conn.execute("""CREATE TABLE ? IF NOT EXISTS (
                fingerprint TEXT UNIQUE,
                request BLOB,
                created INTEGER DEFAULT CURRENT_TIMESTAMP,
                response BLOB,
                downloaded INTEGER,
                state INTEGER)""", (self.table,))

    @classmethod
    def from_settings(cls, settings):
        conn = connection.from_settings(settings)
        # create one-time table. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        table = "dupefilter_%s" % int(time.time())
        return cls(conn, table)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        fp = request_fingerprint(request)

        c = self.conn.Cursor()
        c.execute('INSERT INTO ? (fingerprint) VALUES (?)', (fp,))
        self.conn.commit()

        return c.rowcount < 1

    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clear()

    def clear(self):
        """Clears fingerprints data"""
        self.conn.execute('DELETE FROM ?', (self.table,))

