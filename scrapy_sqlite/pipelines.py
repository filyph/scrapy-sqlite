__author__ = 'Filip Hanes'

import scrapy_sqlite.connection as connection

from twisted.internet.threads import deferToThread
from scrapy.utils.serialize import ScrapyJSONEncoder


class SQLitePipeline(object):
    """Pushes serialized item into a SQLite table"""

    def __init__(self, conn):
        self.conn = conn
        self.encoder = ScrapyJSONEncoder()
        #TODO: ensure table exists
        # c.execute('CREATE TABLE ? (data TEXT NULL)', (table,)

    @classmethod
    def from_crawler(cls, crawler):
        conn = connection.from_crawler(crawler)
        return cls(conn)

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        table = self.item_table(item, spider)
        data = self.encoder.encode(item)
        self.conn.execute('INSERT INTO "%s" VALUES (?)'%table, (data,))
        return item

    def items_table(self, item, spider):
        """Returns SQLite table name based on given spider"""
        return "%s_items" % spider.name
