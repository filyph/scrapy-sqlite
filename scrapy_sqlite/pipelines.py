__author__ = 'Filip Hanes'

import scrapy_sqlite.connection as connection

from twisted.internet.threads import deferToThread
from scrapy.utils.serialize import ScrapyJSONEncoder


class SQLitePipeline(object):
    """Pushes item into a SQLite table"""

    def __init__(self, conn):
        self.conn = conn
        self.encoder = ScrapyJSONEncoder()
        #TODO: ensure table exists
        # c.execute('CREATE TABLE ? (data TEXT NULL)', (table,)
        self.insert = 'INSERT or replace INTO "%s" ("%s") VALUES (%s)'
        self.tables = {}

    @classmethod
    def from_crawler(cls, crawler):
        conn = connection.from_crawler(crawler)
        return cls(conn)

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        table = self.item_table(item, spider)
        columns = list(item.keys())
        values = list(item.values())
        sql = insert % (table, '","'.join(columns), ','.join(['?']*len(columns)))
        try:
            c.execute(sql, values)
        except (sqlite3.ProgrammingError, e):
                self.ensure_schema(table, item, spider)
        return item

    def ensure_schema(self, table, item, spider):
        if table not in self.tables:
            # get existing columns from table schema
            c = self.conn.execute('PRAGMA table_info("%s")' % table, null)
            self.tables[table] = columns = {}
            while row = c.fetchone():
                columns[row['name']] = row['type']
            if not columns:
                self.conn.execute('CREATE TABLE "%s"')

        columns = self.tables[table]
        for key in item.keys():
            if k not in columns:
                # add new columns
                self.conn.execute('ALTER TABLE "%s" ADD COLUMN "%s"' % (table, k))
                columns[k] = ''

        return table

    def item_table(self, item, spider):
        table = item.__class_.__name__

