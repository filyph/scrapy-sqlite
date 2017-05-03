# -*- coding: utf-8 -*-
__author__ = 'Filip Hanes'

try:
    import sqlite3
except ImportError:
    raise ImportError("Please install sqlite3 before running scrapy-sqlite.")


SQLITE_DATABASE = '%(spider)s.sqlite3'
SQLITE_REQUESTS_TABLE= '%(spider)s_requests'

# state codes
SCHEDULED = 1
DOWNLOADING = 2
DOWNLOADED = 3

connections = {}

def from_crawler(crawler):
    """ Factory method that returns an instance of connection
        from crawler

        :return: Connection object
    """

    spider = crawler.spider
    sqlite_database = crawler.settings.get('SQLITE_DATABASE', SQLITE_DATABASE)
    sqlite_database = sqlite_database % {'spider': spider.name}
    table = crawler.settings.get('SQLITE_REQUESTS_TABLE', SQLITE_REQUESTS_TABLE)
    table = table % {'spider': spider.name}

    global connections

    if sqlite_database not in connections:
        conn = connections[sqlite_database] = sqlite3.connect(sqlite_database)

        # ensure that main table used by scheduler, httpcache and dupefilter exists
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS "%s"(
                url TEXT,
                state INTEGER,
                downloaded INTEGER,
                fingerprint TEXT UNIQUE,
                request BLOB,
                response BLOB
                );

            CREATE INDEX IF NOT EXISTS request_state_index
                ON "%s" (state);
            """ % (table, table))
    return connections[sqlite_database]


def from_settings(settings):
    """ Factory method that returns an instance of connection
        from settings

        :return: Connection object
    """

    sqlite_database = settings.get('SQLITE_DATABASE', SQLITE_DATABASE)

    global connections

    if sqlite_database not in connections:
        connections[sqlite_database] = sqlite3.connect(sqlite_database)

    return connections[sqlite_database]

