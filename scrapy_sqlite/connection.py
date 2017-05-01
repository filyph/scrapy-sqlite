# -*- coding: utf-8 -*-

try:
    import sqlite3
except ImportError:
    raise ImportError("Please install sqlite3 before running scrapy-sqlite.")


SQLITE_QUEUE_NAME = 'scrapy_queue'
SQLITE_DATABASE = 'db.sqlite3'


connections = {}

def from_settings(settings):
    """ Factory method that returns an instance of connection

        :return: Connection object
    """

    queue_table = settings.get('SQLITE_QUEUE_TABLE', SQLITE_QUEUE_TABLE)
    sqlite_database = settings.get('SQLITE_DATABASE', SQLITE_DATABASE)

    global connections

    if sqlite_database not in connections:
        connections[sqlite_database] = sqlite3.connect(sqlite_database)

    return connections[sqlite_database]



