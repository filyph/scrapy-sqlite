from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.request import request_fingerprint

try:
    import cPickle as pickle
except ImportError:
    import pickle


class Base(object):
    """Per-spider queue/stack base class"""

    def __init__(self, conn, spider, table):
        """Initialize per-spider SQLite queue.

        Parameters:
            conn -- sqlite connection
            spider -- spider instance
            table -- table for this queue (e.g. "%(spider)s_queue")
        """
        self.conn = conn
        self.spider = spider
        self.table = table % {'spider': spider.name}
        self.SCHEDULED = 1
        self.DOWNLOADING = 2
        self.DOWNLOADED = 3

        self.conn.execute("""CREATE TABLE ? IF NOT EXISTS (
                fingerprint TEXT UNIQUE,
                request BLOB,
                created INTEGER DEFAULT CURRENT_TIMESTAMP,
                response BLOB,
                downloaded INTEGER,
                state INTEGER)""")

    def _encode_request(self, request):
        """Encode a request object"""
        return pickle.dumps(request_to_dict(request, self.spider), protocol=-1)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        return request_from_dict(pickle.loads(encoded_request), self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear table"""
        self.conn.execute('DELETE FROM ?', (self.table,))


class SpiderQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        self.conn.execute('SELECT COUNT(*) FROM ?'% (self.table)))
        count, = c.fetchone()

        return count

    def push(self, request):
        """Push a request"""
        self.conn.execute('REPLACE INTO ? (fingerprint, request, state) VALUES (?,?,?)',\
                (self.table, request_fingerprint(request), self._encode_request(request), self.SCHEDULED))
        self.conn.commit()

    def pop(self):
        """Pop a request"""

        c = self.conn.cursor()
        c.execute('BEGIN IMMEDIATE TRANSACTION'))
        c.execute('SELECT rowid,request,state FROM ? LIMIT 1', (self.table)))
        rowid,body,state = c.fetchone()
        if body:
            c.execute('UPDATE ? SET state = ? WHERE rowid = ?', (self.table, self.DOWNLOADING, rowid))
            self.conn.commit()
            return self._decode_request(body)
        self.conn.commit()

__all__ = ['SpiderQueue']
