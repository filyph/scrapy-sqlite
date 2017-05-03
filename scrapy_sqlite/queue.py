from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.request import request_fingerprint

try:
    import cPickle as pickle
except ImportError:
    import pickle


class Base(object):
    """Per-spider queue/stack base class
    """

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
        self.conn.execute('DELETE FROM "%s"' % self.table)


class SpiderQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        c = self.conn.execute('SELECT COUNT(*) FROM "%s"' % self.table)
        count, = c.fetchone()

        return int(count)

    def push(self, request):
        """Push a request"""
        request_dump = self._encode_request(request)
        fingerprint = request_fingerprint(request)
        # INSERT OR IGNORE acts as dupefilter, because column fingerprint is UNIQUE
        c = self.conn.execute('''
            INSERT OR IGNORE INTO "%s"
                (fingerprint, request, state) VALUES (?, ?, ?)''' % self.table,\
                (fingerprint, request_dump, connection.SCHEDULED))

        if c.rowcount < 1:
            c = self.conn.execute('UPDATE "%s" SET state = ?' % self.table,\
                    (connection.SCHEDULED,))
        self.conn.commit()

    def pop(self):
        """Pop a request"""

        c = self.conn.cursor()
        c.execute('BEGIN IMMEDIATE TRANSACTION')
        c.execute('SELECT rowid,request,state FROM "%s" LIMIT 1'%self.table)
        result = c.fetchone()
        if result:
            rowid, body, state = c.fetchone()
            if body:
                c.execute('UPDATE "%s" SET state = ? WHERE rowid = ?'%self.table, (connection.DOWNLOADING, rowid))
                self.conn.commit()
                return self._decode_request(body)
        self.conn.commit()

__all__ = ['SpiderQueue']
