## A SQLite Scheduler for Scrapy Framework.

Scrapy-sqlite is a tool that lets you feed and queue URLs from SQLite via Scrapy spiders, using the [Scrapy framework](http://doc.scrapy.org/en/latest/index.html).

Inpsired by and modled after [scrapy-rabbitmq](https://github.com/darkrho/scrapy-redis).
which was too inpsired by and modled after [scrapy-redis](https://github.com/darkrho/scrapy-redis).

## Installation

Using pip, type in your command-line prompt

```
pip install scrapy-sqlite
```
 
Or clone the repo and inside the scrapy-sqlite directory, type

```
python setup.py install
```

## Usage

### Step 1: In your scrapy settings, add the following config values:

```
# Enables scheduling storing requests queue in sqlite.

SCHEDULER = "scrapy_sqlite.scheduler.Scheduler"

# Don't cleanup sqlite queues, allows to pause/resume crawls.
SCHEDULER_PERSIST = True

# Schedule requests using a priority queue. (default)
SCHEDULER_QUEUE_CLASS = 'scrapy_sqlite.queue.SpiderQueue'

# SQLite Queue to use to store requests
SQLITE_QUEUE_TABLE = 'scrapy_queue'

# Provide path to SQLite db file
SQLITE_CONNECTION_PARAMETERS = 'db.sqlite3'

# Store scraped item in sqlite for post-processing.
ITEM_PIPELINES = {
    'scrapy_sqlite.pipelines.SQLitePipeline': 1
}

# Cache Storage class 
HTTPCACHE_STORAGE = 'scrapy_sqlite.httpcache.SQLiteCacheStorage'

```

### Step 2: Add SQLiteMixin to Spider.

#### Example: multidomain_spider.py

```
from scrapy.contrib.spiders import CrawlSpider
from scrapy_sqlite.spiders import SQLiteMixin

class MultiDomainSpider(SQLiteMixin, CrawlSpider):
    name = 'multidomain'

    def parse(self, response):
        # parse all the things
        pass

```

### Step 3: Run spider using [scrapy client](http://doc.scrapy.org/en/1.0/topics/shell.html)

```
scrapy runspider multidomain_spider.py
```

### Step 4: Push URLs to SQLite

#### Example: push_web_page_to_queue.py

```
#TODO: reprogramm to sqlite

#!/usr/bin/env python
import pika
import settings

connection = pika.BlockingConnection(pika.ConnectionParameters(
               'localhost'))
channel = connection.channel()

channel.basic_publish(exchange='',
                      routing_key=settings.SQLITE_QUEUE_NAME,
                      body='</html>raw html contents<a href="http://twitter.com/roycehaynes">extract url</a></html>')

connection.close()

```

## Contributing and Forking

See [Contributing Guidlines](CONTRIBUTING.MD)

## Releases

See the [changelog](CHANGELOG.md) for release details.

| Version | Release Date |
| :-----: | :----------: |
| 0.1.0 | 2017-05-01 |



## Copyright & License

Copyright (c) 2017 Filip Hanes - Released under The MIT License.
