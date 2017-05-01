import sys, os
import scrapy_sqlite

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


from codecs import open

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


packages = [
    'scrapy_sqlite'
]

requires = [
    'Scrapy>=0.14'
]

setup(
    name='scrapy-sqlite',
    author='Filip Hanes',
    description='SQLite Plug-in for Scrapy',
    version='0.1.0',
    author_email='filip.hanes@gmail.com',
    license='MIT',
    url='https://github.com/filyph/scrapy-sqlite',
    install_requires=requires,
    packages=packages
)
