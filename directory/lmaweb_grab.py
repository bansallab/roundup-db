#!/usr/local/bin/python3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, aliased
from db_util import *
from mysql_conf import connection

from urllib import parse, request
import json

CHARSET = 'utf-8'

# Query for LMA Web's wordpress plugin called "store locator plus"
data = {
    'lat':40,
    'lng': -96,
    'radius': 10000,
    'options[initial_results_returned]': 10000,
    'action': 'csl_ajax_onload',
    }
data_byte = parse.urlencode(data).encode(CHARSET)
url = "http://www.lmaweb.com/wp-admin/admin-ajax.php"

# POST query to URL
result_byte = request.urlopen(url, data_byte).read()
result = json.loads(result_byte.decode(CHARSET))
website = set(market['url'] for market in result['response'] if market['url'].strip())

# Connect to cownet database
engine = create_engine(connection)
Session = sessionmaker(bind = engine)

# Start session and insert new URLs
session = Session()
new = 0
for this_website in website:
    result = session.query(Source).filter(Source.website == this_website).first()
    if not result:
        source = Source(website = this_website)
        session.add(source)
        try:
            session.commit()
            new += 1
        except:
            session.rollback()

# Clean up
session.close()
engine.dispose()

# Summary
print("Added {} new websites to the database.".format(new))

# now google site search for "report" or even a is_sale type string
