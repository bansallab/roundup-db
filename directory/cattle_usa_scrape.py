from sqlalchemy import Column, Integer, String, Float, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

from urllib import parse, request
from bs4 import BeautifulSoup
import json
import re

Base = declarative_base()
base_url = "http://www.cattleusa.com"

class CattleUSA(Base):
    """The cattle_usa table is an export of the markets holding auctions on CattleUSA."""
    __tablename__ = 'cattle_usa'
    
    id = Column(Integer, primary_key = True)
    name = Column(String(255))
    website = Column(String(255))
    city = Column(String(255))
    state = Column(String(255))
    phone = Column(String(255))
    google_maps_q = Column(String(255))

    
def scan_table_row(tr, session):

    td = tr.find_all('td')
    cattle_usa = CattleUSA()
    location = list(td[2].strings)
    cattle_usa.name = location[1].strip()
    match = re.search(r'\((?P<city>[^,]+),(?P<state>[^\)]+)', location[2])
    if match:
        cattle_usa.city = match.group('city').strip()
        cattle_usa.state = match.group('state').strip()
    if td[2].a:
        match = re.search(r'https?:/+((www.facebook.com)?[^/]+)', td[2].a['href'], re.IGNORECASE)
        if match:
            cattle_usa.website = match.group(1)
    cattle_usa.phone = td[4].string.strip()
    match = re.search(r'maps.google.com/maps\?q=(.+)', td[5].a['href'])
    if match:
        cattle_usa.google_maps_q = match.group(1)
        
    cattle_usa_current = session.query(CattleUSA).filter(CattleUSA.website == cattle_usa.website).first()
    if cattle_usa and not cattle_usa_current:
        session.add(cattle_usa)
        session.commit()

def scrape(engine, session):

    Base.metadata.create_all(engine)
    
    # Search through the full set of events schedule in 2014 for livestock auction locations
    url = "{}/index.php".format(base_url)
    soup = BeautifulSoup(request.urlopen(url).read())
    table = soup.find_all('table')[11]
    for tr in table.find_all('tr')[2:]:
        scan_table_row(tr, session)
