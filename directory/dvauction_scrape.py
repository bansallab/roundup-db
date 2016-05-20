from sqlalchemy import Column, Integer, String, Float, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

from urllib import parse, request
from bs4 import BeautifulSoup
import json
import re
from time import sleep

Base = declarative_base()
base_url = "http://www.dvauction.com"
CHARSET = 'utf-8'

class DVAuction(Base):
    """The dvauction table is an export of calendered sale events at DVAuction."""
    __tablename__ = 'dvauction'
    
    id = Column(Integer, primary_key = True)
    name = Column(String(255))
    address = Column(String(255))
    website = Column(String(255))
    email = Column(String(255))
    phone = Column(String(255))
    sale_count = Column(Integer)


def scan_table(table, session):
    for tr in table.find_all('tr'):
            td = tr.find('td', attrs = {'class': 'col2'})
            event = base_url + td.a['href']
            dvauction = scan_event(event)
            dvauction_current = session.query(DVAuction).filter(DVAuction.website == dvauction.website).first()
            if dvauction and not dvauction_current:
                dvauction.sale_count = 1
                session.add(dvauction)
                session.commit()
            elif dvauction:
                dvauction_current.sale_count += 1
                session.commit()                

def scan_event(event):
    dvauction = DVAuction()
    sleep(2)
    soup = BeautifulSoup(request.urlopen(event).read())
    div = soup.find('div', attrs = {'id': 'main'})
    dl = div.find_all('dl')
    dvauction.website = scan_dl(dl[0], 'Website')
    dvauction.email = scan_dl(dl[0], 'Email')
    dvauction.address = scan_dl(dl[0], 'Address')
    dvauction.phone = scan_dl(dl[0], 'Phone')
    if len(dl) > 1:
        dvauction.name = scan_dl(dl[1], 'Address')

    return dvauction

def scan_dl(dl, dt_text):
    dt = dl.find_all('dt')
    try:
        idx = next(idx for idx in range(len(dt)) if dt[idx].string == dt_text)
    except StopIteration:
        return None
    dd = dt[idx].find_next_sibling('dd')
    if dt_text == 'Website' and dd:
            href = dd.a['href']
            match = re.search(r'https?:/+((www.facebook.com)?[^/]+)', href, re.IGNORECASE)
            string = [match.group(1)]
    else:
        string = dd.strings    
    return '\n'.join(this_string.strip() for this_string in string)

def scrape(engine, session):
    
    # Search through the full set of events schedule in 2014 for livestock auction locations
    month_range = iter(range(1,13))
    for month in month_range:
        url = "{}/schedule/2014/{}".format(base_url, month)
        soup = BeautifulSoup(request.urlopen(url).read())
        table = soup.find('div', attrs = {'id': 'main'}).table
        scan_table(table, session)
