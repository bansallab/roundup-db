import re
import xlrd
import csv
import datetime
import json

from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from sqlalchemy import inspect
from db_class import (
    Market, AMS, AMS_Quantity,
    AMS_Receipts, APHIS, GIPSA, LMA
    )
from db_util import create_session


def split_zip(row):

    zipcode = row['zip']
    if len(zipcode) < 6:
        pass
    elif len(zipcode) == 6:
        row['zip'] = zipcode[:3]
        row['zip_ext'] = zipcode[3:]
    elif len(zipcode) > 6:
        row['zip'] = zipcode[:5]
        row['zip_ext'] = zipcode[6:]


def split_address(row):

    address = row['address']
    match = re.search(r'\bP\.? *O\.? +((box|drawer)[^(0-9)]+[0-9]*)', address, re.IGNORECASE)
    if not match:
        match = re.search(r'^((box|drawer)[^(0-9)]+[0-9]+)$', address, re.IGNORECASE)
    if match:
        row['address'] = address.replace(match.group(0), '').strip('-:;, \t\n')
        row['po'] = re.sub(r' +', ' ', match.group(1).title().strip('-:;,. \t\n'))


def exclude_row(name):

    # exclude 'horse', 'sheep', etc.  unless also 'livestock', 'cattle', etc ...
    # 'horse creek' is for the special case of TWO 'horse creek auction co.'s
    exclude = re.search(r'(horses?|hogs?|mule|sheep|lamb|goat|poultry|pig|pork|small animal)(\b|$)', name, re.IGNORECASE)
    include = re.search(r'(livestock|cattle|horse creek)(\b|$)', name, re.IGNORECASE)
    non_cattle = bool(exclude and not include)

    exclude = re.search(r'(video|internet|electronic)(\b|$)', name, re.IGNORECASE)
    non_physical = bool(exclude)

    return non_cattle or non_physical


def get_AMS_Volume(session):

    archive = Path('../data/external_market/AMS')

    ls = ['lsquantity.csv', 'lsreceipts.csv', 'lsregion_lslocation_lut.csv']
    ls_load = [(
        "LOAD DATA LOCAL INFILE '{!s}'"
        " INTO TABLE ams_quantity"
        " FIELDS TERMINATED BY ','"
        " LINES TERMINATED BY '\\n'"
        " IGNORE 1 LINES"
        " (@date, type_of_sale, office, location, species, type, volume)"
        " SET date=STR_TO_DATE(INSERT(@date, 7, 0, IF(RIGHT(@date, 2) < 13, '20', '19')), '%m/%d/%Y');"
        ), (
        "LOAD DATA LOCAL INFILE '{!s}'"
        " INTO TABLE ams_receipts"
        " FIELDS TERMINATED BY ','"
        " LINES TERMINATED BY '\\n'"
        " IGNORE 1 LINES"
        " (species, type_of_sale, office, location, receipts, @date)"
        " SET date=STR_TO_DATE(INSERT(@date, 7, 0, IF(RIGHT(@date, 2) < 13, '20', '19')), '%m/%d/%Y');"
        ), (
        "LOAD DATA LOCAL INFILE '{!s}'"
        " INTO TABLE ams_location"
        " FIELDS TERMINATED BY ','"
        " OPTIONALLY ENCLOSED BY '\"'"
        " LINES TERMINATED BY '\\n'"
        " IGNORE 1 LINES"
        " (lsregion, lslocation_id, lslocation_name,L,ST,LS);"
        )]

    # import lsqantity and lsreceipts data
    for this_ls, this_ls_load in zip(ls, ls_load):
        csv_file = archive / Path(this_ls)
        if csv_file.exists():
            session.execute(this_ls_load.format(csv_file))
            session.commit()
        else:
            print("File '{!s}' not found.".format(csv_file))


def get_AMS(session):

    ## FIXME Do I need to join on location and state since LS is missing from xlsx?
    if not session.query(AMS_Quantity).first():
        get_AMS_Volume(session)

    archive = Path('../data/external_market/AMS')
    version = 'AMS - Cattle_Market_Summary_vr3 - cleaned.xlsx'
    sheet_name = 'Data'
    skip_to_row = 1
    attr_map = {
        'foreign_id': 0,
        'name': 1,
        'city': 2,
        'state': 3,
        'match_addr': 12,
        }

    # import market summary, cattle auctions only
    file_to_import = archive / Path(version)
    if file_to_import.exists():
        wb = xlrd.open_workbook(str(file_to_import))
        sheet = wb.sheet_by_name(sheet_name)
        for rownum in range(skip_to_row, sheet.nrows):
            row = sheet.row_values(rownum)
            row = {k: row[v] for k, v in attr_map.items()}
            query = session.query(AMS_Quantity
                ).filter(AMS_Quantity.location == row['foreign_id']
                ).filter(AMS_Quantity.type_of_sale == 'A'
                ).filter(AMS_Quantity.species.like('Ca%')) # Cattle or Calves
            if exclude_row(row['name']) or not query.first():
                continue
            row['row'] = rownum
            if row['name'] == row['city']:
                row['name'] = ''
            if row['name'] or row['city']:
                match_addr = row.pop('match_addr')
                row = {k: v for k, v in row.items() if v}
                session.add(AMS(**row))
                session.commit()
                match = re.search(r'(?P<city>^[^,]+),(?P<state>.+)', match_addr)
                if match:
                    split = (
                        row['city'] != match.group('city').strip()
                        or row['state'] != match.group('state').strip()
                        )
                    if split:
                        row['city'] = match.group('city').strip()
                        row['state'] = match.group('state').strip()
                        session.add(AMS(**row))
                        session.commit()
    else:
        print('AMS file not found.')


def get_APHIS(session):

    skip_to_row = 1
    archive = Path('../data/external_market/APHIS')
    request = Request('http://www.aphis.usda.gov/wps/portal/aphis/ourfocus/animalhealth/sa_livestock_markets/')
    version = datetime.date.today().strftime('%B_%Y')
    sheet_label = 'exportToExcel'
    attr_map = {
        'foreign_id': 0,
        'name': 1,
        'address': 2,
        'city': 3,
        'state': 4,
        'zip': 5,
        'bovine': 6,
    }

    csv_file = archive / Path(version + '.csv')
    if csv_file.exists():
        with csv_file.open('r') as io:
            reader = csv.reader(io)
            next(reader)
            for rownum, row in enumerate(reader, start=1):
                row = {k: row[v] for k, v in attr_map.items()}
                row['row'] = rownum
                if row.pop('bovine') == 'Yes' and not exclude_row(row['name']):
                    split_zip(row)
                    split_address(row)
                    match = re.search(r'\bd\.?b\.?a\.?\b(.*)', row['name'], re.IGNORECASE)
                    if match:
                        row['name'] = row['name'].replace(match.group(0), '').strip(';:, \t\n')
                        row = {k: v for k, v in row.items() if v}
                        session.add(APHIS(**row))
                        session.commit()
                        row['name'] = match.group(1).strip(';:, \t\n')
                    row = {k: v for k, v in row.items() if v}
                    session.add(APHIS(**row))
                    session.commit()
    else:
        with urlopen(request) as net_io:
            soup = BeautifulSoup(net_io.read())
        state_link = soup.find_all('a', href=re.compile(r'\.*.xls$'))
        with csv_file.open('w') as io:
            writer = csv.writer(io)
            header = True
            for state in state_link:
                request = Request(state['href'])
                with urlopen(request) as net_io:
                    wb = xlrd.open_workbook(file_contents=net_io.read())
                if sheet_label in wb.sheet_names():
                    sheet = wb.sheet_by_name(sheet_label)
                    if header:
                        writer.writerow(sheet.row_values(0))
                        header = False
                    for rownum in range(skip_to_row, sheet.nrows):
                        row = sheet.row_values(rownum)
                        writer.writerow(row)

        # call again to load into database
        get_APHIS(session)


def get_GIPSA(session):

    skip_to_row = 4
    archive = Path('../data/external_market/GIPSA')
    request = Request('http://www.gipsa.usda.gov/psp/markets.aspx')
    xls_file = Path('regulated/SOC_list.xls')
    attr_map = {
        'name': 0,
        'dba': 1,
        'address': 2,
        'address_2': 3,
        'city': 4,
        'state': 5,
        'zip': 6,
        }

    with urlopen(request) as io:
        soup = BeautifulSoup(io.read())
    soup_a = soup.find('a', href=str(xls_file))
    match = re.search(r'\(([^:]*)\)', soup_a.next_sibling)
    version = match.group(1).replace(' ', '_')
    csv_file = archive / Path(xls_file.stem + '_' + version).with_suffix('.csv')

    if csv_file.exists():
        with csv_file.open('r') as io:
            reader = csv.reader(io)
            next(reader)
            for rownum, row in enumerate(reader, start=1):
                row = {k: row[v] for k, v in attr_map.items()}
                if not exclude_row(row['name'] + ' ' + row['dba']):
                    row['row'] = rownum
                    row['address'] = ' '.join([row['address'], row.pop('address_2')]).strip()
                    split_zip(row)
                    split_address(row)
                    dba = row.pop('dba')
                    row = {k: v for k, v in row.items() if v}
                    session.add(GIPSA(**row))
                    if dba:
                        row['name'] = dba
                        session.add(GIPSA(**row))
                    session.commit()
    else:
        request = Request('http://www.gipsa.usda.gov/psp/' + soup_a['href'])
        with urlopen(request) as io:
            wb = xlrd.open_workbook(file_contents=io.read())
        sheet = wb.sheet_by_name('Sheet')
        with csv_file.open('w') as io:
            writer = csv.writer(io)
            writer.writerow(sheet.row_values(skip_to_row - 1))
            for rownum in range(skip_to_row, sheet.nrows):
                row = sheet.row_values(rownum)
                writer.writerow(row)

        # call again to load into database
        get_GIPSA(session)


def get_LMA(session):

    # LMA Web's wordpress plugin called "store locator plus"
    request = Request('http://www.lmaweb.com/wp-admin/admin-ajax.php')

    # Query for csl_ajax_onload function
    data = {
        'lat': 40,
        'lng': -96,
        'radius': 10000,
        'options[initial_results_returned]': 10000,
        'action': 'csl_ajax_onload',
        }
    request.data = urlencode(data).encode()

    # Check for local dump and load it or rebuild table from website
    archive = Path('../data/external_market/LMA')
    csv_glob = archive.glob('*.csv')
    if csv_glob:
        csv_file = max(csv_glob, key=lambda x: x.stat().st_ctime)
        with csv_file.open('r') as io:
            for row in csv.DictReader(io):
                if not exclude_row(row['name']):
                    session.add(LMA(**row))
        session.commit()
    else:
        # POST query to URL
        with urlopen(request) as io:
            result = json.loads(io.read().decode())

        # Use session to insert new LMA entries
        csv_data = []
        for row in result['response']:
            row['foreign_id'] = row.pop('id')
            if row.get('phone'):
                a = BeautifulSoup(row['phone']).a
                if a:
                    row['phone'] = a.get_text()
            if row.get('zip'):
                split_zip(row)
            if row.get('address2'):
                row['address'] += '\n' + row.pop('address2')
            split_address(row)
            row = {k: v for k, v in row.items() if v}
            csv_data.append(row.__dict__)
            if row and not exclude_row(row.name):
                session.add(LMA(**row))

        session.commit()

        # Save a csv file for all the downloaded data
        mapper = inspect(LMA)
        header = list(
            c.key for c in mapper.column_attrs
            if not (c.key in ['id', 'discriminator'])
            )
        csv_file = archive / Path(datetime.date.today().strftime('%Y-%m-%d.csv'))
        with csv_file.open('w', encoding='utf-8') as io:
            writer = csv.DictWriter(io, header, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(csv_data)


def main():

    Session = create_session(check=True, port=3306)
    session = Session()

    table = [
        'ams',
#        'aphis',
#        'gipsa',
#        'lma',
        ]

    for this_table in table:
        globals()['get_' + this_table.upper()](session)

    # bug? the @ symbol breaks MATCH...AGAINST(...IN BOOLEAN MODE)
    query = session.query(Market).filter(Market.address.like('%@%'))
    for result in query:
        result.address = result.address.replace('@', 'at')

    session.commit()

if __name__ == '__main__':

    main()
