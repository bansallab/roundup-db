"""Convert CSV format market reports to records in the roundup_report, movement and address tables."""
import csv
import argparse
import json
import re
import sys
import traceback
import dateutil.parser
from datetime import date, timedelta
from time import sleep
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from pathlib import Path
from sqlalchemy import and_, not_, or_, text, func
from sqlalchemy.sql.expression import null
from sqlalchemy.orm import aliased
from db_class import (
    CtyODp4, Address, Geoname, Premises,
    Roundup, Movement, RoundupReport, Association, RoundupMarket,
    Market, RoundupWebsite
    )
from db_util import create_session, state_abbr, state_full, geoname_query_field

CHARSET = 'utf-8'

# Header strings, must match Premises, Geoname objects
address_prefix = ['sale_', 'consignor_', 'buyer_']
address_field = ['name', 'address', 'po', 'city', 'state', 'zip', 'zip_ext']
nameless_address_field = address_field.copy()
nameless_address_field.remove('name')

# Thresholds for market de-duplication
FT_NAME_THRESHOLD = 5
FT_ADDRESS_THRESHOLD = 4
FT_NAME_ONLY_THRESHOLD = 10

# Lag for Market News Service report date match
MNS_LAG_MAX = 3

class BadReportLogic(Exception):
    """Custom error for screwy CSV files."""

def read_csv(row):
    """Extract list of potential holdings (nodes) in a row, supplied as a dictionary, from a csv file."""

    node = []
    for this_address_prefix in address_prefix:
        this_node = {
            k.replace(this_address_prefix, ''): v for k, v in row.items()
            if this_address_prefix in k
            }
        this_node = {
            k: v for k, v in this_node.items()
            if k in address_field and v
            }
        if this_node:
            node.append(this_node)
        else:
            node.append(None)

    return tuple(node)

class Insert(object):
    """An instance of Insert can handle Premises and Movement insertions,
    creating Geoname entries as necessary."""

    def __init__(self, session):
        self.session = session

    def report(self, row, this_csv_file):

        report = None
        file_name = this_csv_file.name
        result = self.session.query(RoundupReport).filter_by(reference=file_name).first()
        if result:
            print("Report {} already entered.".format(file_name))
        elif row:
            row = {k: v.strip() for k, v in row.items() if v}
            report = {
                'reference': file_name,
                'date': date(
                    int(row['sale_year']),
                    int(row['sale_month']),
                    int(row['sale_day'])
                    ),
                }
            report.update({
                k.replace('sale_',''): v for k, v in row.items() if k in ['sale_title', 'sale_head']
                })
            report = RoundupReport(**report)

        return report

    def address(self, address):
        """Add address (if new) and return inserted (or found)
        objects of class Roundup.
        """

        def get_address(address):

            where = {k: address.get(k) for k in address_field}
            query = self.session.query(RoundupMarket).filter_by(**where)
            result = query.first()
            if not result:
                query = self.session.query(Roundup).filter_by(**where)

            result = query.first()
            if result:
                address = result
            else:
                address = Roundup(**address)
                self.session.add(address)

            return address

        if not isinstance(address, Address):
            address = get_address(address)

        return address

    def movement(self, from_address, to_address, row, report):
        """Insert a new Movement."""

        cattle = {k.replace('cattle_',''): v for k, v in row.items() if v and 'cattle_' in k}

        if len(set(cattle.values())) < len(cattle):
            print(report.reference)
            print("Identical values in {}".format(cattle))
            if input("Proceed anyway? y/(n)")!='y':
                raise Exception("Aborted import to check duplicate values.")

        movement = Movement(
            report=report,
            from_address=from_address,
            to_address=to_address,
            **cattle
            )

        self.session.add(movement)

    def premises(self, from_address, to_address):

        def get_geoname(address):

            def create_geoname(address):

                def use_geoname_api(address):

                    def query_geoname_api(address, fuzzy, city=None):

                        def get_geoname_url(address, fuzzy, city):

                            if not city:
                                city = address.city
                            if state_abbr.get(address.state):
                                state = address.state
                            else:
                                state = state_full.get(address.state)

                            full_query = {
                                'adminCode1': state,
                                'style': 'full',
                                'username': 'roundup',
                                }

                            if address.zip:
                                postalcode = None
                                if len(address.zip)==5:
                                    postalcode = address.zip
                                elif address.zip_ext and len(address.zip)==3:
                                    postalcode = address.zip

                                if postalcode:
                                    base_url = 'http://api.geonames.org/postalCodeSearchJSON?'
                                    full_query.update({
                                        'postalcode': address.zip,
                                        'placename': city,
                                        })
                            else:
                                base_url = 'http://api.geonames.org/searchJSON?'
                                full_query.update({
                                    'name_equals': city,
                                    'fuzzy': "{:.1f}".format(0.1*(10 - fuzzy)),
                                    'featureClass': 'P',
                                    'continentCode': 'NA',
                                    })

                            query = {k: v for k, v in full_query.items() if v}
                            geoname_url = base_url + urlencode(query)

                            return geoname_url

                        geoname = []
                        geoname_url = get_geoname_url(address, fuzzy, city)
                        request = Request(geoname_url)
                        sleep(0.5)
                        with urlopen(request) as io:
                            response = json.loads(io.read().decode())
                        if response.get('status'):
                            print(response['status']['message'])
                            msg = "Geonames returned a status message on address {}. Continue? y/(n) "
                            rsp = input(msg.format(address.id))
                            if 'y'==rsp:
                                return geoname
                            else:
                                self.session.rollback()
                                self.session.close()
                                sys.exit()
                        if response.get('geonames'):
                            geoname = response['geonames']
                        elif response.get('postalCodes'):
                            geoname = response['postalCodes']
                            if len(geoname) > 1:
                                print('Multiple responses from postalCode search.')
                        geoname = [
                            this_geoname for this_geoname in geoname
                            if this_geoname.get('adminCode2') and (this_geoname['adminCode1'] in state_abbr.keys())
                            ]

                        return geoname

                    def fix_city(address):

                        ask = "Okay to replace {} with \'{}\' in geocode query? y/(n) "
                        new_city = None
                        old_city = address.city
                        ask_if_correct = None

                        correction = [
                            {
                                'regex': r'^wh?i?t?e?\.? *su?l?p?h?u?r?\.?',
                                'correct': 'White Sulphur Springs',
                                },
                            {
                                'regex': r'pompey\'?s +pill?a?r?',
                                'correct': 'Pompey\'s Pillar',
                                },
                            {
                                'regex': r'heber city',
                                'correct': 'Heber',
                                },
                            {
                                'regex': r'miles +ci?t?y?',
                                'correct': 'Miles City',
                                },
                            {
                                'regex': r'^l(\.| ) *falls$',
                                'correct':'Little Falls',
                                },
                            {
                                'regex': r'^l(\.| ) *prairie$',
                                'correct':'Long Prairie',
                                },
                            {
                                'regex': r'^ff$',
                                'correct':'Fergus Falls',
                                },
                            {
                                'regex': r'(^|\b)(?P<sub>ft(\.| ) *).+',
                                'sub':'Fort ',
                                },
                            {
                                'regex': r'(^|\b)(?P<sub>pt\.?)($|\b)',
                                'sub':'Point',
                                },
                            {
                                'regex': r'(^|\b)(?P<sub>st(\.| ) *).+',
                                'sub':'Saint ',
                                },
                            {
                                'regex': r'(^|\b)(?P<sub>mt(\.| ) *).+',
                                'sub':'Mount ',
                                },
                            {
                                'regex': r'(^|\b)(?P<sub>w(\.| ) *).+',
                                'sub':'West ',
                                },
                            {
                                'regex': r'(^|\b)(?P<sub>s(\.| ) *).+',
                                'sub':'South ',
                                },
                            ]

                        for this_correction in correction:
                            match = re.search(this_correction['regex'], old_city, re.IGNORECASE)
                            if match and this_correction.get('correct'):
                                ask_if_correct = this_correction['correct']
                                break
                            elif match and this_correction.get('sub'):
                                old_city = old_city.replace(match.group('sub'), this_correction['sub'])
                                ask_if_correct = old_city

                        if ask_if_correct:
                            replace = "\'{}\'".format(address.city)
                            if address.state:
                                replace += " in {}".format(address.state)
                            input_prompt = ask.format(replace, ask_if_correct)
                            confirm = bool('y' == input(input_prompt))
                            if confirm:
                                new_city = ask_if_correct

                        return new_city

                    fuzzy = 0
                    geoname = query_geoname_api(address, fuzzy)
                    if not geoname:
                        new_city = fix_city(address)
                        if new_city:
                            geoname = query_geoname_api(address, fuzzy, new_city)
                    while fuzzy < 10 and not geoname:
                        fuzzy += 1
                        geoname = query_geoname_api(address, fuzzy, new_city)

                    return geoname, fuzzy

                geoname = None
                if address.city:
                    geoname, fuzzy = use_geoname_api(address)

                if geoname:
                    fuzzy = "{:.1f}".format(0.1*(10 - fuzzy))
                    for idx, this_geoname in enumerate(geoname):
                        this_geoname = {k: v for k, v in this_geoname.items() if k in geoname_query_field}
                        geoname[idx] = Geoname(fuzzy=fuzzy, **this_geoname)
                else:
                    # this empty geoname will prevent futile searches on the same address data
                    geoname = [Geoname()]

                return geoname

            if isinstance(address, RoundupMarket):
                query = self.session.query(Premises).join(Association).filter_by(address_id=address.id)
                geoname = [query.one().geoname]
            else:
                # check for geonames already searched for given identical location information
                where = {k: getattr(address, k) for k in nameless_address_field}
                query = self.session.query(Address).filter_by(**where
                    ).join(Geoname, Address.id==Geoname.address_id
                    )
                result = query.all()
                if len(result)==1:
                    geoname = result[0].geoname_cache
                elif len(result)==0:
                    geoname = None
                else:
                    print('Unexpected.')
                    raise(Exception)

            if not geoname:
                geoname = create_geoname(address)
                address.geoname_cache = geoname

            return geoname

        def minimize_distance(from_geoname, to_geoname):

            from_geoname = [this_geoname for this_geoname in from_geoname if this_geoname.adminCode2]
            to_geoname = [this_geoname for this_geoname in to_geoname if this_geoname.adminCode2]

            geoname = None
            distance_min = float("inf")
            for this_to_geoname in to_geoname:
                # Destination FIPS
                destination = state_abbr[this_to_geoname.adminCode1] + this_to_geoname.adminCode2
                for this_from_geoname in from_geoname:
                    # Origin FIPS
                    origin = state_abbr[this_from_geoname.adminCode1] + this_from_geoname.adminCode2
                    where = {'org': origin, 'dest': destination}
                    distance = self.ctyod.query(CtyODp4.GCD).filter_by(**where).scalar()
                    if distance!=None and distance<distance_min:
                        distance_min = distance
                        geoname = {'from': this_from_geoname, 'to': this_to_geoname}

            return geoname

        def get_premises(geoname, address):

            if len(geoname)==1:
                geoname = geoname[0]
            else:
                geoname = next(
                    (item for item in geoname if item.geonameId==None),
                    Geoname()
                    )
                geoname.address = address

            premises = Premises(geoname=geoname)

            return premises

        from_geoname = get_geoname(from_address)
        to_geoname = get_geoname(to_address)

        geoname = minimize_distance(from_geoname, to_geoname)

        if geoname:
            from_premises = Premises(geoname=geoname['from'])
            to_premises = Premises(geoname=geoname['to'])
        else:
            from_premises = get_premises(from_geoname, from_address)
            to_premises = get_premises(to_geoname, to_address)

        if not isinstance(from_address, RoundupMarket):
            self.session.add(Association(premises=from_premises, address=from_address, to_address=to_address))
        if not isinstance(to_address, RoundupMarket):
            self.session.add(Association(premises=to_premises, address=to_address, from_address=from_address))

def main(args):

    def import_roundup_report(path, glob):

        # list of csv files to import
        csv_file = Path(path).glob(glob)

        Session = create_session(check=True, **session_args)
        for this_csv_file in csv_file:

            insert = Insert(Session())

            try:
                # Establish a new session to communicate with the mysql server.
                # Each csv_file can be rolled back individually.
                with this_csv_file.open('r') as io:

                    reader = csv.DictReader(io)
                    row = next(reader, {})
                    report = insert.report(row, this_csv_file)
                    if report:
                        io.seek(0)
                        io.readline()
                        for row in reader:
                            row = {k: v.strip() for k, v in row.items() if v}
                            sale, consignor, buyer = read_csv(row)
                            if consignor and sale:
                                from_address = insert.address(consignor)
                                to_address = insert.address(sale)
                                insert.movement(from_address, to_address, row, report)
                                if buyer:
                                    from_address = to_address
                                    to_address = insert.address(buyer)
                                    insert.movement(from_address, to_address, row, report)
                            elif consignor and buyer:
                                from_address = insert.address(consignor)
                                to_address = insert.address(buyer)
                                insert.movement(from_address, to_address, row, report)
                            elif sale and buyer:
                                from_address = insert.address(sale)
                                to_address = insert.address(buyer)
                                insert.movement(from_address, to_address, row, report)
                            elif sale:
                                # no consignor or buyer information was extracted for a sale
                                continue
                            else:
                                # This should not happen, but would if only a consignor or buyer is listed in the CSV file.
                                msg = (
                                    "A csv file appears to include incorrectly extracted data.\n"
                                    "File: {}\nConsignor: {}\nSale: {}\nBuyer: {}".format(this_csv_file, consignor, sale, buyer)
                                    )
                                raise BadReportLogic(msg)
                    insert.session.commit()
            except BadReportLogic as err:
                print(err)
                insert.session.rollback()
            except:
                msg = "While importing report {}:".format(this_csv_file)
                print(msg)
                print(traceback.format_exc())
                insert.session.rollback()
            else:
                archived = this_csv_file.parent / archive_path / this_csv_file.name
                if not archived.exists():
                    this_csv_file.rename(archived)
            finally:
                insert.session.close()

    def deduplicate_market():

        def next_match(session, market, market_chain):

            def get_query(exclude, city=True):
                # Search against markets beyond the existing chain of matches
                # or anything else in exclude, added from non-null mis-matches.
                query = session.query(Market).filter(not_(Market.id.in_(exclude)))
                # State filter applies to all remaining matches.
                query = query.filter_by(state=market.state)
                if city:
                    # City filter often applies
                    query = query.filter_by(city=market.city)
                return query

            def also_exclude(exclude):
                query = session.query(Association
                    ).join(Market, Market.id==Association.address_id
                    )
                return query.filter(Market.id.in_(exclude))

            result = None
            exclude = set(this_market.id for this_market in market_chain)

            # During import, rows with multiple versions of an attribute were split.
            # These are certainly duplicates.
            row = getattr(market, 'row', None)
            ThisMarket = type(market)
            if row:
               result = session.query(ThisMarket
                   ).filter(not_(ThisMarket.id.in_(exclude))
                   ).filter_by(row=row).first()

            # Gather null features, as needed and if available, from match_chain
            c = {k: None for k in ['name', 'address', 'po']}
            for k in c:
                c[k] = [getattr(this_market, k, None) for this_market in market_chain]
                c[k] = [v for v in c[k] if v]

            # The following matches are all valid, but their order implies precedence.
            if not result and c['po'] and market.city:
                query = get_query(exclude)
                po_filter = [Market.po==v for v in c['po']]
                next_query = query.filter(or_(*po_filter))
                result = next_query.first()
                next_query = query.filter(Market.po!=None)
                exclude |= set(r.id for r in next_query)
                exclude |= set(r.address_id for r in also_exclude(exclude))

            if not result and c['address'] and market.city:
                query = get_query(exclude)
                for address in c['address']:
                    full_text = Market.address.match(address)
                    next_query = query.filter(full_text > FT_ADDRESS_THRESHOLD).order_by(full_text.desc())
                    result = next_query.first()
                    if result:
                        break
                next_query = query.filter(Market.address!=None)
                exclude |= set(r.id for r in next_query)
                exclude |= set(r.address_id for r in also_exclude(exclude))

            if not result and c['name'] and market.city:
                query = get_query(exclude)
                for name in c['name']:
                    full_text = Market.name.match(name)
                    next_query = query.filter(full_text > FT_NAME_THRESHOLD).order_by(full_text.desc())
                    result = next_query.first()
                    if result:
                        break
                next_query = query.filter(Market.name!=None)
                exclude |= set(r.id for r in next_query)
                exclude |= set(r.address_id for r in also_exclude(exclude))

            if not result and not any(c.values()) and market.city:
                query = get_query(exclude)
                result = query.first()

            if not result and c['name']:
                query = get_query(exclude, city=False)
                next_query = None
                if c['address'] and not c['po']:
                    next_query = query.filter(Market.address==None, Market.po!=None)
                elif c['po'] and not c['address']:
                    next_query = query.filter(Market.address!=None, Market.po==None)
                if next_query:
                    for name in c['name']:
                        full_text = Market.name.match(name)
                        next_query = next_query.filter(full_text > FT_NAME_ONLY_THRESHOLD).order_by(full_text.desc())
                        result = next_query.first()
                        if result:
                            break

            if result:
                query = session.query(Association
                    ).join(Market, Market.id==Association.address_id
                    ).filter_by(id=result.id
                    )
                association = query.first()

            if result and association:
                premises = association.premises
            else:
                premises = None

            return result, premises

        Session = create_session(check=False, **session_args)
        session = Session()

        query = session.query(Market
            ).outerjoin(Association, Market.id==Association.address_id
            ).filter_by(premises_id=None
            )

        market = query.first()
        while market:
            match_chain = []
            repeat = True
            while repeat:
                match_chain.append(market)
                match, premises = next_match(session, market, match_chain)
                if match and premises:
                    repeat = False
                elif not match:
                    repeat = False
                else:
                    market = match

            if not premises:
                premises = Premises()

            for this_match in match_chain:
                if premises.geoname and not premises.geoname.geonameId:
                    premises.geoname = None
                session.add(Association(premises=premises, address=this_match))

            session.commit()
            market = query.first()

        session.close()

    def geoname_roundup_market():

        def get_geoname(session, market):

            def location_search(location, most_fuzzy=0):

                def query_open_mapquest(location, geocodeQuality, geoname=[]):

                    api_key = 'DKSnfc0n2psp0v900BUPD1hNDGHJnhql'  # developer.mapquest.com itc2@georgetown.edu
                    base_url = 'http://open.mapquestapi.com/geocoding/v1/address?key=' + api_key + '&'
                    param = {
                        'street': location.get('address'),
                        'city': location.get('city'),
                        'state': location.get('state'),
                        'postalCode': location.get('zip'),
                        'country': 'US',
                        }

                    query = {k: v for k, v in param.items() if v}
                    url = base_url + urlencode(query)

                    request = Request(url)
                    with urlopen(request) as io:
                        response = json.loads(io.read().decode())
                    mapquest = [
                        res for res in response['results'][0]['locations']
                        if res['geocodeQuality'] in geocodeQuality
                        ]

                    match = None
                    for this_mapquest in mapquest:
                        match = next((
                            this_geoname for this_geoname in geoname
                            if this_mapquest['adminArea4'] in this_geoname['adminName2']
                            ), None)
                        if match:
                            break

                    return match, mapquest

                def query_geoname_reverse(lat_lng):

                    base_url = 'http://api.geonames.org/findNearbyPlaceNameJSON?'
                    param = {
                        'lat': lat_lng['lat'],
                        'lng': lat_lng['lng'],
                        'style': 'full',
                        'username': 'roundup',
                        }

                    query = {k: v for k, v in param.items() if v}
                    geoname_url = base_url + urlencode(query)

                    request = Request(geoname_url)
                    sleep(0.5)
                    with urlopen(request) as io:
                        response = json.loads(io.read().decode())
                    if 'geonames' in response:
                        geoname = response['geonames']
                    else:
                        print("No geonames in response from {}.".format(geoname_url))
                        geoname = []
                    geoname = [
                        this_geoname for this_geoname in geoname
                        if this_geoname.get('adminCode2') and (this_geoname['adminCode1'] in state_abbr.keys())
                        ]

                    return geoname

                def debbreviate(location):

                    search = [
                        {'pattern': r'(^|\b)St\.? ', 'sub':'Saint '},
                        {'pattern': r'(^|\b)Mt\.? ', 'sub':'Mount '},
                        {'pattern': r'(^|\b)Ft\.? ', 'sub':'Fort '},
                        {'pattern': r'(^|\b)N\.? ', 'sub':'North '},
                        {'pattern': r'(^|\b)S\.? ', 'sub':'South '},
                        {'pattern': r'(^|\b)Mc ', 'sub':'Mc'},
                        {'pattern': r'(^|\b)Sprgs', 'sub':'Springs'},
                        ]

                    city = location['city']
                    for this_search in search:
                        city = re.sub(this_search['pattern'], this_search['sub'], city, flags=re.IGNORECASE)

                    if city==location['city']:
                        change = False
                    else:
                        location['city'] = city
                        change = True

                    return change

                def query_geoname(location, fuzzy):

                    base_url = 'http://api.geonames.org/searchJSON?'
                    param = {
                        'name_equals': location['city'],
                        'adminCode1': location['state'],
                        'fuzzy': "{:.1f}".format(0.1*(10 - fuzzy)),
                        'style': 'full',
                        'username': 'roundup',
                        'featureClass': 'P',
                        'continentCode': 'NA',
                        }

                    query = {k: v for k, v in param.items() if v}
                    geoname_url = base_url + urlencode(query)

                    request = Request(geoname_url)
                    sleep(0.5)
                    with urlopen(request) as io:
                        response = json.loads(io.read().decode())
                    if 'geonames' in response:
                        geoname = response['geonames']
                    else:
                        geoname = []

                    geoname = [
                        this_geoname for this_geoname in geoname
                        if this_geoname.get('adminCode2') and (this_geoname['adminCode1'] in state_abbr.keys())
                        ]

                    return geoname

                geoname = []

                if not geoname and location.get('address'):
                    street = {k: v for k, v in location.items() if k in ['address', 'city', 'state']}
                    match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'])
                    if len(mapquest)==1:
                        fuzzy = None
                        lat_lng = mapquest[0]['latLng']
                        geoname = query_geoname_reverse(lat_lng)

                if not geoname and not location.get('city'):
                    if location.get('zip'):
                        zip = {k: v for k, v in location.items() if k in ['zip', 'state']}
                        match, mapquest = query_open_mapquest(zip, ['ZIP'])
                    elif location.get('address'):
                        street = {k: v for k, v in location.items() if k in ['address', 'city', 'state']}
                        match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'])
                    if len(mapquest)==1:
                        location['city'] = mapquest[0]['adminArea5']
                    else:
                        print('Now what ...')  #FIXME

                if not geoname:
                    fuzzy = -1
                    while fuzzy <= most_fuzzy:
                        fuzzy += 1  # C'mon, it's just Mollins -> Mullins!
                        geoname = query_geoname(location, fuzzy)
                        if geoname:
                            break

                if not geoname:
                    success = debbreviate(location)
                    if success:
                        fuzzy = 0
                        geoname = query_geoname(location, fuzzy)

                if not geoname and location.get('zip'):
                    fuzzy = 6
                    geoname = query_geoname(location, fuzzy)
                    zip = {k: v for k, v in location.items() if k in ['zip']}
                    match, mapquest = query_open_mapquest(zip, ['ZIP'], geoname)
                    if match:
                        geoname = [match]
                    elif len(mapquest)==1:
                        fuzzy = None
                        lat_lng = mapquest[0]['latLng']
                        geoname = query_geoname_reverse(lat_lng)

                if len(geoname) > 1:
                    match = False
                    if not match and location.get('zip'):
                        zip = {k: v for k, v in location.items() if k in ['zip']}
                        match, mapquest = query_open_mapquest(zip, ['ZIP'], geoname)
                    if not match and location.get('address'):
                        street = {k: v for k, v in location.items() if k in ['address', 'city', 'state']}
                        match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'], geoname)
                    if not match and location.get('address'):
                        street = {k: v for k, v in location.items() if k in ['address', 'state']}
                        match, mapquest = query_open_mapquest(street, ['ADDRESS', 'STREET'], geoname)
                    if match:
                        geoname = [match]

                return geoname, fuzzy

            location = {k: getattr(market, k) for k in ['address', 'city', 'state', 'zip']}

            geoname, fuzzy = location_search(location, most_fuzzy=2)

            for idx, this_geoname in enumerate(geoname):
                this_geoname = {k: v for k, v in this_geoname.items() if k in geoname_query_field and v}
                if fuzzy:
                    this_geoname['fuzzy'] = "{:.1f}".format(0.1*(10 - fuzzy))
                geoname[idx] = this_geoname

            return geoname

        Session = create_session(check=False, **session_args)
        session = Session()

        outer_query = (
            session.query(Premises)
            .filter_by(geoname=None)
            .join(Association)
            .filter_by(to_address=None, from_address=None)
            .group_by(Premises)
            )

        for premises in outer_query:

            query = (
                session.query(Market)
                .join(Association, Market.id == Association.address_id)
                .filter(Association.premises_id == premises.id)
                .filter(or_(
                    Market.address != null(),
                    Market.city != null(),
                    Market.zip != null(),
                    ))
                )

            # Prefer city, state associated with non-null street address
            inner_query = query.order_by(Market.po, Market.address.desc())

            # Geolocate market location
            geoname = []
            for market in inner_query:
                geoname = get_geoname(session, market)
                if len(geoname) == 1:
                    break

            if len(geoname) == 0:
                print("No geoname for premises {}.".format(premises.id))
                geoname = Geoname()
            elif len(geoname) > 1:
                print("Multiple geonames for premises {}.".format(premises.id))
                geoname = Geoname()
            else:
                geoname = Geoname(**geoname[0])

            geoname.address = market
            premises.geoname = geoname
            session.commit()

        session.close()

    def geoname_roundup():

        Session = create_session(check=True, **session_args)
        session = Session()
        insert = Insert(session)
        Session = create_session(check=False, database='CtyOD')
        insert.ctyod = Session()

        # Roundup addresses in the from_address position, to intersect with next query
        subquery = (
            session.query(Movement.id)
            .outerjoin(
                Association,
                and_(
                    Movement.from_address_id == Association.address_id,
                    Movement.to_address_id == Association.to_address_id,
                    )
                )
            .filter(Association.premises_id == null())
            .subquery()
            )

        # Roundup addresses in the to_address position
        query = (
            session.query(Movement.from_address_id, Movement.to_address_id)
            .outerjoin(
                Association,
                and_(
                    Movement.to_address_id == Association.address_id,
                    Movement.from_address_id == Association.from_address_id,
                    )
                )
            .filter(Association.premises_id == null())
            .join(subquery, Movement.id == subquery.c.movement_id)
            .group_by(Movement.from_address_id, Movement.to_address_id)
            )

        for movement in query:
            from_address = (
                session.query(Address)
                .filter_by(id=movement.from_address_id)
                .one()
                )
            to_address = (
                session.query(Address)
                .filter_by(id=movement.to_address_id)
                .one()
                )
            try:
                insert.premises(from_address, to_address)
                insert.session.commit()
            except:
                insert.session.rollback()

        insert.session.close()
        insert.ctyod.close()

    def add_market():

        Session = create_session(check=False, **session_args)
        session = Session()

        insert_stmt = text(
            "INSERT INTO roundup_market SET"
            " address_id=:address_id, roundup_website_id=:foreign_id;"
            )
        update_stmt = text(
            "UPDATE address SET source='roundup_market' "
            " WHERE address_id=:address_id;"
            )

        add_another = True
        while add_another:
            address_id = input('Enter address_id: ')
            session.execute(update_stmt.params(address_id=address_id))
            foreign_id = input('Enter roundup_website_id: ')
            session.execute(insert_stmt.params(address_id=address_id, foreign_id=foreign_id))
            add_another = bool(
                input('Add another market? yes/(no) ')=='yes'
                )

        session.commit()
        session.close()

        if input('Continue execution? yes/(no) ')!='yes':
            sys.exit()

    def get_mns_receipts():

        def get_report_date(report_date, line):

            match = None
            for this_line in line:
                match = re.search(r'report for (.+)$', this_line, re.IGNORECASE)
                if match:
                    break

            if not match:
                match = re.search(r'\s{2,}(.*?)\s{2,}', line[1])

            if match:
                date_string = match.group(1)
                try:
                    new_date = dateutil.parser.parse(date_string, fuzzy=True).date()
                except ValueError:
                    new_date = None
                if new_date!=date.today():
                    report_date = new_date

            return report_date

        def get_report_head(line):

            head = None
            for this_line in line:
                match = re.search(r'receipts: *([0-9,]+)', this_line, re.IGNORECASE)
                if match:
                    head = match.group(1).replace(',', '')
                    break

            return head

        file_name_pattern = "{mns}{y:d}{m:02d}{d:02d}"
        url_pattern = "http://search.ams.usda.gov/mndms/{y:d}/{m:02d}/{name}.TXT"
        Session = create_session(check=False, **session_args)
        session = Session()
        query = (
            session.query(RoundupReport, RoundupWebsite)
            .join(RoundupWebsite, RoundupReport.reference.startswith(RoundupWebsite.script))
            .filter(RoundupReport.mns_id==None)
            .filter(RoundupWebsite.mns_id!=None)
            )
        for report, website in query:
            sale_date = report.date
            mns_code = website.mns_id.split(',') if website.mns_id else []
            head = None
            for i in range(MNS_LAG_MAX + 1):
                report_date = sale_date + timedelta(days=i)
                for this_mns_code in mns_code:
                    file_name = file_name_pattern.format(y=report_date.year, m=report_date.month, d=report_date.day, mns=this_mns_code)
                    url = url_pattern.format(y=report_date.year, m=report_date.month, name=file_name)
                    try:
                        response = urlopen(url)
                    except HTTPError as e:
                        if e.code==404:
                            continue
                        else:
                            raise e
                    line = response.read().decode('utf-8', errors='ignore').splitlines()
                    report_date = get_report_date(report_date, line)
                    if report_date!=sale_date:
                        continue
                    head = get_report_head(line)
                    break
                if head:
                    report.mns_id = file_name
                    report.receipts = head
                    session.commit()
                    break
            if not head:
                report.mns_id = 'na'
                session.commit()

        session.close()

    # dry-run requires a initialized mysql database with the given name
    if args.dry_run:
        archive_path = Path('.')
        session_args = {'database': args.dry_run}
    else:
        archive_path = Path('dbased')
        session_args = {}

    # Insert into address and movement tables, without creating premises associations.
    if args.path:
        import_roundup_report(args.path, args.glob)
        if args.dry_run or input('Reports ingested, continue execution? yes/(no) ') != 'yes':
            sys.exit()

    # Manually indicate that a newly added address is a market.
    if args.add_market:
        add_market()

    # Check for Market News Service receipts.
    get_mns_receipts()

    # Match any markets not already associated to a premises to a new or existing premises.
    deduplicate_market()

    # Assign a geoname to any market not already associated to a premises with a non-null geoname_id.
    geoname_roundup_market()

    # Assign a premises and geoname to the roundup addresses.
    geoname_roundup()

if __name__ == "__main__":

    # command line options
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dry-run',
        dest='dry_run',
        help='Import into given database without moving files or geocoding.',
        )
    parser.add_argument(
        '--add-market',
        dest='add_market',
        help='Pause execution to allow insertion into roundup_market tables.',
        action='store_true',
        )
    parser.add_argument(
        '-p',
        '--path',
        dest='path',
        help='Path to market report directory root.',
        )
    parser.add_argument(
        '-g',
        '--glob',
        dest='glob',
        help='Pattern to glob for market reports in csv format.',
        )
    args = parser.parse_args()

    main(args)
