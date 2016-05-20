import csv
from os import system
from pathlib import Path
from sqlalchemy import func, distinct, and_
from db_class import Premises, Geoname, Market, Association, RoundupWebsite, RoundupMarket, Movement
from db_util import create_session, state_abbr

dict_writer_args = {
    'lineterminator': '\n',
    'quoting': csv.QUOTE_NONNUMERIC,
    }

def main():

    Session = create_session()
    session = Session()

    header = ['county_code', 'count', 'count_online']
    csv_file = Path('../data/release/compiled_market_count_by_county.csv')
    with csv_file.open('w') as io:
        writer = csv.DictWriter(io, header, **dict_writer_args)
        writer.writeheader()
        query = session.query(
                Market,
                func.count(distinct(Premises.id)).label('count'),
                func.sum(func.IF(Market.discriminator=='roundup_market', 1, 0)).label('count_online'),
                Geoname
            ).join(Association, Market.id==Association.address_id
            ).join(Premises, Association.premises_id==Premises.id
            ).join(Geoname, Premises.geoname_id==Geoname.id
            ).filter(Geoname.adminCode2!=None
            ).group_by(Geoname.adminCode1, Geoname.adminCode2
            )
        for result in query:
            row = {
                header[0]: state_abbr[result.Geoname.adminCode1] + result.Geoname.adminCode2,
                header[1]: result.count,
                header[2]: int(result.count_online),
                }
            writer.writerow(row)

    header = ['market_name', 'county_code', 'report_count', 'report_percent']
    csv_file = Path('../data/release/compiled_market_marketshed.csv')
    with csv_file.open('w') as io:
        writer = csv.DictWriter(io, header, **dict_writer_args)
        writer.writeheader()
        market_query = session.query(
                RoundupWebsite,
                RoundupMarket,
                func.count(distinct(Movement.roundup_report_id)).label('report_count')
            ).join(RoundupMarket, RoundupWebsite.id==RoundupMarket.foreign_id
            ).join(Movement, RoundupMarket.id==Movement.to_address_id
            ).group_by(RoundupMarket.id
            )
        for market_result in market_query:
            to_address_id = market_result.RoundupMarket.id
            market_name = market_result.RoundupWebsite.script
            query = session.query(
                    Movement,
                    Geoname,
                    func.count(distinct(Movement.roundup_report_id)).label('count'),
                ).join(Association, and_(
                    Association.address_id==Movement.from_address_id,
                    Association.to_address_id==Movement.to_address_id
                    )
                ).join(Premises, Association.premises_id==Premises.id
                ).join(Geoname, Premises.geoname_id==Geoname.id
                ).filter(Geoname.adminCode2!=None
                ).filter(Movement.to_address_id==to_address_id
                ).group_by(Geoname.adminCode1, Geoname.adminCode2
                )
            for result in query:
                row = {
                    header[0]: market_name,
                    header[1]: state_abbr[result.Geoname.adminCode1] + result.Geoname.adminCode2,
                    header[2]: result.count,
                    header[3]: float(result.count) / float(market_result.report_count),
                    }
                writer.writerow(row)

    header = ['premises', 'state', 'county']
    csv_file = Path('../data/release/compiled_market_county.csv')
    with csv_file.open('w') as io:
        writer = csv.DictWriter(io, header, **dict_writer_args)
        writer.writeheader()
        query = (
            session.query(Market, Association, Geoname)
            .join(Association, Market.id==Association.address_id)
            .join(Premises, Association.premises_id==Premises.id)
            .join(Geoname, Premises.geoname_id==Geoname.id)
            .filter(Geoname.adminCode2!=None)
            .group_by(Association.premises_id)
            .order_by(Association.premises_id)
            )
        for result in query:
            row = {
                header[0]: result.Association.premises_id,
                header[1]: state_abbr[result.Geoname.adminCode1],
                header[2]: result.Geoname.adminCode2,
                }
            writer.writerow(row)

    header = [
        'premises',
        'name',
        'address',
        'po',
        'city',
        'state',
        'zip',
        'zip_ext',
        'source',
        'id',
        ]
    csv_file = Path('../data/release/compiled_market.csv')
    with csv_file.open('w') as io:
        writer = csv.DictWriter(io, header, **dict_writer_args)
        writer.writeheader()
        query = (
            session.query(Market, Premises)
            .join(Association, Market.id==Association.address_id)
            .join(Premises, Association.premises_id==Premises.id)
            .filter(Market.discriminator!='roundup_market')
            .order_by(Premises.id)
            )
        for result in query:
            row = {
                header[0]: result.Premises.id,
                header[1]: result.Market.name,
                header[2]: result.Market.address,
                header[3]: result.Market.po,
                header[4]: result.Market.city,
                header[5]: result.Market.state,
                header[6]: result.Market.zip,
                header[7]: result.Market.zip_ext,
                header[8]: result.Market.discriminator,
                }
            if hasattr(result.Market, 'foreign_id'):
                row[header[9]] = str(result.Market.foreign_id)
            else:
                row[header[9]] = None
            writer.writerow(row)

if __name__ == '__main__':
    main()
