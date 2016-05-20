from pathlib import Path
from os import system
from sqlalchemy import inspect
from db_class import AgCensus, County
from db_util import create_session
from sys import argv

def get_ERS_Region(session):

    import_file = Path('../data/external_county/farm_resource_regions.csv')
    mysql_load = (
        "LOAD DATA LOCAL INFILE '{!s}'"
        " INTO TABLE ers_region"
        " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
        " (@fips, region)"
        " set state=LEFT(LPAD(@fips, 5, '0'), 2), county=RIGHT(LPAD(@fips, 5, '0'), 3);"
        )
    mapper = inspect(County)
    table = mapper.tables[0]

    session.execute("CREATE TEMPORARY TABLE ers_region(state VARCHAR(2), county VARCHAR(3), region INT);")
    session.execute(mysql_load.format(import_file))
    mysql_update = (
        "UPDATE {0}"
        " JOIN ers_region USING(state, county)"
        " SET {0}.ers_region = region;"
        )
    session.execute(mysql_update.format(table.name))
    session.commit()


def load_file(table, mysql_load, import_file, port):

    if import_file.exists():
        Session = create_session(port=port)
        session = Session()
        engine = session.get_bind()
        table.drop(engine, checkfirst=True)
        table.create(engine)
        try:
            session.execute(mysql_load.format(import_file))
            session.commit()
        except Exception as err:
            print(err)
            session.rollback()
        session.close()
    else:
        print("File '{!s}' not found.".format(import_file))


if __name__=='__main__':

    port = argv[1]

    ## AgCensus

    mapper = inspect(AgCensus)
    table = mapper.tables[0]
    column = list(
        c.key for c in mapper.column_attrs
        if not (c.key in ['id'])
        )
    archive = Path('../data/external_county/qs_census2012.tar.gz')
    system('tar -xf {}'.format(str(archive)))
    import_file = Path(archive.stem).with_suffix('.tab')
    mysql_load = (
        "LOAD DATA LOCAL INFILE '{!s}'"
        " INTO TABLE ag_census"
        " FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'"
        " IGNORE 1 LINES"
        " (" + ",".join(column) +");"
        )
    load_file(table, mysql_load, import_file, port)
    system('rm {}'.format(str(import_file)))

    ## County

    mapper = inspect(County)
    table = mapper.tables[0]
    column = list(c.key for c in mapper.column_attrs)
    column.remove('include_flag')

    import_file = Path('../data/external_county/county_list.txt')
    mysql_load = (
        "LOAD DATA LOCAL INFILE '{!s}'"
        " INTO TABLE county"
        " FIELDS TERMINATED BY '   ' LINES TERMINATED BY '\\r\\n'"
        " IGNORE 12 LINES"
        " (" + ",".join(column) + ")"
        " SET include_flag = True;"
        )

    load_file(table, mysql_load, import_file, port)

    ## Set inclusion flag

    Session = create_session(port=port, check=False)
    session = Session()

    query = [
        # Historical counties no longer exist
        session.query(County).filter_by(history_flag='2'),
        # Incorrectly not labeled as historical
        session.query(County).filter_by(name='Ormsby'),
        session.query(County).filter_by(name='Nansemond'),
        # Exclude state and districts
        session.query(County).filter(County.county.in_(['000', '888', '999'])),
        # Exclude AK and HI
        session.query(County).filter(County.state.in_(['02', '15'])),
        # Independent cities are mostly not included in NASS census
        session.query(County).filter(County.county.op('regexp')('^[5-9]')),
        ]
    for this_query in query:
        this_query.update({County.include_flag: False}, synchronize_session=False)
        session.commit()

    # Restore some independent cities that are included in NASS census
    query = [
        session.query(County).filter_by(state='51').filter(County.county.in_(['550', '810', '800'])),
        session.query(County).filter_by(state='32').filter(County.county.in_(['510'])),
        session.query(County).filter_by(state='48').filter(County.county.in_(['501', '503', '505', '507'])),
        ]
    for this_query in query:
        this_query.update({County.include_flag: True}, synchronize_session=False)
        session.commit()

    session.close()
