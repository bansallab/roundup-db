import csv
from db_class import Loadboard, Market, cownet
from sqlalchemy import func
from db_util import create_session
from os.path import expanduser
from pathlib import Path
from datetime import date, datetime
import numpy as np
import matplotlib.pyplot as plt

##Location of data files
data_dir = Path(expanduser('~')) / Path('Dropbox/Livestock_Network_Load_Board_Data')

##List of header keys, used for creating loadbard objects
header = ('row', 'posted', 'shipping', 'origin', 'destination', 'time', 'loads', 'rate', 'type', 'miles')

##List of widths for fixed width txt files
width = [0, 11, 11, 26, 26, 26, 16, 16, 16, 16, 16]
stop_pos = np.cumsum(width)

##Fixed-width files before this data, tab seperated after
change_date = date(2014, 11, 22)

def create_table(session):

    engine = session.get_bind()
    table = cownet.metadata.tables['loadboard']
    table.drop(engine, checkfirst=True)
    table.create(engine)


def get_column(line, i):

    column = line[stop_pos[i]: stop_pos[i + 1]]

    return column.replace('&nbsp;', '').strip()


def import_loadboard():

    # Create a session for interacting with the mysql database
    session = create_session('cownet', port=3306, check=False)
    create_table(session)

    prefix = 'livestock_network_'
    for file in data_dir.glob(prefix + '*.txt'):
        create_date = datetime.strptime(file.stem.replace(prefix, ''), '%Y-%m-%d').date()
        if create_date <= change_date:
            print("import '{}'".format(file.name))
            with file.open('r') as io:
                for line in io:
                    if len(line) != stop_pos[-1]:
                        continue
                    row = {
                        k: get_column(line, i)  for i, k in enumerate(header)
                        if k not in ('row', 'posted')
                        }
                    row = {k: v for k, v in row.items() if v}
                    query = session.query(Loadboard).filter_by(**row)

                    ##Checking for no repeats (matches)
                    if not query.first():
                        loadboard = Loadboard(**row)
                        session.add(loadboard)
            session.commit()
        else:
            print('No methods for tab separated input.')

    session.close()


def plot_degree_distribution():

    session = create_session('cownet', port=3306, check=False)
    query = session.query(Loadboard).group_by(Loadboard.origin)
    origin = [loadboard.origin for loadboard in query]
    degree = []
    for this_origin in origin:
        query = session.query(Loadboard).filter_by(**{'origin': this_origin}).group_by(Loadboard.destination)
        degree.append(query.count())
    session.close()

    counts, bins, patches = plt.hist(degree, tuple(range(max(degree) + 1)))
    plt.xlim([1, 40])
    plt.xlabel('degree')
    plt.ylabel('frequency')

    # Label the raw counts below the x-axis...
    bin_centers = 0.5 * np.diff(bins) + bins[:-1]
    for count, x in zip(counts, bin_centers):
        if count > 0:
            # Label the raw counts
            plt.annotate(
                "{:.0f}".format(count), xy=(x, 1), xycoords=('data', 'axes fraction'),
                xytext=(0, 25), textcoords='offset points', va='top', ha='center', rotation=90,
                )

    plt.savefig('loadboard_degree_distribution.pdf')


def main(arg):

    if arg['load_data']:
        import_loadboard()

    if arg['plot_degree_distribution']:
        plot_degree_distribution()

    # Does out_degree predict market presence?
    # Logistic Regression

    session = create_session('cownet', port=3306, check=False)
    query = session.query(Loadboard).group_by(Loadboard.origin)
    origin = [loadboard.origin for loadboard in query]
    degree = []
    has_market = []
    for this_origin in origin:
        query = session.query(Loadboard).filter_by(**{'origin': this_origin}).group_by(Loadboard.destination)
        degree.append(query.count())

        comma_split = this_origin.split(',')
        if len(comma_split)==2:
            query = session.query(Market
                ).filter(func.lower(Market.city) == func.lower(comma_split[0].strip())
                ).filter(func.lower(Market.state) == func.lower(comma_split[1].strip())
                )
            has_market.append(bool(query.first()))
        else:
            has_market.append(None)

    session.close()

    out_file = Path('logistic_regression_data.csv')
    with out_file.open('w') as io:
        writer = csv.writer(io)
        for x, y in zip(degree, has_market):
            writer.writerow([x, y])


if __name__ == '__main__':

    arg = {
        'load_data': False,
        'plot_degree_distribution': False
        }

    main(arg)