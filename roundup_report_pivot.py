from db_class import RoundupReport
from db_util import create_session
from pandas import read_sql, DataFrame, Grouper, to_datetime
from sqlalchemy import not_
from sys import argv

Session = create_session(check=False)
session = Session()
query = session.query(RoundupReport).filter(not_(RoundupReport.reference.like('southern_livestock%')))
df = read_sql(query.statement, session.bind)
session.close()

df['datetime'] = to_datetime(df['date'])
df['script'] = df['reference'].str.rsplit('_', expand=True, n=1).iloc[:, 0]
pivot = DataFrame(columns=df['script'].unique(), dtype=object)
for k, g in df.groupby([Grouper(freq='1W', key='datetime'), 'script']):
    pivot.at[k[0].date(), k[1]] = [date.weekday() for date in g['date']]
pivot.sort_index(inplace=True)
pivot.to_csv(argv[0].replace('.py', '.csv'))
