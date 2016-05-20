from os.path import expanduser
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL as create_url
from sqlalchemy.orm import sessionmaker
import sys

geoname_query_field = ['geonameId', 'adminCode1', 'adminCode2']

def create_session(database=None, port=None, check=True, echo=False):

    connect_args = {
        'option_files': expanduser('~') + '/.my.cnf',
        'option_groups': ['client', 'roundup-db'],
        }
    connect = create_url(drivername='mysql+mysqlconnector')
    if database:
        connect.database = database
    if port:
        connect.port = port

    engine = create_engine(connect, connect_args=connect_args, echo=echo)

    # Confirm database
    if check:
        status = {
            'user': engine.execute('SELECT USER()').first()[0],
            'database': engine.execute('SELECT DATABASE()').first()[0],
            'hostname': engine.execute('SELECT @@HOSTNAME').first()[0],
            }
        really = input('\nUSER: {user}\nDATABASE: {database}\nHOSTNAME: {hostname}\nIs that correct? yes/(no) '.format(**status))
        if really != 'yes':
            print('Okay, good thing we checked. Bye.')
            sys.exit()

    Session = sessionmaker(bind=engine)

    return Session

state_abbr = {
    'AK': '02',
    'AL': '01',
    'AR': '05',
    'AS': '60',
    'AZ': '04',
    'CA': '06',
    'CO': '08',
    'CT': '09',
    'DC': '11',
    'DE': '10',
    'FL': '12',
    'GA': '13',
    'GU': '66',
    'HI': '15',
    'IA': '19',
    'ID': '16',
    'IL': '17',
    'IN': '18',
    'KS': '20',
    'KY': '21',
    'LA': '22',
    'MA': '25',
    'MD': '24',
    'ME': '23',
    'MI': '26',
    'MN': '27',
    'MO': '29',
    'MS': '28',
    'MT': '30',
    'NC': '37',
    'ND': '38',
    'NE': '31',
    'NH': '33',
    'NJ': '34',
    'NM': '35',
    'NV': '32',
    'NY': '36',
    'OH': '39',
    'OK': '40',
    'OR': '41',
    'PA': '42',
    'PR': '72',
    'RI': '44',
    'SC': '45',
    'SD': '46',
    'TN': '47',
    'TX': '48',
    'UT': '49',
    'VA': '51',
    'VI': '78',
    'VT': '50',
    'WA': '53',
    'WI': '55',
    'WV': '54',
    'WY': '56',
    }

state_full = {
    'Alberta': 'AB',
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID', 
    'Illinois': 'IL',
    'Ill': 'IL', 
    'Indiana': 'IN', 
    'Iowa': 'IA', 
    'Kansas': 'KS', 
    'Kentucky': 'KY', 
    'Louisiana': 'LA', 
    'Maine': 'ME', 
    'Maryland': 'MD', 
    'Massachusetts': 'MA', 
    'Michigan': 'MI',
    'Mich': 'MI',
    'Minnesota': 'MN', 
    'Minn': 'MN', 
    'Mississippi': 'MS',
    'Miss': 'MS',
    'Missouri': 'MO', 
    'Montana': 'MT',
    'Mont': 'MT',
    'Neb': 'NE',
    'Nebraska': 'NE',
    'Nevada': 'NV', 
    'New Hampshire': 'NH', 
    'New Jersey': 'NJ', 
    'New Mexico': 'NM', 
    'New York': 'NY', 
    'North Carolina': 'NC', 
    'N.D': 'ND',
    'North Dakota': 'ND',
    'Ohio': 'OH', 
    'Oklahoma': 'OK',
    'Okla': 'OK',
    'Oregon': 'OR', 
    'Pennsylvania': 'PA', 
    'Rhode Island': 'RI', 
    'South Carolina': 'SC', 
    'S.D': 'SD', 
    'South Dakota': 'SD',
    'Saskatchewan': 'SK', 
    'Tennessee': 'TN', 
    'Texas': 'TX', 
    'Utah': 'UT', 
    'Vermont': 'VT', 
    'Virginia': 'VA', 
    'Washington': 'WA', 
    'West Virginia': 'WV', 
    'Wisonsin': 'WI', 
    'Wyoming': 'WY', 
    'Wyo': 'WY',
    }
