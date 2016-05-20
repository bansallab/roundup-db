from db_class import LMA, cownet
from db_util import create_session

from urllib.request import Request, urlopen
from urllib.parse import urlencode
import json

request = Request('http://www.lmaweb.com/wp-admin/admin-ajax.php')

def scrape(engine, session):

    cownet.metadata.create_all(engine)
    
    # Query for LMA Web's wordpress plugin called "store locator plus"
    data = {
        'lat':40,
        'lng': -96,
        'radius': 10000,
        'options[initial_results_returned]': 10000,
        'action': 'csl_ajax_onload',
        }
    request.data = urlencode(data).encode()
#    data_byte = parse.urlencode(data).encode()

    # POST query to URL
#    result_byte = request.urlopen(base_url, data_byte).read()
    with urlopen(request) as io:
        result = json.loads(io.read().decode())

    # Use session to insert new URLs
    try:
        for this_response in result['response']:
            lmaweb = LMAWeb(**this_response)
            lmaweb_current = session.query(LMAWeb).filter(LMAWeb.id == lmaweb.id).first()
            if lmaweb and not lmaweb_current: 
                session.add(lmaweb)
        session.commit()
    except:
        session.rollback()
