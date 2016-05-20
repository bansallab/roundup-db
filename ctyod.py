from db_util import create_session
from db_class import CtyOD

from pathlib import Path
from urllib.request import Request, urlopen 
from zipfile import ZipFile
from io import BytesIO

def main():

    session = create_session(database='CtyOD')
    engine = session.get_bind()
    CtyOD.metadata.drop_all(engine)
    CtyOD.metadata.create_all(engine)
    field_name = [c.name for c in CtyOD.__table__.columns]

    load_string = (
        "LOAD DATA LOCAL INFILE 'ck/{!s}'"
        " INTO TABLE ctyodp4"
        " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\r\\n'"
        " IGNORE 1 LINES;"
        )

    base_url = "http://cta.ornl.gov/transnet/"
    for idx in range(1, 7):
        
        csv_file = Path('CtyODp4-{}.csv'.format(idx)) 
        archive = Path('ck') / csv_file

        if not archive.exists():
            url = base_url + str(csv_file.with_suffix(".zip"))
            request = Request(url)
            with urlopen(request) as io:
                archive = ZipFile(BytesIO(io.read()))
            archive.extractall()
            
        session.execute(load_string.format(csv_file))
        session.commit()

    session.close()

if __name__ == '__main__':
    main()
