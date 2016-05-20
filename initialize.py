from db_class import Base, StateCode
from db_util import create_session, state_abbr
from sqlalchemy.exc import DatabaseError
from sys import argv, exit

Session = create_session(port=argv[1], check=True)
session = Session()

really = input("Really delete all tables from this database? yes/(no) ")
if really != 'yes':
    print("Okay, good thing we checked.")
    exit()

Base.metadata.drop_all(bind=session.get_bind())
Base.metadata.create_all(bind=session.get_bind())

for full_text in ['name', 'address']:
    try:
        session.execute("ALTER TABLE address ADD FULLTEXT({});".format(full_text))
    except DatabaseError:
        # bug: The generated warning should not raise an error.
        pass

if len(argv)==3:
    website = argv[2]
    load = (
        "LOAD DATA LOCAL INFILE '{!s}'"
        " INTO TABLE roundup_website"
        " FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'"
        " IGNORE 1 LINES"
        " (@skip, @aphis, @ams_office, @ams_location, @skip, @skip, @skip, @last_check, script, example_report, @note, website, @skip, robots)"
#        " (@skip, premises_id, script, @last_check, website, example_report, note, robots)"
        " SET "
        "last_check = IF(@last_check='NULL',NULL,STR_TO_DATE(@last_check,'%Y-%m-%d')), "
        "note = CONCAT_WS(',',@aphis,@ams_location,@note);"
        )
    session.execute(load.format(website))
    session.commit()

## Build state_code table
for abbr, code in state_abbr.items():
    state_code = StateCode(id=code, adminCode1=abbr)
    session.add(state_code)
session.commit()
