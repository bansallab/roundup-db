from sqlalchemy import ForeignKey, Boolean, Enum
from sqlalchemy import Column, Integer, String, Float, Numeric, Date, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class RoundupWebsite(Base):
    """This table is edited manually and used to track websites possibly reporting data online."""

    id = Column('roundup_website_id', Integer, primary_key=True)
    premises_id = Column(None, ForeignKey('premises.premises_id'))
    mns_id = Column(Text)
    script = Column(Text)
    last_check = Column(Date)
    website = Column(Text)
    example_report = Column(Text)
    note = Column(Text)
    robots = Column(Text)

    __tablename__ = id.name.replace('_id', '')


class Address(Base):
    """Address given for a location holding livestock."""

    id = Column('address_id', Integer, primary_key=True)
    name = Column(Text)
    address = Column(Text)
    po = Column(Text)
    city = Column(Text)
    state = Column(Text)
    zip = Column(String(5))
    zip_ext = Column(String(4))
    discriminator = Column('source', String(255))

    __tablename__ = id.name.replace('_id', '')
    __mapper_args__ = {'polymorphic_on': discriminator}


class Geoname(Base):
    """Premises have been successfully located at some spatial scale,
    using the data from the address with address_id,
    but may correspond to none, one or multiple animal holdings."""

    id = Column('geoname_id', Integer, primary_key=True)
    address_id = Column(None, ForeignKey('address.address_id', ondelete='cascade'))
    fuzzy = Column(Numeric(scale=1, precision=2))
    geonameId = Column(Integer)
    adminCode1 = Column(String(2))
    adminCode2 = Column(String(3))

    __tablename__ = id.name.replace('_id', '')
    address = relationship('Address', backref='geoname_cache')


class Premises(Base):
    """The combination of a premises and address in this association table
    represents a unique animal holding for our purposes."""

    id = Column('premises_id', Integer, primary_key=True)
    geoname_id = Column(None, ForeignKey('geoname.geoname_id', ondelete='cascade'))

    __tablename__ = id.name.replace('_id', '')
    geoname = relationship('Geoname')


class Association(Base):
    """Provides Many-to-Many relationship between address and premises."""

    premises_id = Column(None, ForeignKey('premises.premises_id', ondelete='cascade'), primary_key=True)
    address_id = Column(None, ForeignKey('address.address_id', ondelete='cascade'), primary_key=True)
    to_address_id = Column(None, ForeignKey('address.address_id', ondelete='cascade'))
    from_address_id = Column(None, ForeignKey('address.address_id', ondelete='cascade'))

    __tablename__ = 'association'
    premises = relationship('Premises', foreign_keys=premises_id)
    address = relationship('Address', foreign_keys=address_id)
    to_address = relationship('Address', foreign_keys=to_address_id)
    from_address = relationship('Address', foreign_keys=from_address_id)


class Roundup(Address):

    __mapper_args__ = {'polymorphic_identity': 'roundup'}


class Market(Address):
    # exists only to allow queries that aggregate across markets inheriting this class
    # however, still have to manually update address.roundup_market
    pass


class RoundupMarket(Market):

    id = Column('address_id', None, ForeignKey('address.address_id', ondelete='cascade'), primary_key=True)
    foreign_id = Column('roundup_website_id', None, ForeignKey('roundup_website.roundup_website_id'))

    __tablename__ = 'roundup_market'
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class AMS(Market):
    """Markets that appear in LPGMN reports and compiled by APHIS."""

    id = Column('address_id', None, ForeignKey('address.address_id', ondelete='cascade'), primary_key=True)
    foreign_id = Column(Integer)
    row = Column(Integer)

    __tablename__ = 'ams'
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class APHIS(Market):
    """Approved Livestock Market"""

    id = Column('address_id', None, ForeignKey('address.address_id', ondelete='cascade'), primary_key=True)
    foreign_id = Column(String(5))
    row = Column(Integer)

    __tablename__ = 'aphis'
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class GIPSA(Market):
    """Registered and Bonded Market Agencies Selling on Commission"""

    id = Column('address_id', None, ForeignKey('address.address_id', ondelete='cascade'), primary_key=True)
    row = Column(Integer)

    __tablename__ = 'gipsa'
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class LMA(Market):
    """Download of the Auction/Dealer Store locator plus database."""

    id = Column('address_id', None, ForeignKey('address.address_id', ondelete='cascade'), primary_key=True)
    foreign_id = Column(Integer)
    url = Column(Text)
    attributes = Column(Text)
    rank = Column(Text)
    country = Column(Text)
    description = Column(Text)
    email = Column(Text)
    fax = Column(Text)
    lat = Column(Text)
    lng = Column(Text)
    phone = Column(Text)
    featured = Column(Text)
    hours = Column(Text)
    tags = Column(Text)
    option_value = Column(Text)
    sl_pages_url = Column(Text)
    image = Column(Text)
    distance = Column(Float)

    __tablename__ = 'lma'
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class Movement(Base):

    id = Column('movement_id', Integer, primary_key=True)
    roundup_report_id = Column(None, ForeignKey('roundup_report.roundup_report_id', ondelete='cascade'), nullable=False)
    from_address_id = Column(None, ForeignKey('address.address_id'), nullable=False)
    to_address_id = Column(None, ForeignKey('address.address_id'), nullable=False)
    cattle = Column(Text)
    head = Column(Text)
    avg_weight = Column(Integer)
    price = Column(Numeric(precision=10, scale=2))
    price_cwt = Column(Numeric(precision=10, scale=2))

    __tablename__ = id.name.replace('_id', '')
    from_address = relationship('Address', foreign_keys=from_address_id)
    to_address = relationship('Address', foreign_keys=to_address_id)
    report = relationship('RoundupReport', foreign_keys=roundup_report_id)


class RoundupReport(Base):

    id = Column('roundup_report_id', Integer, primary_key=True)
    reference = Column(Text, nullable=False)
    date = Column(Date, nullable=False)
    title = Column(Text)
    head = Column(Integer)
    receipts = Column(Integer)
    mns_id = Column(String(20))

    __tablename__ = id.name.replace('_id', '')


class AMS_Quantity(Base):
    """Tables compiled by APHIS."""

    id = Column('ams_quantity_id', Integer, primary_key=True)
    date = Column(Date, nullable=False)
    type_of_sale = Column(String(1))
    office = Column(String(2))
    location = Column(Integer)
    species = Column(Text)
    type = Column(Text)
    volume = Column(Integer)

    __tablename__ = id.name.replace('_id', '')


class AMS_Receipts(Base):
    """Tables compiled by APHIS."""

    id = Column('ams_receipts_id', Integer, primary_key=True)
    species = Column(String(1))
    type_of_sale = Column(String(1))
    office = Column(String(2))
    location = Column(Integer)
    receipts = Column(Integer)
    date = Column(Date, nullable=False)

    __tablename__ = id.name.replace('_id', '')


class AMS_Location(Base):
    """Tables compiled by APHIS."""

    lsregion = Column(Text)
    lslocation_id = Column(Integer, primary_key=True)
    lslocation_name = Column(Text)
    L = Column(String(1), primary_key=True)
    ST = Column(String(2))
    LS = Column(String(2), primary_key=True)

    __tablename__ = 'ams_location'


class Loadboard(Base):

    id = Column('loadboard_id', Integer, primary_key=True)
    shipping = Column(Text)
    origin = Column(Text)
    destination = Column(Text)
    time = Column(Text)
    loads = Column(Integer)
    rate = Column(Text)
    type = Column(Text)
    miles = Column(Text)

    __tablename__ = id.name.replace('_id', '')


class CtyODp4(Base):

    org = Column(String(5), primary_key=True)
    dest = Column(String(5), primary_key=True)
    O_name = Column(Text)
    D_name = Column(Text)
    GCD = Column(Float)
    H_imp = Column(Float)
    H_mi_us = Column(Float)
    H_mi_oU = Column(Float)
    R_imp = Column(Float)
    R_mi_us = Column(Float)
    R_mi_oU = Column(Float)
    W_imp = Column(Float)
    W_mi = Column(Float)
    HRH_imp = Column(Float)
    HRH_Hmi = Column(Float)
    HRH_Rmi = Column(Float)
    All_imp = Column(Float)
    All_Lnd = Column(Float)
    All_Wmi = Column(Float)

    __tablename__ = 'ctyodp4'
    __table_args__ = {'schema': 'CtyOD'}


class AgCensus(Base):
    """see http://quickstats.nass.usda.gov/api#param_define"""

    id = Column('ag_census_id', Integer, primary_key=True)
    source_desc = Column(String(60))
    sector_desc = Column(String(60))
    group_desc = Column(String(80))
    commodity_desc = Column(String(80))
    class_desc = Column(String(180))
    prodn_practice_desc = Column(String(180))
    util_practice_desc = Column(String(180))
    statisticcat_desc = Column(String(80))
    unit_desc = Column(String(60))
    short_desc = Column(String(512))
    domain_desc = Column(String(256))
    domaincat_desc = Column(String(512))
    agg_level_desc = Column(String(40))
    state_ansi = Column(String(2))
    state_fips_code = Column(String(2))
    state_alpha = Column(String(2))
    state_name = Column(String(30))
    asd_code = Column(String(2))
    asd_desc = Column(String(60))
    county_ansi = Column(String(3))
    county_code = Column(String(3))
    county_name = Column(String(30))
    region_desc = Column(String(80))
    zip_5 = Column(String(5))
    watershed_code = Column(String(8))
    watershed_desc = Column(String(120))
    congr_district_code = Column(String(2))
    country_code = Column(String(4))
    country_name = Column(String(60))
    location_desc = Column(String(120))
    year = Column(Integer)
    freq_desc = Column(String(30))
    begin_code = Column(String(2))
    end_code = Column(String(2))
    reference_period_desc = Column(String(40))
    week_ending = Column(String(10))
    load_time = Column(String(19))
    value = Column(String(24))
    cv_pct = Column(String(7))

    __tablename__ = id.name.replace('_id', '')


class County(Base):
    """NASS data sourced from http://www.nass.usda.gov/Data_and_Statistics/index.asp.
    For ERS regions see data/external_county/farm_production_regions.xls."""

    state = Column(String(2), primary_key=True)
    district = Column(String(2), primary_key=True)
    county = Column(String(3), primary_key=True)
    name = Column(String(64))
    history_flag = Column(String(1))
    include_flag = Column(Boolean)
    ers_region = Column(Enum(
        'Heartland',
        'Northern Crescent',
        'Northern Great Plains',
        'Prairie Gateway',
        'Eastern Uplands',
        'Southern Seaboard',
        'Fruitful Rim',
        'Basin and Range',
        'Mississippi Portal',
        ))

    __tablename__ = 'county'


class Georef(Base):
    """Manually (as of now) georeferenced premises."""

    id = Column('georef_id', Integer, primary_key=True)
    premises_id = Column(None, ForeignKey('premises.premises_id'))
    source = Column(Enum('google_maps_satellite', 'google_maps'))
    lat = Column(Numeric(8, 6))
    lng = Column(Numeric(9, 6))

    __tablename__ = id.name.replace('_id', '')


class StateCode(Base):
    """Conversion between Geoname.adminCode1 and State FIPS code."""

    id = Column('state_code_id', String(2), primary_key=True)
    adminCode1 = Column(String(2))

    __tablename__ = id.name.replace('_id', '')
