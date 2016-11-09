from sqlalchemy import Column, create_engine
from sqlalchemy import Integer, String, Float, DateTime, Interval, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta


def _int(value, default=int()):
    """
    Attempt to cast *value* into an integer, returning *default* if it fails.
    """
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return default


def _float(value, default=float()):
    """
    Attempt to cast *value* into a float, returning *default* if it fails.
    """
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return default


def _bool(value, default=bool()):
    """
    Attempt to cast *value* into a bool, returning *default* if it fails.
    """
    if value is None:
        return None
    try:
        return bool(value)
    except ValueError:
        return default


def _timestamp_ms(epoch):
    if len(str(epoch)) == 13:
        ms = int(str(epoch)[-3:])
        epoch = float(str(epoch)[0:-3])

    timestamp = datetime.fromtimestamp(float(epoch))
    timestamp += timedelta(milliseconds=ms)

    return timestamp


Base = declarative_base()


class USGSException (Exception):
    pass


class Quake(Base):
    __tablename__ = 'quakes'

    id = Column(String, primary_key=True)

    mag = Column(Float)
    place = Column(String)
    time = Column(DateTime)
    updated = Column(DateTime)
    tz = Column(Interval)
    url = Column(String)
    detail = Column(String)
    felt = Column(Integer)
    cdi = Column(Float)
    mmi = Column(Float)
    alert = Column(String)
    status = Column(String)
    tsunami = Column(Boolean)
    sig = Column(Integer)
    net = Column(String)
    code = Column(String)
    ids = Column(String)
    sources = Column(String)
    types = Column(String)
    nst = Column(Integer)
    dmin = Column(Float)
    rms = Column(Float)
    gap = Column(Float)
    magType = Column(String)
    type = Column(String)

    longitude = Column(Float)
    latitude = Column(Float)
    depth = Column(Float)

    def __init__(self, id, longitude, latitude, depth, mag, place, time, updated, tz, url, detail, felt, cdi, mmi, alert, status, tsunami, sig, net, ids, code, sources, types, nst, dmin, rms, gap, magType, type, title):
        self.id = id
        self.longitude = longitude
        self.latitude = latitude
        self.depth = depth
        self.mag = mag
        self.place = place
        self.time = time
        self.updated = updated
        self.tz = tz
        self.url = url
        self.detail = detail
        self.felt = felt
        self.cdi = cdi
        self.mmi = mmi
        self.alert = alert
        self.status = status
        self.tsunami = tsunami
        self.sig = sig
        self.net = net
        self.ids = ids
        self.code = code
        self.sources = sources
        self.types = types
        self.nst = nst
        self.dmin = dmin
        self.rms = rms
        self.gap = gap
        self.magType = magType
        self.type = type
        self.title = title

    def __unicode__(self):
        return "<Quake {}>".format(self.id)

    def __repr__(self):
        return "<Quake {}>".format(self.id)

    def __str__(self):
        return "<Quake {}>".format(self.id)

    @staticmethod
    def _from_json(json_data):
        """
        Creates a Quake from json data.
        Args:
            json_data (dict): The raw json data to parse.
        Returns:
            Quake: an Earthquake object.
        """
        try:
            coordinates = json_data['geometry']['coordinates']
        except KeyError:
            raise USGSException(
                "The geometry information was not returned " +
                "from the USGS website.")
        try:
            properties = json_data['properties']
        except KeyError:
            raise USGSException(
                "One of the earthquakes did not have any property information")
        return Quake(id=json_data['id'],

                     longitude=_float(coordinates[0]),
                     latitude=_float(coordinates[1]),
                     depth=_float(coordinates[2]),

                     time=_timestamp_ms(properties.get('time')),
                     updated=_timestamp_ms(properties.get('updated')),
                     tz=timedelta(minutes=_int(properties.get('tz'))),

                     mag=_float(properties.get('mag')),
                     place=properties.get('place', str()),

                     title=properties.get('title', str()),
                     url=properties.get('url', str()),
                     detail=properties.get('detail', str()),

                     felt=properties.get('felt'),
                     cdi=properties.get('cdi'),
                     mmi=properties.get('mmi'),
                     alert=properties.get('alert'),
                     status=properties.get('status', str()),

                     tsunami=_bool(properties.get('tsunami', bool())),
                     sig=_int(properties.get('sig')),

                     net=properties.get('net', str()),
                     ids=properties.get('ids', str()),
                     code=properties.get('code', str()),
                     sources=properties.get('sources', str()),
                     types=properties.get('types', str()),
                     nst=properties.get('nst'),

                     dmin=_float(properties.get('dmin')),
                     rms=_float(properties.get('rms')),
                     gap=_float(properties.get('gap')),

                     magType=properties.get('magType'),
                     type=properties.get('type'))


engine = create_engine('sqlite:///quakes.db')
Base.metadata.create_all(engine)
