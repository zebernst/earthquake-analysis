from sqlalchemy import Table, Column, create_engine, ForeignKey
from sqlalchemy import Integer, String, Float, DateTime, Interval, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta, timezone
import requests


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


def _timestamp_ms(value):
    t = value / 1000.0
    return datetime.fromtimestamp(t)


Base = declarative_base()


class USGSException (Exception):
    pass


url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{}_{}.geojson'
ALLOWED_LEVELS = {"significant", "4.5", "2.5", "1.0", "all"}
ALLOWED_PERIODS = {"hour", "day", "week", "month"}


association_table = Table('association_table', Base.metadata,
                          Column('feed_id', Integer, ForeignKey('feeds.id')),
                          Column('quake_id', String, ForeignKey('quakes.id')))


class Feed(Base):
    __tablename__ = 'feeds'

    id = Column(Integer, primary_key=True)

    level = Column(String)
    period = Column(String)

    url = Column(String)
    title = Column(String)
    time = Column(DateTime)
    api = Column(String)

    bbox = relationship('BoundingBox', uselist=False, back_populates='feed')
    quakes = relationship('Quake',
                          secondary=association_table,
                          back_populates='feeds')

    def __init__(self, level, period):
        """
        Captures the feed with the given severity level and period.
        ValueError is raised if either severity level or period are not
        supported by the USGS data feeds.  IOError is raised if the
        response status following the request for data is not 200 OK.
        """
        if level.lower() not in ALLOWED_LEVELS:
            raise ValueError("invalid severity level")
        if period.lower() not in ALLOWED_PERIODS:
            raise ValueError("invalid period")

        feed_url = url.format(level.lower(), period.lower())
        response = requests.get(feed_url)

        self.level = level.capitalize()
        self.period = period.capitalize()

        if response.status_code != 200:
            message = "HTTP status code {:d}".format(response.status_code)
            raise IOError(message)

        self.data = response.json()
        metadata = self.data['metadata']
        self.url = metadata["url"]
        self.title = metadata["title"]
        self.time = datetime.fromtimestamp(metadata["generated"] / 1000.0,
                                           tz=timezone.utc)
        self.api = metadata["api"]
        self.count = metadata['count']

    # Special methods
    def __len__(self):
        """
        Returns the number of events captured from the data feed.
        """
        return self.count

    def __unicode__(self):
        return "<Feed: {lvl} {per} | {dt}>".format(lvl=self.level,
                                                   per=self.period,
                                                   dt=self.time.isoformat())

    def __repr__(self):
        return "<Feed: {lvl} {per} | {dt}>".format(lvl=self.level,
                                                   per=self.period,
                                                   dt=self.time.isoformat())

    def __str__(self):
        return "<Feed: {lvl} {per} | {dt}>".format(lvl=self.level,
                                                   per=self.period,
                                                   dt=self.time.isoformat())

    # Iterators for specific event data
    @property
    def events(self):
        """
        A generator that allows iteration over all events in the feed.
        """
        for event in self.data["features"]:
            yield event

    # Other methods
    def refresh(self):
        """
        Updates this feed with new data.
        IOError is raised if data acquisition fails.
        """
        response = requests.get(self.url)

        if response.status_code != 200:
            message = "HTTP status code {:d}".format(response.status_code)
            raise IOError(message)

        self.data = response.json()
        metadata = self.data['metadata']
        self.url = metadata["url"]
        self.title = metadata["title"]
        self.time = datetime.fromtimestamp(metadata["generated"] / 1000.0,
                                           tz=timezone.utc)
        self.api = metadata["api"]
        self.count = metadata['count']


class BoundingBox(Base):
    __tablename__ = 'bboxes'

    id = Column(Integer, primary_key=True)
    min_longitude = Column(Float)
    max_longitude = Column(Float)
    min_latitude = Column(Float)
    max_latitude = Column(Float)
    min_depth = Column(Float)
    max_depth = Column(Float)
    feed_id = Column(Integer, ForeignKey('feeds.id'))

    feed = relationship('Feed', back_populates='bbox')

    def __init__(self, min_lon, min_lat, min_dep, max_lon, max_lat, max_dep):
        self.min_longitude = min_lon
        self.max_longitude = max_lon
        self.min_latitude = min_lat
        self.max_latitude = max_lat
        self.min_depth = min_dep
        self.max_depth = max_dep

    # Special methods
    def __unicode__(self):
        return '<BoundingBox ({0:.2f}, {1:.2f}), ({2:.2f}, {3:.2f})>'.\
            format(self.min_longitude, self.min_latitude,
                   self.max_longitude, self.max_latitude)

    def __repr__(self):
        return '<BoundingBox ({0:.2f}, {1:.2f}), ({2:.2f}, {3:.2f})>'.\
            format(self.min_longitude, self.min_latitude,
                   self.max_longitude, self.max_latitude)

    def __str__(self):
        return '<BoundingBox ({0:.2f}, {1:.2f}), ({2:.2f}, {3:.2f})>'.\
            format(self.min_longitude, self.min_latitude,
                   self.max_longitude, self.max_latitude)

    @staticmethod
    def _from_json(json_data):
        return BoundingBox(min_lon=json_data[0],
                           min_lat=json_data[1],
                           min_dep=json_data[2],
                           max_lon=json_data[3],
                           max_lat=json_data[4],
                           max_dep=json_data[5])


class Quake(Base):
    __tablename__ = 'quakes'

    id = Column(String, primary_key=True)
    title = Column(String)

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

    feeds = relationship('Feed',
                         secondary=association_table,
                         back_populates='quakes')

    def __init__(self, id, longitude, latitude, depth, mag, place, time,
                 updated, tz, url, detail, felt, cdi, mmi, alert, status,
                 tsunami, sig, net, ids, code, sources, types, nst, dmin, rms,
                 gap, magType, type, title):
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
