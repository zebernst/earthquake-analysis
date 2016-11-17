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


def _datetime(value):
    """
    Convert an epoch timestamp (in ms) to a datetime object.
    """
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
    """
    An object representing the data contained in a USGS feed.

    Attributes:
        level (str):    The severity level to filter events by.
            The only valid values of this parameter are "significant",
            "4.5", "2.5", "1.0", and "all".
        period (str):   The time period that events are confined to.
            The only valid values of this parameter are "hour", "day",
            "week", and "month".
        data (dict):    The JSON data for the events contained in the feed.
        url (str):      The url that the feed was retrieved from.
        title (str):    The title of the feed.
        time (datetime.datetime): The time that the feed was generated.
        api (str):      The API version of the USGS feed.
        count (int):    The number of events contained in the feed.
    """
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

        ValueError is raised if either severity level or period given are not
        supported by the USGS data feeds.  IOError is raised if the
        response status following the request for data is not '200 OK'.
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

    def __repr__(self):
        return "<Feed: {lvl} {per} | {dt}>".format(
            lvl=self.level,
            per=self.period,
            dt=self.time.isoformat())

    def __str__(self):
        return "<Feed: {lvl} {per} | {dt}>".format(
            lvl=self.level,
            per=self.period,
            dt=self.time.isoformat())

    # Iterators for specific event data
    @property
    def events(self):
        """
        generator: Iterates over all events contained in the feed.
        """
        for event in self.data["features"]:
            yield event

    # Other methods
    def refresh(self):
        """
        Update feed with new data, raising IOError if data acquisition fails.
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
    """
    An object representing the area that contains all events in a given Feed.

    Attributes:
        min_longitude (float):  The western border of the bounding box.
        max_longitude (float):  The eastern border of the bounding box.
        min_latitude (float):   The southern border of the bounding box.
        max_latitude (float):   The northern border of the bounding box.
        min_depth (float):      The deepest boundary of the bounding box.
        max_depth (float):      The shallowest boundary of the bounding box.
    """
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

    # Special methods
    def __repr__(self):
        return '<BoundingBox ({0:.2f}, {1:.2f}), ({2:.2f}, {3:.2f})>'.format(
            self.min_longitude, self.min_latitude,
            self.max_longitude, self.max_latitude)

    def __str__(self):
        return '<BoundingBox ({0:.2f}, {1:.2f}), ({2:.2f}, {3:.2f})>'.format(
            self.min_longitude, self.min_latitude,
            self.max_longitude, self.max_latitude)

    @classmethod
    def instantiate(cls, json_data):
        """
        Creates a BoundingBox from json data.

        Args:
            json_data (dict): The raw json data to parse.

        Returns:
            BoundingBox: a BoundingBox object.
        """
        instance = cls()
        instance.min_longitude = json_data[0]
        instance.max_longitude = json_data[1]
        instance.min_latitude = json_data[2]
        instance.max_latitude = json_data[3]
        instance.min_depth = json_data[4]
        instance.max_depth = json_data[5]

        return instance


class Quake(Base):
    """
    An object representing an earthquake.

    Attributes:
        id (str):           A unique identifier for the earthquake.
        longitude (float):  Latitude of the earthquake (in degrees).
        latitude (float):   Longitude of the earthquake (in degrees).
        depth (float):      Depth of the earthquake (in km).
        mag (float):        Magnitude of the earthquake.
        title (str):        Short summary of the event (magnitude and place).
        place (str):        The description of a region near the event.
        time (datetime.datetime):     The time that the event occurred.
        updated (datetime.datetime):  The time the event data was last updated.
        tz (datetime.timedelta):      The timezone offset from UTC (in mins).
        url (str):        The webpage corresponding to this earthquake.
        detail (str):     The url of this event's detailed geojson feed.
        felt (int):       The total number of felt reports submitted to DYFI?
        cdi (float):      The maximum reported intensity for the event.
        mmi (float):      The maximum estimated instrumental intensity.
        alert (str):      The alert level from the PAGER impact scale.
        status (str):     Indicates whether a human has reviewed the event.
        tsunami (bool):   True if event was significant and in the ocean.
        sig (int):        A number describing the significance of the event.
        net (str):        A contributor id, denotes the preferred network.
        code (str):       A unique code assigned by the corresponding source.
        ids (str):        A list of unique ids assigned to the event.
        sources (str):    A list of network contributors.
        types (str):      A list of product types associated with the event.
        nst (int):        Total number of stations used to determine location.
        dmin (float):     Distance from the epicenter to the nearest station.
        rms (float):      The root-mean-square travel time, in seconds.
        gap (float):      The largest azimuthal gap between adjacent stations.
        magType (str):    The method used to calculate the preferred magnitude.
        type (str):       Type of seismic event.
    """
    __tablename__ = 'quakes'

    id = Column(String, primary_key=True)
    longitude = Column(Float)
    latitude = Column(Float)
    depth = Column(Float)
    mag = Column(Float)
    title = Column(String)
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

    feeds = relationship('Feed',
                         secondary=association_table,
                         back_populates='quakes')

    def __repr__(self):
        return "<Quake {}>".format(self.id)

    def __str__(self):
        return "<Quake {}>".format(self.id)

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False

        self_attrs = {c.name: getattr(self, c.name)
                      for c in self.__table__.columns}
        other_attrs = {c.name: getattr(other, c.name)
                       for c in other.__table__.columns}
        return self_attrs == other_attrs

    @classmethod
    def instantiate(cls, json_data):
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

        instance = cls()

        instance.id = json_data['id']
        instance.longitude = _float(coordinates[0])
        instance.latitude = _float(coordinates[1])
        instance.mag = _float(properties.get('mag'))

        instance.depth = _float(coordinates[2])
        instance.place = properties.get('place')

        instance.time = _datetime(properties.get('time'))
        instance.updated = _datetime(properties.get('updated'))
        instance.tz = timedelta(minutes=_int(properties.get('tz')))

        instance.title = properties.get('title')
        instance.url = properties.get('url')
        instance.detail = properties.get('detail')

        instance.felt = properties.get('felt')
        instance.cdi = properties.get('cdi')
        instance.mmi = properties.get('mmi')
        instance.alert = properties.get('alert')
        instance.status = properties.get('status')
        instance.tsunami = _bool(properties.get('tsunami'))
        instance.sig = _int(properties.get('sig'))
        instance.net = properties.get('net')
        instance.ids = properties.get('ids')
        instance.code = properties.get('code')
        instance.sources = properties.get('sources')
        instance.types = properties.get('types')
        instance.nst = properties.get('nst')
        instance.dmin = _float(properties.get('dmin'))
        instance.rms = _float(properties.get('rms'))
        instance.gap = _float(properties.get('gap'))
        instance.magType = properties.get('magType')
        instance.type = properties.get('type')

        return instance


engine = create_engine('sqlite:///quakes.db')
Base.metadata.create_all(engine)
