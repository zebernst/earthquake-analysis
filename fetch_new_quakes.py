from sqlalchemy.orm import sessionmaker
from sqlalchemy import exists, create_engine
import requests
import inflect

from db_structure import Base, Quake

engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()
p = inflect.engine()

api_url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/'
feed_url = api_url + '{}_{}.geojson'.format('all', 'week')
# levels = ["significant", "4.5", "2.5", "1.0", "all"]
# periods = ["hour", "day", "week", "month"]

lastmodified = session.query(Quake).order_by(Quake.time.desc()).first()
# TODO: automatically select correct feed to grab based on timedelta

saved_ids = set(list(zip(*session.query(Quake.id)))[0])
geodata = requests.get(feed_url).json()
new_events = []

for event in geodata['features']:
    if event['id'] not in saved_ids:
        new_events.append(Quake._from_json(event))

session.add_all(new_events)
session.commit()

num = len(new_events)
print('Added {num} new {events} to the database.'
      .format(num=num, events=p.plural_noun('event', num)))
