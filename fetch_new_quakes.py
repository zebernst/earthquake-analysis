from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import requests
import inflect
import pickle

from db_structure import Base, Quake
from db_structure import _timestamp_ms

# Initialize database connection
engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

# Fetch new earthquake data
try:
    with open('meta.pk', 'rb') as fi:
        lastmeta = pickle.load(fi)
except FileNotFoundError:
    pass

api_url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/'
feed_url = api_url + '{}_{}.geojson'.format('all', 'week')
# levels = ["significant", "4.5", "2.5", "1.0", "all"]
# periods = ["hour", "day", "week", "month"]
geodata = requests.get(feed_url).json()

# Add new events to database
new_events = []
saved_ids = set(list(zip(*session.query(Quake.id)))[0])
for event in geodata['features']:
    if event['id'] not in saved_ids:
        new_events.append(Quake._from_json(event))

session.add_all(new_events)
session.commit()

# Save feed metadata to disk
metadata = geodata['metadata']
metadata['generated'] = _timestamp_ms(metadata['generated'])

with open('meta.pk', 'wb') as fi:
    pickle.dump(metadata, fi)

# Print status to console
num = len(new_events)
print('Added {num} new {events} to the database.'
      .format(num=num,
              events=inflect.engine().plural_noun('event', num)))
