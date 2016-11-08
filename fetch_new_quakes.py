from sqlalchemy.orm import sessionmaker
from sqlalchemy import exists, create_engine
import requests

from db_structure import Base, Quake

engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

api_url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/'
feed_url = api_url + '{}_{}.geojson'.format('all', 'day')
# levels = ["significant", "4.5", "2.5", "1.0", "all"]
# periods = ["hour", "day", "week", "month"]

r = requests.get(feed_url)
geodata = r.json()

new_quakes = []
for event in geodata['features']:
    exists = session.query(exists().where(Quake.id == event['id'])).scalar()
    if exists is False:
        new_quakes.append(Quake._from_json(event))

session.add_all(new_quakes)
session.commit()
