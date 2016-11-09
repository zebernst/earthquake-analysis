from sqlalchemy.orm import sessionmaker
from sqlalchemy import exists, create_engine
import requests

from db_structure import Base, Quake

engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

api_url = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/'
feed_url = api_url + '{}_{}.geojson'.format('all', 'month')
# levels = ["significant", "4.5", "2.5", "1.0", "all"]
# periods = ["hour", "day", "week", "month"]

lastmodified = session.query(Quake).order_by(Quake.time.desc()).first()
# TODO: automatically select correct feed to grab based on timedelta

r = requests.get(feed_url)
geodata = r.json()

new_quakes = []
for event in geodata['features']:
    duplicate = session.query(exists().where(Quake.id == event['id'])).scalar()
    if duplicate is not True:
        new_quakes.append(Quake._from_json(event))

session.add_all(new_quakes)
session.commit()
print('Added {} new events to the database!'.format(len(new_quakes)))
