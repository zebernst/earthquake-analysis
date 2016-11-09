from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np

from db_structure import Base, Quake

engine = create_engine('sqlite:///quakes_float.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

lats, lons = [], []
magnitudes = []
for event in session.query(Quake).all():
    if event.latitude and event.longitude and event.mag:
        lats.append(event.latitude)
        lons.append(event.longitude)
        magnitudes.append(event.mag)

eq_map = Basemap(projection='robin', resolution='l', area_thresh=1000.0,
                 lat_0=0, lon_0=-130)
eq_map.drawcoastlines()
eq_map.drawcountries()
eq_map.fillcontinents(color='gray')
eq_map.drawmapboundary()
eq_map.drawmeridians(np.arange(0, 360, 30))
eq_map.drawparallels(np.arange(-90, 90, 30))

min_marker_size = 2.5
for lon, lat, mag in zip(lons, lats, magnitudes):
    x, y = eq_map(lon, lat)
    msize = float(mag) * min_marker_size
    eq_map.plot(x, y, 'ro', markersize=msize)

plt.show()
