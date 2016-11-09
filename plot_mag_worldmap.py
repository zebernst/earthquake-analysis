from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np

from db_structure import Base, Quake

engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

q = session.query(Quake.latitude, Quake.longitude, Quake.mag)
q = q.filter(Quake.latitude.isnot(None))
q = q.filter(Quake.longitude.isnot(None))
q = q.filter(Quake.mag.isnot(None))

results = list(zip(*q.all()))
lats = results[0]
lons = results[1]
mags = results[2]


def get_marker_color(magnitude):
    # Returns green for small earthquakes, yellow for moderate
    #  earthquakes, and red for significant earthquakes.
    if magnitude < 3.0:
        return ('go')
    elif magnitude < 5.0:
        return ('yo')
    else:
        return ('ro')


# Make this plot larger.
plt.figure(figsize=(16, 12))

eq_map = Basemap(projection='robin', resolution='l', area_thresh=1000.0,
                 lat_0=0, lon_0=-130)
eq_map.drawcoastlines()
eq_map.drawcountries()
eq_map.fillcontinents(color='gray')
eq_map.drawmapboundary()
eq_map.drawmeridians(np.arange(0, 360, 30))
eq_map.drawparallels(np.arange(-90, 90, 30))

min_marker_size = 2.5
for lon, lat, mag in zip(lons, lats, mags):
    x, y = eq_map(lon, lat)
    msize = float(mag) * min_marker_size
    marker_string = get_marker_color(mag)
    eq_map.plot(x, y, marker_string, markersize=msize)

plt.show()
