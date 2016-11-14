from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import numpy as np
import itertools
from ascii_graph import Pyasciigraph

from db_structure import Base, Quake

engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()


q = session.query(Quake.mag).filter(Quake.mag.isnot(None)).all()
mags = list(itertools.chain.from_iterable(q))

(vals, bins) = np.histogram(mags, bins='scott')
histdata = list(itertools.zip_longest(map(str, bins), vals, fillvalue=0))

graph = Pyasciigraph(line_length=120)
for line in graph.graph('title', histdata):
    print(line)
