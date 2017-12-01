from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import itertools
from ascii_graph import Pyasciigraph

from db_structure import Base, Quake

engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()


q = session.query(Quake.mag).filter(Quake.mag.isnot(None)).all()
df = pd.DataFrame(data=q, columns=['mag'])

(vals, bins) = np.histogram(df, bins=np.linspace(0,10,num=100))
hist_df = pd.DataFrame(data=list(zip(vals, bins)), columns=['count', 'bin'])

histdata = list(itertools.zip_longest(map(str, hist_df['bin'].round(1)), hist_df['count'], fillvalue=0))

graph = Pyasciigraph(line_length=120)
for line in graph.graph('Magnitude', histdata):
    print(line)
