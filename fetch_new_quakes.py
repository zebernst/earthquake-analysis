from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_structure import Base, Feed, BoundingBox, Quake
import inflect
from tqdm import tqdm
from textwrap import dedent


def update_row(row, new_obj):
    """Update each attribute of an existing row with new data"""
    for column in row.__table__.columns:
        value = getattr(row, column.name)
        setattr(row, column.name, value)
    return

# Initialize database connection
engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

# Fetch new earthquake data
feed = Feed('all', 'day')
feed.bbox = BoundingBox.instantiate(feed.data['bbox'])
session.add(feed)
session.flush()

# Add new events to database
new, upd = 0, 0
tqdm_args = {
    'desc': 'Updating database',
    'total': feed.count,
    'leave': False,
    'unit': '',
    'unit_scale': True,
    'dynamic_ncols': True,
    'bar_format': '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}]'
    }

for event in tqdm(feed.events, **tqdm_args):
    quake_obj = session.query(Quake).get(event['id'])
    if not quake_obj:
        quake_obj = Quake.instantiate(event)
        new += 1
    if quake_obj:
        new_inst = Quake.instantiate(event)
        update_row(quake_obj, new_inst)
        upd += 1
    feed.quakes.append(quake_obj)
session.flush()
# TODO: add checks before actually committing to disk
# session.commit()

# Print status to console
p = inflect.engine()
status_str = dedent('''\
                    {new} added
                    {upd} updated
                    ''')
print(status_str.format(new=p.no('event', new), upd=p.no('event', upd)))
