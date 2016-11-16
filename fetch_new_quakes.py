from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_structure import Base, Feed, BoundingBox, Quake
import inflect
from tqdm import tqdm
from matplotlib.cbook import dedent

# Initialize database connection
engine = create_engine('sqlite:///quakes_test.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()


# Fetch new earthquake data
feed = Feed('all', 'day')
feed.bbox = BoundingBox._from_json(feed.data['bbox'])
session.add(feed)
session.flush()


# Add new events to database
def unique_instance(session, feed, event):
    row = session.query(Quake).get(event['id'])
    if not row:
        row = Quake._from_json(event)
        return row, True
    if row:
        new_inst = Quake._from_json(event)
        for column in row.__table__.columns:
            value = getattr(new_inst, column.name)
            setattr(row, column.name, value)
        return row, False

tqdm_args = {
    'desc': 'Updating database',
    'total': feed.count,
    'leave': False,
    'unit': '',
    'unit_scale': True,
    'dynamic_ncols': True,
    'bar_format': '{l_bar}{bar}| {n_fmt}/{total_fmt} '
                  '[{elapsed}@ eta:{remaining}, {rate_fmt}]'
    }

new, upd = 0, 0
for event in tqdm(iterable=feed.events, **tqdm_args):
    instance, created = unique_instance(session, feed, event)
    feed.quakes.append(instance)
    if created:
        new += 1
    if not created:
        upd += 1

session.flush()
session.commit()

# Print status to console
p = inflect.engine()
print(dedent('''
             {new_events} added
             {upd_events} updated
             '''.format(new_events=p.no('event', new),
                        upd_events=p.no('event', upd))))
