from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from db_structure import Base, Feed, BoundingBox, Quake, valid_levels, valid_periods
from tqdm import tqdm
import inflect
import argparse

# Initialize database connection
engine = create_engine('sqlite:///quakes.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Fetches new earthquakes from data source.')
parser.add_argument('-l', '--level', default='all', choices=valid_levels)
parser.add_argument('-p', '--period', default='day', choices=valid_periods)
args = parser.parse_args()

# Fetch new earthquake data
feed = Feed(level=args.level, period=args.period)
feed.bbox = BoundingBox.instantiate(feed.data['bbox'])
session.add(feed)
session.flush()

# Add new events to database
new, upd = 0, 0
tqdm_args = {
    'desc': 'Updating database',
    'total': feed.count,
    'leave': True,
    'unit': '',
    'unit_scale': True,
    'dynamic_ncols': True,
    'bar_format': '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}]'
    }

def update_row(row, new_obj):
    """Update each attribute of an existing row with new data"""
    for column in row.__table__.columns:
        value = getattr(new_obj, column.name)
        setattr(row, column.name, value)
    return

for event in tqdm(feed.events, **tqdm_args):
    quake_obj = session.query(Quake).get(event['id'])
    if quake_obj is None:
        quake_obj = Quake.instantiate(event)
        setattr(quake_obj, 'original_feed', feed)
        new += 1
    elif quake_obj is not None:
        new_inst = Quake.instantiate(event)
        setattr(new_inst, 'modified_feed', feed)
        update_row(quake_obj, new_inst)
        upd += 1
    session.add(quake_obj)
session.flush()

commit_confirmation = input('Commit changes to database? [Y/n]: ')
if commit_confirmation.lower().startswith('y'):
    session.commit()

    # Print status to console
    p = inflect.engine()
    print('{new} added\n{upd} updated'.format(new=p.no('new event', new), 
                                              upd=p.no('event', upd)))
