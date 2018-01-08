from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db_structure import Base, Feed, BoundingBox, Quake, valid_levels, valid_periods
from tqdm import tqdm
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
new, upd, err = 0, 0, 0
tqdm_args = {
    'desc': 'Updating database',
    'total': feed.count,
    'leave': True,
    'unit': '',
    'unit_scale': True,
    'dynamic_ncols': True,
    'bar_format': '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}]'
    }

def update_row(db_row, new_obj):
    """Update each attribute of an existing row with new data"""
    try:
        for col in db_row.__table__.columns:
            value = getattr(new_obj, col.name)
            setattr(db_row, col.name, value)
        return True #success
    except:
        return False #fail

for event in tqdm(feed.events, **tqdm_args):
    # query database by quake id to see if event already exists in database
    quake_obj = session.query(Quake).get(event['id'])
    try:
        if quake_obj is None:
            # if no match in database, make a new quake instance 
            # and associate it with the Feed.
            quake_obj = Quake.instantiate(event)
            setattr(quake_obj, 'original_feed', feed)
            new += 1
        elif quake_obj is not None:
            # if there is a match in the database, make a new quake instance, 
            # associate it with the Feed, and update the attributes of the 
            # original quake in the database with those of the new one.
            new_inst = Quake.instantiate(event)
            setattr(new_inst, 'modified_feed', feed)
            update_row(quake_obj, new_inst)
            upd += 1
        session.add(quake_obj)
    except TypeError:
        # if there's a TypeError while updating the events, 
        # ignore that specific event and move on to the next loop.
        err += 1
        continue
session.flush()

# Print status to terminal
print('{:>4d} event(s) added'.format(new))
print('{:>4d} event(s) updated'.format(upd))
if err > 0:
    print('{:>4d} event(s) skipped due to invalid data'.format(err))

# commit_confirmation = input('Commit changes to database? [Y/n]: ')
if input('Commit changes to database? [Y/n]: ').lower().startswith('y'):
    session.commit()
