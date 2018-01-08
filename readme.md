# Earthquake Analysis

## Python

##### [db_structure.py](db_structure.py)
this file describes the structure of the sqlite database, which is described using sqlalchemy. it also contains data structures which are used in every other python file.

##### [fetch_new_quakes.py](fetch_new_quakes.py)
run `python fetch_new_quakes.py -l LEVEL -p PERIOD` to fetch new data from the USGS and add it to the database. valid levels include `significant`, `4.5`, `2.5`, `1.0`, and `all`. valid periods include `hour`, `day`, `week`, and `month`.

##### [hist_mag.py](hist_mag.py)
plots a rough histogram showing the magnitudes of the earthquakes contained in the sqlite database. outputs to the command line.

## R
*created for my STAT 087 - Intro to Data Science course*

##### [earthquakes.md](earthquakes.md)
a report on trends found in the data gathered. output tailored specially for github.

##### [earthquakes.rmd](earthquakes.rmd)
the R markdown source code behind the [earthquakes.md](earthquakes.md) file. 
