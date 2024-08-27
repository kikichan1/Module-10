# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
from datetime import datetime, timedelta

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect an existing database into a new model
Base = automap_base()

# Reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Station = Base.classes.station
Measurement = Base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

# Create the homepage route, listing all the available routes
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Analysis of climate in Honolulu, Hawaii<br/><br/>"
        f"Available routes:<br/><br/>"
        f"Precipitation route, fetching precipitation of all stations for the previous year:<br/>"
        f"/api/v1.0/precipitation<br/><br/>"
        f"Station route, fetching the list of stations from the dataset:<br/>"
        f"/api/v1.0/station<br/><br/>"
        f"Temperature route, fetching the dates and temperature observations of the most-active station for the previous year:<br/>"
        f"/api/v1.0/tobs<br/><br/>"
        f"Input the start date in /api/v1.0/start in the format YYYY-MM-DD (e.g. /api/v1.0/2017-08-01), fetching the lowest temperature, average temperature and average temperature of all stations for all the dates greater than or equal to the specified start date:<br/>"
        f"/api/v1.0/<start><br/><br/>"
        f"Input the start date and end date in /api/v1.0/start/end in the format YYYY-MM-DD (e.g. /api/v1.0/2017-08-01/2017-08-23), fetching the lowest temperature, average temperature and average temperature of all stations for the dates from the start date to the end date, inclusive:<br/>"
        f"/api/v1.0/<start>/<end>"    
    )

most_recent_date = session.query(func.max(Measurement.date)).scalar()
most_recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
one_year_back = most_recent_date - timedelta(days=365, hours=23)

# Create the precipitation route,
# fetching precipitation of
# all stations for the previous year of data
@app.route("/api/v1.0/precipitation")

def precipitation():
    results_precipitation = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_back).\
        filter(Measurement.date <= most_recent_date).all()
    
    session.close()
    
    date_precipitation = []
    for date, prcp in results_precipitation:
        date_precipitation_dict = {date: prcp}
        date_precipitation.append(date_precipitation_dict)
    
    return jsonify(date_precipitation)

# Create the station route,
# fetching the list of stations from the dataset
@app.route("/api/v1.0/station")
def station():
    results = session.query(Station.station).all()
    
    session.close()

    station_list = []
    station_list = [{'station': result[0]} for result in results]

    return jsonify(station_list)

# Create the temperature route,
# fetching the dates and temperature observations of 
# the most-active station for the previous year
@app.route("/api/v1.0/tobs")
def temperature():
    results_temperature = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= one_year_back).\
        filter(Measurement.date <= most_recent_date).\
        filter(Measurement.station == 'USC00519281').\
        all()

    session.close()
    
    date_temperature = []
    for date, tobs in results_temperature:
        date_temperature_dict = {date: tobs}
        date_temperature.append(date_temperature_dict)
    
    return jsonify(date_temperature)

# Create the specified start date route, 
# fetching the lowest temperature, average temperature and average temperature of
# all stations for all the dates greater than or equal to the specified start date
@app.route("/api/v1.0/<start>")

def temperature_by_start_date(start):
    lowest_temperature = session.query(func.min(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        scalar()

    average_temperature = session.query(func.avg(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        scalar()

    highest_temperature = session.query(func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        scalar()

    session.close()
    
    result_by_start_date_dict = {
        "date_range": {
            "start": start,
            "end": '2017-08-23'           
        },
        "tmin": lowest_temperature,
        "tavg": average_temperature,
        "tmax": highest_temperature
    }
    
    return jsonify(result_by_start_date_dict)

# Create the specified start date and end date route,
# fetching the lowest temperature, average temperature and average temperature of
# all stations for the dates from the start date to the end date, inclusive
@app.route("/api/v1.0/<start>/<end>")

def temperature_by_range(start, end):
    lowest_temperature_range = session.query(func.min(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).\
        scalar()

    average_temperature_range = session.query(func.avg(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).\
        scalar()

    highest_temperature_range = session.query(func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).\
        scalar()

    session.close()
    
    result_by_range_dict = {
        "date_range": {
            "start": start,
            "end": end           
        },
        "tmin": lowest_temperature_range,
        "tavg": average_temperature_range,
        "tmax": highest_temperature_range
    }
    
    return jsonify(result_by_range_dict)

if __name__ == '__main__':
    app.run(debug=True)