from flask import Flask, Markup, request, redirect, render_template, jsonify
from flask_cors import CORS
import requests
from datetime import date
import time
import os
import json
from fred import Fred
import pandas as pd

#INIT THE CLIENT
API_KEY = os.environ.get('FRED_API_KEY')
fr = Fred(api_key=API_KEY,response_type='json')

#creates instance of app
app = Flask(__name__)
CORS(app)
app.config.from_object(__name__)

@app.route("/")
def health():
    return 'OK'    

@app.route("/retrievehousingstarts")
def retrieveData():

    #call the historical housing starts series
    observations = json.dumps(json.loads(fr.series.observations('HOUST'))['observations'])
    HOUST_DF = pd.read_json(observations)
    #convert date strings to proper datetimes
    HOUST_DF['date'] = pd.to_datetime(HOUST_DF['date'])
    HOUST_DF.columns = ['realtime_start', 'realtime_end', 'date', 'HOUSING_STARTS']
    HOUST_DF.drop(axis=1, columns=['realtime_start','realtime_end'], inplace=True)
    HOUST_DF.set_index("date", inplace=True)
    HOUST_DF = HOUST_DF.resample('D').ffill() #this forward fills the previous value up until a new value exists

    return(HOUST_DF.to_json())

@app.route("/fredsearch")
def fredsearch():

    return 'Search results'    

#instantiate app
if __name__ == "__main__":
    app.run(debug=True, port=3000)