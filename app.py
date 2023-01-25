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
def retrieveHousingData():

    #call the historical housing starts series
    observations = json.dumps(json.loads(fr.series.observations('HOUST'))['observations'])
    HOUST_DF = pd.read_json(observations)
    #convert date strings to proper datetimes
    HOUST_DF['date'] = pd.to_datetime(HOUST_DF['date'])
    HOUST_DF.columns = ['realtime_start', 'realtime_end', 'date', 'HOUSING_STARTS']
    HOUST_DF.drop(axis=1, columns=['realtime_start','realtime_end'], inplace=True)
    HOUST_DF.set_index("date", inplace=True)
    HOUST_DF = HOUST_DF.resample('D').ffill() #this forward fills the previous value up until a new value exists

    HOUST_DF.index = HOUST_DF.index.strftime('%Y/%m/%d')

    return(HOUST_DF.to_json())

@app.route("/retrievemortgagerates")
def retrieveMortgageData():

    #call historical mortgage rates
    observations = json.dumps(json.loads(fr.series.observations('MORTGAGE30US'))['observations'])
    MTG_DF = pd.read_json(observations) 
    MTG_DF.columns = ['realtime_start', 'realtime_end', 'date', 'MTG_RATE']

    #convert date strings to proper datetimes
    MTG_DF['date'] = pd.to_datetime(MTG_DF['date'])
    MTG_DF.columns = ['realtime_start', 'realtime_end', 'date', 'MTG_RATE']
    MTG_DF.drop(axis=1, columns=['realtime_start','realtime_end'], inplace=True)
    MTG_DF.set_index('date', inplace=True)
    # using the resample method
    # https://pandas.pydata.org/docs/reference/api/pandas.core.resample.Resampler.fillna.html
    MTG_DF = MTG_DF.resample('D').ffill() #this forward fills the previous value up until a new value exists

    MTG_DF.index = MTG_DF.index.strftime('%Y/%m/%d')

    return(MTG_DF.to_json())

@app.route("/mergedatasets")
def mergeDatasets():
    
    #for dataset in datasets:
    #call historical mortgage rates
    observations = json.dumps(json.loads(fr.series.observations('MORTGAGE30US'))['observations'])
    MTG_DF = pd.read_json(observations) 
    MTG_DF.columns = ['realtime_start', 'realtime_end', 'date', 'MTG_RATE']

    #convert date strings to proper datetimes
    MTG_DF['date'] = pd.to_datetime(MTG_DF['date'])
    MTG_DF.columns = ['realtime_start', 'realtime_end', 'date', 'MTG_RATE']
    MTG_DF.drop(axis=1, columns=['realtime_start','realtime_end'], inplace=True)
    MTG_DF.set_index('date', inplace=True)
    # using the resample method
    # https://pandas.pydata.org/docs/reference/api/pandas.core.resample.Resampler.fillna.html
    MTG_DF = MTG_DF.resample('D').ffill() #this forward fills the previous value up until a new value exists

    #call the historical housing starts series
    observations = json.dumps(json.loads(fr.series.observations('HOUST'))['observations'])
    HOUST_DF = pd.read_json(observations)
    #convert date strings to proper datetimes
    HOUST_DF['date'] = pd.to_datetime(HOUST_DF['date'])
    HOUST_DF.columns = ['realtime_start', 'realtime_end', 'date', 'HOUSING_STARTS']
    HOUST_DF.drop(axis=1, columns=['realtime_start','realtime_end'], inplace=True)
    HOUST_DF.set_index("date", inplace=True)
    HOUST_DF = HOUST_DF.resample('D').ffill() #this forward fills the previous value up until a new value exists

    MAIN_FRAME = pd.merge(MTG_DF, HOUST_DF, left_index=True, right_index=True)
    MAIN_FRAME.index = MAIN_FRAME.index.strftime('%Y/%m/%d')

    return(MAIN_FRAME.to_json())

@app.route("/fredsearch")
def fredsearch():

    searchKey = request.args.get('searchKey')

    if searchKey == None:
        return "Error: no search terms provided"

    params = {'limit':50,}
    res = fr.series.search(searchKey,params=params)
    # print(res)
    observations = json.dumps(json.loads(res))
    SEARCH_RES_DF = pd.read_json(observations)
    SEARCH_RES_DF_NEW = SEARCH_RES_DF['seriess']
    # print(SEARCH_RES_DF_NEW.head())
    # Convert back to JSON
    return(SEARCH_RES_DF_NEW.to_json())

#instantiate app
if __name__ == "__main__":
    app.run(debug=True, port=3000)