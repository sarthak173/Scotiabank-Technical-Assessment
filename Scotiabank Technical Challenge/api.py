from flask import Flask
import pandas as pd
from statistics import mean
import numpy as np 
from dask import dataframe as dd
from os import path
import json

import geopy
from geopy.geocoders import Nominatim 
from geopy.point import Point 



app = Flask(__name__)

def determine_season(row):
    
    if row['Month'] == '01' or row['Month'] == '02' or row['Month'] == '03' or row['Month'] == '04':
        season = 'Winter'
    elif row['Month'] == '05' or row['Month'] == '06' or row['Month'] == '07' or row['Month'] == '08':
        season = 'Spring'
    else:
        season = 'Fall'
    return season

@app.route('/test/<string:station>')
def display(station):
    path = '..\\Jobs\\'+station+'.csv'
    df = pd.read_csv(path)
    res = []
    winter_avg = []
    spring_avg = []
    fall_avg = []
    # extract year and month information
    df['Year'] = df['DATE'].str[:4]
    df['Month'] = df['DATE'].str[-2:]
    
    print(df.head())
    uniqiue_years = df['Year'].unique()
    for year in uniqiue_years:
        #initialize dictionaries to store information for each year's seasons
        winter_data = {}
        spring_data = {}
        fall_data = {}

        # search for rows with current unique year and make sure that the average temperature is not null
        # define seasons as 1-4: winter, 5-8: spring, 9-12: fall
        for idx, row in df.iterrows():
            if row['Year'] == year and np.isnan(row['TAVG']) == False:
                if row['Month'] == '01' or row['Month'] == '02' or row['Month'] == '03' or row['Month'] == '04':
                    season = 'Winter'
                    winter_avg.append(row['TAVG'])
                elif row['Month'] == '05' or row['Month'] == '06' or row['Month'] == '07' or row['Month'] == '08':
                    season = 'Spring'
                    spring_avg.append(row['TAVG'])
                else:
                    season = 'Fall'
                    fall_avg.append(row['TAVG']) 
        #add all average temperature values to a list and if that list exists, compute the average value for the list and construct a dictionary
        if len(winter_avg) != 0:
            winter_avg_val = mean(winter_avg)
            winter_data['Year'] = year
            winter_data['Season'] = 'Winter'
            winter_data['Average Temperature'] = winter_avg_val
            res.append(winter_data)
        if len(spring_avg) != 0:
            spring_avg_val = mean(spring_avg)
            spring_data['Year'] = year
            spring_data['Season'] = 'Spring'
            spring_data['Average Temperature'] = spring_avg_val
            res.append(spring_data)
        if len(fall_avg) != 0:
            fall_avg_val = mean(fall_avg)
            fall_data['Year'] = year
            fall_data['Season'] = 'Fall'
            fall_data['Average Temperature'] = (fall_avg_val)
            res.append(fall_data)
        
        # clean up the arrays for next year's data
        winter_avg = []
        spring_avg = []
        fall_avg = []

    return str(res)
  

@app.route('/datapoints/<string:letter>')
def get_datapoints(letter):
    if len(letter) > 1 or letter.isalpha() == False:
        return "Bad Input"
    if letter.islower():
        letter = letter.toupper()
    print(letter)
    df = pd.read_csv('stations.csv')
    # get column with station id
    # for each station id: check if path exists in dataset
    # if path exists: read csv associated with that file
    # then for each year in that csv, calculate the counts for each year and each season
    df_filtered = df[df['id'].str.match(letter)]
    station_ids = df_filtered['id']
    print(station_ids.shape)
    file_counter = 0 
    missing_counter = 0
    bad_data = 0
    res = []
    for id in station_ids:
        # check if path exists
        file_path = '..\\Jobs\\'+id+'.csv'
        if path.exists(file_path):

            file_counter += 1
            current_df = pd.read_csv(file_path)
            # print(current_df.columns)
            if 'TAVG' not in current_df.columns:
                bad_data += 1
                # break
            else:
                current_df = current_df[current_df['TAVG'].notna()]
                current_df['Year'] = current_df['DATE'].str[:4]
                current_df['Month'] = current_df['DATE'].str[-2:]
                current_df['Season'] = current_df.apply(determine_season, axis=1)
                print(current_df)
                uniqiue_years = current_df['Year'].unique()
                res.append('{Station: ' + id + str(current_df.groupby(['Year', 'Season'])['STATION'].count().to_json(orient='columns')))
                
        else:
            missing_counter += 1
    
    print(file_counter)
    return json.dumps(res)

@app.route('/area/<string:lat1>/<string:lon1>/<string:lat2>/<string:lon2>/<int:startYear>/<int:endYear>')
def computearea(lat1,lon1,lat2,lon2,startYear,endYear):
    station_data = pd.read_csv('stations.csv')
    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)
    orient1 = False
    orient2 = False

    if lon1 < lon2 and lat1 < lat2:
        bottom_left = (lat1, lon1)
        top_right = (lat2, lon2)
        orient1 = True
    elif lon1 > lon2 and lat1 > lat2:
        top_right = (lat1, lon1)
        bottom_left = (lat2, lon2)
        orient1 = True
    elif lon1 < lon2 and lat1 > lat2:
        top_left = (lat1, lon1)
        bottom_right = (lat2, lon2)
        orient2 = True
    elif lon1 > lon2 and lat1 < lat2:
        top_left =  (lat2, lon2)
        bottom_right = (lat1, lon1)
        orient2 = True

    stations = []
    if orient1 == True:
        # get all the stations inside the rectangle:
        for idx, row in station_data.iterrows():
            # make sure it is within the boundary made by the two corner points
            if row['lat'] > bottom_left[0] and row['lat'] < top_right[0] and row['lon'] > bottom_left[1] and row['lon'] < top_right[1]:
                # if it is inside the boundary, append it to the list of stations 
                stations.append(row['id'])
    
    elif orient2 == True:
        # get all the stations inside the rectangle:
        for idx, row in station_data.iterrows():
            # make sure it is within the boundary made by the two corner points
            if row['lat'] < top_left[0] and row['lat'] > bottom_right[0] and row['lon'] < bottom_right[1] and row['lon'] > top_left[1]:
                stations.append(row['id'])
    averages = []
    # for stations in the rectangle area, compute the average temperature over
    # the period of startYear - endYear and then average out the values inside the
    # rectangle to get an average value over the area.
    for station in stations:
        
        current_df = pd.read_csv('..\\Jobs\\'+station+'.csv')
        current_df['Year'] = current_df['DATE'].str[:4]
        if 'TAVG' not in current_df.columns:
            print("Average temperature not missing")
        else:
            current_df = current_df[current_df['TAVG'].notna()]
            groupby_df = current_df.groupby('Year').mean().reset_index()
            groupby_df = groupby_df[groupby_df['TAVG'].notna()]
            groupby_df = groupby_df[(groupby_df['Year'] >= str(startYear)) & (groupby_df['Year'] <= str(endYear))]
            if len(groupby_df['TAVG']) >= 1:
                averages.append(mean(groupby_df['TAVG']))
            else:
                print("not enough data points")

    return str(mean(averages))

# bonus item - 1: get the country of specified station using geopy

@app.route('/country/<string:station>')
def get_country(station):
    df = pd.read_csv('stations.csv')
    # isolate the row with the id and then use iloc to access coordinates
    station_row = df[df['id'] == station]
    lat = station_row.iloc[0]['lat']
    lon = station_row.iloc[0]['lon']
    # use google's API to return information on the station and
    # access the returned json object to return the country
    locator = Nominatim(user_agent='google')
   
    location = locator.reverse(f'{lat}, {lon}')
    return str(location.raw['address']['country'])

# get outliers in terms of temperature, using standard deviation
@app.route('/outlier/<string:station>')
def get_outlier(station):
    current_df = pd.read_csv('..\\Jobs\\'+station+'.csv')
    current_df['Year'] = current_df['DATE'].str[:4]
    if 'TAVG' not in current_df.columns:
        return 'Temperature data not present'
    else:
        # group the data by years and calculate the average values for each year
        current_df = current_df[current_df['TAVG'].notna()]
        groupby_df = current_df.groupby('Year').mean().reset_index()
        anomalies = []
        average_temperatures = groupby_df['TAVG']
        # print(np.std(average_temperatures))
        data_std = groupby_df['TAVG'].std()
        data_mean = groupby_df['TAVG'].mean()
        # define the cut off point to be 2 standard deviations
        cut_off = data_std * 2

        lower_limit = data_mean - cut_off
        upper_limit = data_mean + cut_off

        # if any entry in the temperature column exceeds either limit,
        # append it to the anomaly array.
        for outlier in groupby_df['TAVG']:
            if outlier > upper_limit or outlier < lower_limit:
                anomalies.append(outlier)
        
        return json.dumps(anomalies)