#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 10:36:50 2019

@author: ryansalsbury
"""

import sqlite3
import pandas as pd
import numpy as np
import geopy
from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from geopy.distance import geodesic


#Create initial data
data = [['Location1', 38.98, -94.67, '5'], ['Location2', 38.88, -94.81,'2'], ['Location3', 52.09, -94.57, '1']]
  
#Convert data to pandas data frame
df = pd.DataFrame(data, columns = ['Location_Name', 'Lat', 'Long', 'Review']) 

#Create new database in sqlite connection
conn = sqlite3.connect("ReviewsDatabase.db")
#Delete table if already created
conn.execute("DROP TABLE IF EXISTS reviews;")
#Create reviews table
df.to_sql("reviews", conn, index=False)

#Create and configure application
app = Flask(__name__)
app.config["DEBUG"] = True

#Create function to return data from database as dictionaries rather than lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

#Create Home Page
@app.route('/', methods=['GET'])
def home():
    return '''<h1>Business Reviews API</h1>
<p>Search Database by lat and long coordinates</p>'''

#Create route to return all data from the reviews table
@app.route('/api/reviews/all', methods=['GET'])
def api_all():
    with sqlite3.connect("ReviewsDatabase.db") as conn:
        conn.row_factory = dict_factory
        cur = conn.cursor()
        return jsonify(cur.execute("select * from reviews").fetchall())

#Create function to return an error
@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404

#Create Route to allow users to enter latitude/longitude values and return all results within 50 miles of coordinates entered
@app.route('/api/reviews', methods=['GET'])
def api_params():
    
    #Create 2 paramateres - lat and long
    params = request.args
    lat = params.get('lat')
    long = params.get('long')
    
    #Create empty list to store difference in miles between parameters entered and database values
    distance = []
    
    #Calculate distance in miles if lat and long are both entered
    if (lat and long):
        with sqlite3.connect("ReviewsDatabase.db") as conn:
            for row in conn.execute("select * from reviews").fetchall():
                #Use distance function to calculate distance between
                #each row of coordinates(lat = row[1], long = row[2]) in reviews table and user input coordinates (lat and long)
                miles = geopy.distance.distance((row[1], row[2]), (lat, long)).mi
                #Append the miles variable to the distance list
                distance.append(miles)
            #Convert distance list to a pandas data frame
            dfdist = pd.DataFrame(distance, columns = ['Miles'])
            #Drop distance table if already created
            conn.execute("DROP TABLE IF EXISTS distance;")
            #Create new distance table in databse using the dfdist dataframe
            dfdist.to_sql("distance", conn, index=False)
            #Create query to only inlcude rows that are within 50 miles of user input coordinates
            query = "SELECT * FROM reviews r Join distance d on r.rowid = d.rowid WHERE d.Miles <= 50"

    #If lat and long are not both entered, return an error from the page not found function created earlier
    if not (lat and long):
        return page_not_found(404)
    #Connect to database
    conn = sqlite3.connect('ReviewsDatabase.db')
    #Set row_factory = dict_factory to return values as a dictionary - this works better with JSON output and will include column headers
    conn.row_factory = dict_factory
    #Create cursor object to pull data from database
    cur = conn.cursor()
    #Execute query and return all results
    results = cur.execute(query).fetchall()
    #Use jsonify function to return results in proper JSON format
    return jsonify(results)

#Create function to process user input values to post new data to the reviews table for each colum in the table
def add_review(location,lat,long,review):
    with sqlite3.connect("ReviewsDatabase.db") as conn:
        cur = conn.cursor()
        #Execute query to insert values entered in parameter into a new row in the reviews table
        cur.execute("INSERT INTO reviews (Location_Name,Lat,Long,Review) VALUES (?,?,?,?)", (location,lat,long,review))
        conn.commit()

#Create route to post new data from add_review function
@app.route('/api/reviews', methods=['POST'])
def new_review():
    #Store the new user input values in individual variables for each column in the table
    location = request.form['location']
    lat = request.form['lat']
    long = request.form['long']
    review = request.form['review']
    #use the variables above to use as input in the add_review function to send the data to the reviews table
    add_review(location,lat,long,review)
    #Return Done when data has been successfully sent to reviews table as a new row
    return("Done")

#run the application
app.run()
