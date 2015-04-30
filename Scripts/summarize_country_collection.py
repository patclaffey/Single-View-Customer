#! /usr/local/bin/python3
'''
Purpose: Create json summary document for a given country
         It determines the distinct values and associated counts for the following fields
                 LAST_REGION
                 MOST_RECENT_GENDER
                 MOST_RECENT_PERMISSION
                 MOST_RECENT_BRAND
Output:  Saves data in json format to Mongo database
         Data is stored in collection SVC_SUMMARY for given country
Input:   Country Code - currently ZA or ID
'''
import pymongo
from pymongo import MongoClient, Connection
from datetime import datetime
import time
from collections import Counter
import sys

def country_summary( country_code_in ):

    country_code = country_code_in
    
    #open database connection
    client = MongoClient()
    connection = Connection()

    #define Mongo database and collection
    if country_code == 'ZA':
        db = client['ZA_V2']
        col = db['DW_SVC_ZA']
    elif country_code == 'ID':
        db = client['ID2']
        col = db['DW_SVC_ID']
        
    print ('Number of {0} documents in Mongo database {1} is {2:,}'\
              .format( country_code, db._Database__name, col.count() ) \
            )

    start_time = time.clock()

    #define the Counter objects, one per item
    LAST_REGION_COUNT2 = Counter()
    MOST_RECENT_GENDER_COUNT2 = Counter()
    MOST_RECENT_PERMISSION2 =  Counter()
    MOST_RECENT_BRAND2 =  Counter()

    #go to Mongo and get the distinct column values and their counts
    for doc in col.find():
        LAST_REGION_COUNT2[doc['LAST_REGION']] += 1
        MOST_RECENT_GENDER_COUNT2[doc['MOST_RECENT_GENDER']] += 1
        MOST_RECENT_PERMISSION2[doc['MOST_RECENT_PERMISSION']] += 1
        MOST_RECENT_BRAND2[doc['MOST_RECENT_BRAND']] += 1
        
    end_time = time.clock()
    print ("Time taken is {} seconds".format( round(end_time - start_time, 1) ) )


    LAST_REGION_DB = {}

    #Problem with regions is that some region names contain the dot ( . ) character
    # Mongo does not permit the dot character in Mongo keys
    for key in dict(LAST_REGION_COUNT2):
        #hack for Python None, this should be set to Mongo null
        if key is None:
            key_new = 'None'
        else:
            key_new = key.replace('.',' ')
        
        LAST_REGION_DB[key_new] = LAST_REGION_COUNT2[key]

    #hack for Python None, this should be set to Mongo null    
    if None in MOST_RECENT_GENDER_COUNT2:
        MOST_RECENT_GENDER_COUNT2['None'] = MOST_RECENT_GENDER_COUNT2[None]
        del MOST_RECENT_GENDER_COUNT2[None]

    #hack for Python None, this should be set to Mongo null          
    if None in MOST_RECENT_PERMISSION2:
        MOST_RECENT_PERMISSION2['None'] = MOST_RECENT_PERMISSION2[None]
        del MOST_RECENT_PERMISSION2[None]

    #hack for Python None, this should be set to Mongo null          
    if None in MOST_RECENT_BRAND2:
        MOST_RECENT_BRAND2['None'] = MOST_RECENT_BRAND2[None]
        del MOST_RECENT_BRAND2[None]

    #load the data into Mongo
    db['SVC_SUMMARY'].update( {"_id":country_code}, 
                              {"$set": {
                                  "LAST_REGION": LAST_REGION_DB, 
                                  "MOST_RECENT_GENDER": dict(MOST_RECENT_GENDER_COUNT2),
                                  "MOST_RECENT_PERMISSION": dict(MOST_RECENT_PERMISSION2),
                                  "MOST_RECENT_BRAND": dict(MOST_RECENT_BRAND2),
                                  }
                               },
                             upsert=True
                             )

if __name__ == '__main__':
    country_summary(sys.argv[1])

