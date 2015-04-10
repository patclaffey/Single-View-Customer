from django.shortcuts import render


from django.http import HttpResponse

import pymongo
from pymongo import MongoClient
from collections import OrderedDict
import json
from datetime import datetime
from v1.svc_util import SvcCountry
   

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


def index(request):
    return HttpResponse("You're at the index.")


def svc_country_profile(request, country_code, profile_id):
    client = MongoClient()

    country_data = SvcCountry(country_code)
    db_name = country_data.get_db_name()
    db_col_name = country_data.get_db_collection_name()

    #find_one takes 3 parameters, each of which is a json document
    # first parameter is the query parameter - return a profile id that matches input
    # we need a second parameter the project. It says exclude the column test
    #    This column does not exist which means we actually include all columns
    #    The third column returns the columns in the same order as they exist on Mongo 
    db_profile = client[db_name][db_col_name].find_one({"PROFILE_ID":int(profile_id)}, {'test':0}, as_class=OrderedDict) 

    if db_profile is not None:
        data_out = OrderedDict(db_profile)
        #data_out = db_profile
        output = json.dumps(  data_out, default=date_handler )
    else:
        data_out = {}
        output = {"PROFILE_ID":profile_id, "db_name":db_name, "col_name": db_col_name}

    return HttpResponse( str(output) )


def svc_country_msisdn(request, country_code, msisdn_id):
    client = MongoClient()

    country_data = SvcCountry(country_code)
    db_name = country_data.get_db_name()
    db_col_name = country_data.get_db_collection_name()

    db_msisdn = client[db_name][db_col_name].find_one({"_id":msisdn_id}, {'test':0}, as_class=OrderedDict)

    if db_msisdn is not None:
        data_out = OrderedDict(db_msisdn)
        output = json.dumps(  data_out, default=date_handler )
    else:
        data_out = {}
        output = {}

    return HttpResponse( str(output) )


def svc_country(request, country_code):
    client = MongoClient()

    country_data = SvcCountry(country_code)
    db_name = country_data.get_db_name()
    db_col_name = country_data.get_db_collection_name()

    if db_col_name is not None:
        db_grand_total = client[db_name][db_col_name].count()
    else:
        db_grand_total = None

    data_out = OrderedDict()
    data_out['country_code'] = country_code
    data_out['database'] =  db_name
    data_out['collection'] =  db_col_name 
    data_out['count'] = db_grand_total
    
    output = json.dumps(  data_out)
    return HttpResponse( str(output) )
