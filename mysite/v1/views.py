from django.shortcuts import render



from django.http import HttpResponse

import pymongo
from pymongo import MongoClient
from collections import OrderedDict
import json
from datetime import datetime


def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

def index(request):
    return HttpResponse("You're at the index.")


def svc_country_profile(request, country_code, profile_id):
    client = MongoClient()

    db_name = None
    db_col_name = None

    if country_code.upper() == 'ID':
        db_name = 'ID'
        db_col_name = 'BT_SVC_ID'
    elif country_code.upper() == 'ZA':
        db_name = 'ZA'
        db_col_name = 'BT_SVC_ZA'


    db_profile = client[db_name][db_col_name].find_one({"PROFILE_ID":int(profile_id)})

    if db_profile is not None:
        data_out = OrderedDict(db_profile)
        output = json.dumps(  data_out, default=date_handler )
    else:
        data_out = {}
        output = {"PROFILE_ID":profile_id}

    return HttpResponse( str(output) )


def svc_country_msisdn(request, country_code, msisdn_id):
    client = MongoClient()

    db_name = None
    db_col_name = None

    if country_code.upper() == 'ID':
        db_name = 'ID'
        db_col_name = 'BT_SVC_ID'

    elif country_code.upper() == 'ZA':
        db_name = 'ZA'
        db_col_name = 'BT_SVC_ZA'


    db_msisdn = client[db_name][db_col_name].find_one({"_id":msisdn_id})

    if db_msisdn is not None:
        data_out = OrderedDict(db_msisdn)
        output = json.dumps(  data_out, default=date_handler )
    else:
        data_out = {}
        output = {}

    return HttpResponse( str(output) )

def svc_country(request, country_code):
    client = MongoClient()

    if country_code.upper() == 'ID':
        db_name = 'ID'
        db_col_name = 'BT_SVC_ID'
        db_grand_total = str(client[db_name][db_col_name].count())
    elif country_code.upper() == 'ZA':
        db_name = 'ZA'
        db_col_name = 'BT_SVC_ZA'
        db_grand_total = str(client[db_name][db_col_name].count())
    else :
        db_name = None
        db_col_name = None
        db_grand_total = None

    dbs = client.database_names()
    dbs_values = ' '.join(dbs) 
    data_out = OrderedDict()
    data_out['country_code'] = country_code
    data_out['database'] =  db_name
    data_out['collection'] =  db_col_name 
    data_out['count'] = db_grand_total
    

    output = json.dumps(  data_out)
    return HttpResponse( str(output) )