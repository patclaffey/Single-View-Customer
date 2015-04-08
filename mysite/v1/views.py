from django.shortcuts import render



from django.http import HttpResponse

import pymongo
from pymongo import MongoClient
from collections import OrderedDict
import json


def index(request):
    return HttpResponse("Hello, world. You're at the polls index.")

def svc_za(request):
    return HttpResponse("Hello, this is South Africa.")

def svc_country(request, country_code):
    client = MongoClient()
    dbs = client.database_names()
    dbs_values = ' '.join(dbs) 
    data_out = OrderedDict()
    data_out['country_code'] = country_code
    data_out['database'] = country_code.upper() 
    data_out['collection'] = 'DW_SVC_' + country_code.upper() 
    data_out['count'] = None
    

    output = json.dumps(  data_out)
    #return HttpResponse("Hello %s, this is country code <b>%s</b>" % (dbs_values , country_code) )
    return HttpResponse( str(output) )