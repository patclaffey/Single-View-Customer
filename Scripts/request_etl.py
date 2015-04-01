#! /usr/local/bin/python3
'''



'''
import sys
from datetime import datetime
import pymongo
from pymongo import MongoClient
#import csvFileClass
import cx_Oracle
#import oracleClass
import oracle_etl
import mongo_etl2
import csv_file_etl
        


def report_time(message_in):
    start_time = datetime.now()
    print(message_in + ' ' +  start_time.strftime('%H:%M:%S')   )
    return start_time

def report_elapsed_time(message_in, start_time, end_time ):
    elapsed_time = end_time - start_time
    print(message_in + ' ' +  str(elapsed_time  )  )
    




def run_etl_request(source_type, program_mode,\
                    source_schema, source_object_name,  row_limit,\
                    db_name, collection_name):
    '''
    Purpose: 
    '''
    #log process start time
    print('')
    start_time = report_time('Program start time is ')
    
    # what is the source
    if source_type == 'Csv_File':
        source_description = 'CSV File'
        mySourceObj1 = csv_file_etl.CsvFileStructure( source_object_name )
        print('Source type is {}. Source csv file name is {}'.format\
          (source_description, source_object_name ) )
    elif source_type == 'Oracle':
        source_description = 'Oracle Database'
        oracle_connection = oracle_etl.get_oracle_connection()
        mySourceObj1 = oracle_etl.OracleTable( oracle_connection, source_schema, source_object_name )
        print('Source type is {}. Source table name is {}'.format\
          (source_description, source_object_name ) )
    else:
        source_description = 'Undefined Source'
        mySourceObj1 = None
        



    # three program modes are Verify, Insert, Update
    print('Program run mode is {}'.format(program_mode))

    
    # what is the target
    if program_mode == 'Verify':
        mongo_collection = None
        print('Testing data source only - this does not impact target Mongo database' )
    elif program_mode in ('Update','Insert'):
        print('Target is MongoDB.  Mongo database name is {}, collection name is {}'.\
          format( db_name, collection_name ))
        #get name of MongoDB collection
        mongo_collection = mongo_etl2.get_mongo_collection( db_name, collection_name)
    else:
        print('Target is unknown.')


    # Row limit is set to
    print('Row limit is set to {}'.format(row_limit))

    row_limit_int = int(row_limit)
    #this is where data is actually read from CSV and loaded into Mongo

    if source_type == 'Csv_File' and program_mode == 'Verify':
        csv_file_etl.import_csv_file( program_mode, mySourceObj1 , mongo_collection, row_limit_int )
    elif source_type == 'Csv_File' and (program_mode in ('Update','Insert') ):
        if mongo_collection == None:
            print("**************")
            print("MongoDB is down.  Error, cannot perform ETL as target is not available")
            print("**************")
        else:
            csv_file_etl.import_csv_file( program_mode, mySourceObj1, mongo_collection, row_limit_int )


    if source_type == 'Oracle' and mySourceObj1 == None:
        print("**************")
        print("Oracle is down.  Error, cannot perform ETL as Oracle is not available")
        print("**************")        
    elif source_type == 'Oracle' and program_mode == 'Verify':
        oracle_etl.import_oracle( program_mode, mySourceObj1 , mongo_collection, row_limit_int )
    elif source_type == 'Oracle' and (program_mode in ('Update','Insert') ):
        if mongo_collection == None:
            print("**************")
            print("MongoDB is down.  Error, cannot perform ETL as target is not available")
            print("**************")
        else:
            oracle_etl.import_oracle( program_mode, mySourceObj1, mongo_collection, row_limit_int )
    
    print('')    
    end_time = report_time('Program end time is ')
    report_elapsed_time('Program elapsed time is ', start_time, end_time )


if __name__ == '__main__':

    source_name = sys.argv[1]  # Csv_File,  Oracle
    program_mode = sys.argv[2]  # Verify, Insert, Update
    schema_name = sys.argv[3] 
    data_store_name = sys.argv[4]
    row_limit = sys.argv[5]
    db_name = sys.argv[6]
    collection_name = sys.argv[7]

    run_etl_request(source_name,\
                    program_mode,\
                    schema_name,\
                    data_store_name,\
                    row_limit,\
                    db_name,\
                    collection_name
                    )


    
