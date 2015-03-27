#! /usr/local/bin/python3
'''



'''
import sys
from datetime import datetime
import pymongo
from pymongo import MongoClient
from csvFileClass import *
        
def string_to_dict(value_in):

    if  value_in  != '""':
        value_out  = value_in.strip('"')   
    else: 
        value_out = None
    return value_out

def number_to_dict(value_in):

    if  value_in  != '':
        value_out  = int(value_in)   
    else: 
        value_out = None
    return value_out

def date_to_dict(value_in):    
    if  value_in  != '':
        value_out  = datetime.strptime(value_in, '%d-%b-%y' )
    else: 
        value_out = None
    return value_out 
    
def convert_row_json2(header_row, line_in, column_type):
    
    row_in = line_in.strip().split(",")
    #print(row_in)
    row_in_num = 0
    line_dict = {}
    for header_num,field in enumerate(header_row):
        
        if column_type[ header_num ] in ['String']:
            line_dict[header_row[header_num]] = string_to_dict( row_in[ row_in_num ] )
            
        if column_type[ header_num ] in ['Blank']:
            line_dict[header_row[header_num]] = None
            
        if column_type[ header_num ] in ['Number']:
            line_dict[header_row[header_num]] = number_to_dict( row_in[ row_in_num ] ) 
            
        if column_type[ header_num ] in ['Date']:
            line_dict[header_row[header_num]] = date_to_dict( row_in[ row_in_num ] )   

        if column_type[ header_num ] in ['List']:
            curr_list = []
            if row_in[ row_in_num ][-1] == '"':
                curr_list.append(row_in[ row_in_num ].strip('"'))
                row_in_num += 1
            else:
                loop_count = 0
                loop_list = True
                curr_list.append(row_in[ row_in_num ].strip('"'))
                while loop_list:
                    loop_count += 1
                    row_in_num += 1
                    if row_in[ row_in_num ][-1] != '"':
                        curr_list.append(row_in[ row_in_num ].strip('"'))
                    else:
                        curr_list.append(row_in[ row_in_num ].strip('"'))
                        loop_list = False
                        row_in_num += 1
            line_dict[header_row[header_num]] = curr_list
        else:
            row_in_num += 1
            
    return line_dict


def import_csv_file( program_mode, csvFile, mongo_collection, row_limit ):
    
    filename_csv = csvFile.getCsvFileName() 
    header_row = csvFile.getHeader() 
    column_type_in = csvFile.getColumnTypes()

    row_num = 0
    error_count = 0
    process_rows = row_limit

    with open(filename_csv, 'rt') as f:  # open the csv file in order to read it
        for row in f:
            row_num += 1

            if row_num == 1: # this is the header row
                continue  # do not process the header
            


            if row_num % 50000 == 0:  # print a status message every 50000 records
                curr_time = datetime.now()
                print('Rows ' + str(row_num) + ' current time is ' +  curr_time.strftime('%H:%M:%S')   )
                elapsed_time = curr_time - start_time
                print('Program elapsed time is ' +  str(elapsed_time  )  )

            if row_num < 5:  # not used
                pass
                #print(row_dict)

            if row_num == process_rows:
                break            

            try:
                row_dict = convert_row_json2(header_row, row, column_type_in)

                if program_mode == "Verify":
                    pass
                elif program_mode == "Insert":
                    mongo_collection.insert(row_dict)
                elif program_mode == "Update":
                    mongo_collection.update({'_id': row_dict['_id']},row_dict) 
                else:
                    pass

            except:
                error_count += 1
                if error_count < 5:
                    next_row = row
                    print("Row_num " + str(row_num) + " Error:" , sys.exc_info()[0])
                    print(row)

            if row_num == 2:  
                sample_dict1 = row_dict  # store sample_row
            elif row_num == 3:
                sample_dict2 = row_dict # store sample_row


    print(' ')
    print('Total number of rows processed ' + str(row_num)  )
    print('Total number of bad rows/errors ' + str(error_count)  )

def report_time(message_in):
    start_time = datetime.now()
    print(message_in + ' ' +  start_time.strftime('%H:%M:%S')   )
    return start_time

def report_elapsed_time(message_in, start_time, end_time ):
    elapsed_time = end_time - start_time
    print(message_in + ' ' +  str(elapsed_time  )  )
    
def get_mongo_collection( db_name, collection_name):
    client = MongoClient()
    mongo_database = client[db_name]
    mongo_collection = mongo_database[collection_name]
    return mongo_collection

    
def run_etl_request(source_type, program_mode,\
                    csvFile,  row_limit,\
                    db_name, collection_name):
    '''
    Purpose: 
    '''
    #log process start time
    print('')
    start_time = report_time('Program start time is ')
    
    # what is the source
    source_type = 'Csv_File'
    source_description = 'CSV File'
    print('Source type is {}. CSV file name is {}'.format\
          (source_description, csvFile.getCsvFileName() ) )
    
    # what is the target
    print('Target is MongoDB.  Mongo database name is {}, collection name is {}'.\
          format( db_name, collection_name ))


    # three program modes are Verify, Insert, Update
    print('Program run mode is {}'.format(program_mode))


    # Row limit is set to
    print('Row limit is set to {}'.format(row_limit))
    

    #get name of MongoDB collection
    mongo_collection = get_mongo_collection( db_name, collection_name)

    row_limit_int = int(row_limit)
    #this is where data is actually read from CSV and loaded into Mongo
    import_csv_file( program_mode, csvFile, mongo_collection, row_limit_int )
    
    print('')    
    end_time = report_time('Program end time is ')
    report_elapsed_time('Program elapsed time is ', start_time, end_time )


if __name__ == '__main__':
    '''db_name = sys.argv[1]
    collection_name = sys.argv[2]
    csvFile = sys.argv[3]
    program_mode = sys.argv[4]
    row_limit = sys.argv[5]'''

    program_mode = sys.argv[1]  # Verify, Insert, Update
    source_name = sys.argv[2]  # Csv_File,  Oracle
    csvFile = sys.argv[3]
    row_limit = sys.argv[4]
    db_name = sys.argv[5]
    collection_name = sys.argv[6]

    
    myCsv1 = CsvFileStructure( csvFile )
    run_mongo_etl(db_name, collection_name, myCsv1, program_mode, row_limit)
    
