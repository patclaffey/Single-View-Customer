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
    
def run_mongo_etl(db_name, collection_name, csvFile, program_mode, row_limit):
    '''
    Purpose: 
    '''
    # three program modes are Verify, Insert, Update
    print(csvFile.getCsvFileName() )
    row_num = 0
    error_count = 0
    process_rows = int(row_limit)
    filename_csv = ""
    start_time = datetime.now()
    
    client = MongoClient()
    mongo_database = client[db_name]
    mongo_collection = mongo_database[collection_name]


    filename_csv = csvFile.getCsvFileName() 
    header_row = csvFile.getHeader() 
    column_type_in = csvFile.getColumnTypes()


    start_time = datetime.now()
    print('Program start time is ' +  start_time.strftime('%H:%M:%S')   )
    print(' ')

    with open(filename_csv, 'rt') as f:
        for row in f:

            row_num += 1

            if row_num == 1: # header row, skip
                continue

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
                sample_dict1 = row_dict

            if row_num == 3:
                sample_dict2 = row_dict 

            if row_num % 50000 == 0:
                curr_time = datetime.now()
                print('Rows ' + str(row_num) + ' current time is ' +  curr_time.strftime('%H:%M:%S')   )
                elapsed_time = curr_time - start_time
                print('Program elapsed time is ' +  str(elapsed_time  )  )


            if row_num < 5:
                pass
                #print(row_dict)

            if row_num == process_rows:
                break

    end_time = datetime.now()
    print(' ')
    print('Program end time is ' +  end_time.strftime('%H:%M:%S')   )
    elapsed_time = end_time - start_time
    print('Program elapsed time is ' +  str(elapsed_time  )  )
    print(' ')
    print('Total row numbers ' + str(row_num)  )
    print('Total errors ' + str(error_count)  )


if __name__ == '__main__':
    db_name = sys.argv[1]
    collection_name = sys.argv[2]
    csvFile = sys.argv[3]
    program_mode = sys.argv[4]
    row_limit = sys.argv[5]
    
    myCsv1 = CsvFileStructure( csvFile )
    run_mongo_etl(db_name, collection_name, myCsv1, program_mode, row_limit)
    
