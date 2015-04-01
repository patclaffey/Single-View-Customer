#! /usr/local/bin/python3
'''



'''
import sys
from datetime import datetime
import pymongo
from pymongo import MongoClient
import csvFileClass
import cx_Oracle
import oracleClass
        
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
    
def convert_row_json2(header_row, row_in, column_type):
    
    #row_in = line_in.strip().split(",")
    print(header_row)
    print(column_type)
    print(row_in)
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


def convert_row_json3(header_row, row_in, column_type):
    
    #row_in = line_in.strip().split(",")
    #print(header_row)
    #print(column_type)
    #print(row_in)
    row_in_num = 0
    line_dict = {}
    for header_num,field in enumerate(header_row):
        line_dict[header_row[header_num]] = row_in[ header_num ] 
    #print(line_dict)        
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
            
            row_list = row.strip().split(",")

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
                row_dict = convert_row_json2(header_row, row_list, column_type_in)

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
    mongo_collection = None
    try:
        client = MongoClient()
        mongo_database = client[db_name]
        mongo_collection = mongo_database[collection_name]
    except:
        pass
    return mongo_collection

def import_oracle( program_mode, tableObj, mongo_collection, row_limit ):
    start_time = datetime.now()
    oracle_connection = tableObj.getOracleConnection()
    oracle_schema = tableObj.getSchemaName() 
    table_name = tableObj.getTableName() 
    header_row = tableObj.getColumnNames() 
    column_type_in = tableObj.getColumnTypes()

    row_num = 0
    error_count = 0
    process_rows = row_limit

    sql_command = "select * from " + oracle_schema + "." + table_name
    print( sql_command )

    cur = oracle_connection.cursor()
    cur.execute( sql_command )

    for row in cur:
        row_num += 1
        
        if row_num == process_rows:
            break

        if row_num % 50000 == 0:  # print a status message every 50000 records
            curr_time = datetime.now()
            print('Rows ' + str(row_num) + ' current time is ' +  curr_time.strftime('%H:%M:%S')   )
            elapsed_time = curr_time - start_time
            print('Program elapsed time is ' +  str(elapsed_time  )  )

        try:
            row_dict = convert_row_json3(header_row, row, column_type_in)

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
        
        #row_dict = convert_row_json3(header_row, row, column_type_in)
        #print(row_dict)
    cur.close()  

    print(' ')
    print('Total number of rows processed ' + str(row_num)  )
    print('Total number of bad rows/errors ' + str(error_count)  )

def get_oracle_connection():
    '''
    Name: openConnection
    Purpose: Open Connection to Oracle Database
              Get credentials - username, password
              Get source_name
              Open Connection
    '''
    oracle_connection= None
    try:

        oracle_connection = cx_Oracle.connect('pclaffey[BT_DW_ODS]/Seafield89@DWHDF07')
        #self.user_name = self.Connection.username
        #self.tns_entry = self.Connection.tnsentry
    except:
        pass
    return oracle_connection

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
        mySourceObj1 = csvFileClass.CsvFileStructure( source_object_name )
        print('Source type is {}. Source csv file name is {}'.format\
          (source_description, source_object_name ) )
    elif source_type == 'Oracle':
        source_description = 'Oracle Database'
        oracle_connection = get_oracle_connection()
        mySourceObj1 = oracleClass.OracleTable( oracle_connection, source_schema, source_object_name )
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
        mongo_collection = get_mongo_collection( db_name, collection_name)
    else:
        print('Target is unknown.')


    # Row limit is set to
    print('Row limit is set to {}'.format(row_limit))

    row_limit_int = int(row_limit)
    #this is where data is actually read from CSV and loaded into Mongo

    if source_type == 'Csv_File' and program_mode == 'Verify':
        import_csv_file( program_mode, mySourceObj1 , mongo_collection, row_limit_int )
    elif source_type == 'Csv_File' and (program_mode in ('Update','Insert') ):
        if mongo_collection == None:
            print("**************")
            print("MongoDB is down.  Error, cannot perform ETL as target is not available")
            print("**************")
        else:
            import_csv_file( program_mode, mySourceObj1, mongo_collection, row_limit_int )


    if source_type == 'Oracle' and mySourceObj1 == None:
            print("**************")
            print("Oracle is down.  Error, cannot perform ETL as Oracle is not available")
            print("**************")        
    elif source_type == 'Oracle' and program_mode == 'Verify':
        import_oracle( program_mode, mySourceObj1 , mongo_collection, row_limit_int )
    elif source_type == 'Oracle' and (program_mode in ('Update','Insert') ):
        if mongo_collection == None:
            print("**************")
            print("MongoDB is down.  Error, cannot perform ETL as target is not available")
            print("**************")
        else:
            import_oracle( program_mode, mySourceObj1, mongo_collection, row_limit_int )
    
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
    
