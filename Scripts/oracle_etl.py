'''
Purpose:    Module to hold all code to support Oracle as a data source
            for BT python ETL
            It contains the following components
            -  get_oracle_connection - opens a connection to Oracle
            -  OracleTable is a class that holds the structure of a specific table
            -  convert_row_json - converts a oracle row to json
            -  import_json - inports an Oracle row into Mongo
Input:      **** configuration file required   ***
Output:     
Exceptions: None

'''
import sys
from datetime import datetime
import cx_Oracle
import collections


def get_oracle_connection():
    '''
    Name: openConnection
    Purpose: Open Connection to Oracle Database
              Get credentials - username, password
              Get tns_entry
    '''
    oracle_connection= None
    
    try:
        oracle_connection = cx_Oracle.connect('pclaffey[BT_DW_ODS]/Seafield89@DWHDF07')
    except:
        pass
    
    return oracle_connection


class OracleTable(object):
    '''
    Purpose:  Creates class to represent Oracle table structure
    Input: Oracle Connection
           Name of source schema
           Name of table
    Output: see description for class methods
    '''


    def __init__(self, oracle_connection, source_schema, table_in):
        '''
        Purpose:  Initalize class
                  Get structure of Oracle table from all_tab_columns
                  Populates a python list with table column names
                  Map and set the _id column name as required by MongoDB
                  Automatically determine type of each table column
        Input: Name of table.  Additional info from config file
        Output: Object of this class
         '''       
        self.Connection = oracle_connection
        self.schema_name = source_schema
        self.tablename = table_in
        self.user_name = ""
        self.tns_entry = ""
        self.key_field_name = 'MSISDN'  # Mongo DB _id field
        self.header_list = []  # Python list object with column names
        self.column_type = []  # Python list object with column types
        self.row_count = 0
        self.status = False

        #populate self.header_list with list of column names for this table
        self.createHeaderList()

        #look for field named in self.key_field_name and update to _id
        #this is required by Mongo to identify primary key
        self.setMongoId()

        #populate self.column_type with list of column types for this table
        #The available types are: List, Number, String, Date
        self.createTypeList()

        self.status = True

            
    def getStatus(self):
        '''
        Purpose: Get status of object initialization
        Input:  None
        Output: True if object initialization successful otherwise false
        '''    
        return self.status


    def getOracleConnection(self):
        '''
        Purpose: Create Oracle database connection
        Input:  None
        Output: Oracle database connection
        '''   
        return self.Connection


    def getSchemaName(self):
        '''
        Purpose: Get Oracle table schema name
        Input:  None
        Output: Oracle table schema name
        '''   
        return self.schema_name


    def getTableName(self):
        '''
        Purpose: Get Oracle table name
        Input:  None
        Output: Oracle table name
        '''   
        return self.tablename
    
    
    def getUserName(self):
        '''
        Purpose: Get Oracle user name
        Input:  None
        Output: Oracle user name
        '''     
        return self.user_name


    def getTnsEntry(self):
        '''
        Purpose: Get Oracle tns entry 
        Input:  None
        Output: Oracle tns entry
        '''   
        return self.tns_entry

    
    def getColumnNames(self):
        return self.header_list


    def getColumnTypes(self):
        '''
        Purpose: Get table column types
        Input:  None
        Output: List object containing table column formats e.g. Date, String..
        '''
        return self.column_type

    
    def getNumberColumns(self):
        '''
        Purpose: Get number of columns in table
        Input:  None
        Output: table column count
        '''     
        return len( self.getColumnNames() )


    def printColumnReport(self):
        '''
        Purpose: Report on column fields and types
        Input:  None
        Output: Report on column fields and types
        '''          
        self.next_row_seq = self.getSampleRow1().strip().split(",")
        self.header_row = self.getHeader()
        self.column_type = self.getColumnTypes()
        for self.num,self.field in enumerate(self.header_row):
            print('Field number ' + str(self.num) + ' is ' + self.header_row[self.num]
                                    + ' ' +  self.column_type[ self.num ] 
                      + ' csv ' +  repr(self.next_row_seq[ self.num ] )  + '  ' )
            

    def openConnection (self):
        '''
        Name: openConnection
        Purpose: Open Connection to Oracle Database
              Get credentials - username, password
              Get source_name
              Open Connection
        '''
        self.Connection = cx_Oracle.connect('pclaffey[BT_DW_ODS]/Seafield89@DWHDF07')
        self.user_name = self.Connection.username
        self.tns_entry = self.Connection.tnsentry
    

                    
    def createHeaderList(self):
        '''
        Purpose: Get the column names from Oracle database in order to create JSON keys
        Input:  column names from all_tab_columns
        Output: List of strings, each string is a column name
        '''

        self.cur = self.Connection.cursor()
        self.cur.execute("select column_name from all_tab_columns  \
        where table_name = upper('" + self.tablename + "') \
        order by column_id")
        for self.result in self.cur:
            self.header_list.append(self.result[0])
        self.cur.close()  

    def setMongoId(self):
        '''
        Purpose: Add _id key field to python header list object
                The _id field is required by Mongo to identify primary key
        Input:  Name of column that acts as Mongo key  ( self.key_field_name )
                Header record as python list object
        Output: Header record as python list with _id Mongo primary key identifier
        '''
        
        self.procedure_status = False

        if '_id' in self.header_list:
            pass
        elif self.key_field_name in self.header_list:
            self.field_index = self.header_list.index( self.key_field_name )
            self.header_list[ self.field_index ] = '_id'
            self.procedure_status = True
        else:
            self.procedure_status = False


    def createTypeList(self):
        '''
        Purpose:  Automatically determine type for each column based
                  on a representative sample data row from CSV file
        Input:  A single string that is a sample row from CSV file
        Output:  A ptyhon list object holding classification of each columns
                The column types are one of the following:
                List - column contains a list
                Number - column is nummeric
                Date - column is a date
                String - column is a string
        '''

        self.oracle_column_type = []
        self.cur = self.Connection.cursor()
        self.cur.execute("select data_type from all_tab_columns  \
        where table_name = upper('" + self.tablename + "') \
        order by column_id")
        for self.result in self.cur:
            self.oracle_column_type.append(self.result[0])
        self.cur.close()
        
        self.column_type = []

        for self.num,self.field in enumerate(self.oracle_column_type):

            if self.header_list[ self.num ][-5:] == "_LIST":
                self.column_type.append('List')
                
            elif self.field == 'NUMBER':
                self.column_type.append('Number')

            elif self.field == 'DATE':
                self.column_type.append('Date')

            else:
                self.column_type.append('String')


def convert_row_json(header_row, row_in, column_type):
    '''
    Purpose: Convert single Oracle data record to JSON format
             Key value is column name, value is row column value
    Input:  header_row - list of table column names
            row_in - data record in
            column_type - type of each column
    Output: table column count
    '''     
    row_in_num = 0
    line_dict = collections.OrderedDict()
    
    for column_number,column_name in enumerate(header_row):
        #column_number - integer that is the index number of the column
        #column_name - column name at a given index
        if column_type[ column_number ] == 'List':
            #column_type[ column_number ] - holds column type e.g. String, Number etc
            #line_dict is dictionary, key is column name, value is column value
            if row_in[ column_number ] is None:
                # process empty list
                # if a list is empty then insert an empty list into JSON which is [ ]
                line_dict[header_row[column_number]] = list()
            else:
                process non empty list
                #otherwise convert string to list breaking on comma
                line_dict[header_row[column_number]] = \
                    str(row_in[ column_number ]).split(',')
        else:
            # this is logic for columns that are not lists
            # python automatically handles strings, numbers, dates, nulls
            line_dict[header_row[column_number]] = row_in[ column_number ]        
    return line_dict


def import_oracle( program_mode, tableObj, mongo_collection, row_limit ):
    '''
    Purpose: process that reads data from Oracle and imports into Mongo
    Input:   program mode
                   Verify - verify that input is read correctly
                   Insert - insert data into mongo
                   Update - update data in mongo
             tableObj - object representing table structure
             mongo_collection - Mongo collection object
             row_limit - limit number of rows
    Output:   in all cases a short report is printed on screen
              in case of Insert and Update modes state of Mongo database is changed
    '''  
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
            row_dict = convert_row_json(header_row, row, column_type_in)

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

        
