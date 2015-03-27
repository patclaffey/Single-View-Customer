'''
Purpose:    Define class to represent structure of Oracle table so that it can
               subsequently be loaded into mongo db
Input:      Name of Oracle Table.
            Need to define configuration file
Output:     Class representing the Oracle table structure 
Exceptions: None

'''

import sys
from datetime import datetime
import cx_Oracle


class OracleTable(object):
    '''
    Purpose:  Creates class to represent Oracle table structure
    Input: Oracle table name
    Output: see description for class methods
    '''

    def __init__(self, table_in):
        '''
        Purpose:  Initalize class
                  Get structure of Oracle table from all_tab_columns
                  Populates a python list with table column names
                  Map and set the _id column name as required by MongoDB
                  Automatically determine type of each table column
        Input: Name of table.  Additional info from config file
        Output: Object of this class
         '''       
        self.Connection = None
        self.tablename = table_in
        self.user_name = ""
        self.tns_entry = ""
        self.key_field_name = 'MSISDN'  # Mongo DB _id field
        self.header_list = []  # Python object holding header list

        self.row_count = 0
        self.column_type = []
        self.status = False

        #Method that opens connection, does checks based on file content and structure
        self.openConnection()

        #create python list object to hold csv header row
        self.createHeaderList()

        #look for field named in self.key_field_name and update to _id
        #this is required by Mongo to identify primary key
        self.setMongoId()

        #automatically determine type for each Oracle table columm
        #The available types are: List, Number, String, Date
        self.createTypeList()


        self.status = True

            
    def getStatus(self):
        return self.status

    
    def getUserName(self):
        return self.user_name


    def getTnsEntry(self):
        return self.tns_entry

    
    def getColumnNames(self):
        return self.header_list


    def getColumnTypes(self):
        return self.column_type
    
    def getNumberColumns(self):
        return len( self.getColumnNames() )


    def printColumnReport(self):
        
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
                Blank - column is blank
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

    

        
