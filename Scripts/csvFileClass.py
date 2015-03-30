'''
Purpose:    Define class to represent structure of CVS file so that it can
               subsequently be loaded into mongo db
Input:      Name of CSV File.
            Need to define input directory for file
Output:     Class representing the CSV file structure 
Exceptions: None

'''
import sys
from datetime import datetime


class CsvFileStructure(object):
    '''
    Purpose:  Creates class to represent the structure and format of a CSV file
    Input:  CSV file name
    Output: see description for class methods
    '''

    def __init__(self, file_in):
        '''
        Purpose:  Initalize class
                  Opens and reads CSV file
                  Populates a python list with csv file column names
                  Map and set the _id column name as required by MongoDB
                  Automatically determine yype of each CSV column
        Input: Name of CSV file.  File should be stored in project import directory
        Output: Object of this class
         '''       
        self.csv_filename = file_in
        self.key_field_name = 'MSISDN'  # Mongo DB field name
        self.header_list = []  # Python object holding header list
        self.csv_header_row = ""
        self.sample_row1 = ""
        self.sample_row2 = ""
        self.row_count = 0
        self.column_type = []
        self.status = False

        #Method that opens file, does checks based on file content and structure
        self.openCSV()

        #create python list object to hold csv header row
        self.createHeaderList()

        #look for field named in self.key_field_name and update to _id
        #this is required by Mongo to identify primary key
        self.setMongoId()

        #automatically determine type for each CSV file columm
        #The available types are: List, Number, String, Date, Blank
        self.mapCsvColumns(self.sample_row1)

        self.status = True




            
    def getStatus(self):
        '''
        Purpose: Get status of object initialization
        Input:  None
        Output: True if object initialization successful otherwise false
        '''        
        return self.status

    
    def getCsvFileName(self):
        '''
        Purpose: Get CSV File Name
        Input:  None
        Output: CSV File Name
        '''        
        return self.csv_filename

    
    def getHeader(self):
        '''
        Purpose: Get CSV File header record
        Input:  None
        Output: List object containing csv file column names
        '''      
        return self.header_list

    
    def getSampleRow1(self):
        '''
        Purpose: Get a sample row from file (first row of data)
        Input:  None
        Output: String object that is data line from csv file
        '''      
        return self.sample_row1

    
    def getSampleRow2(self):
        '''
        Purpose: Get a sample row from file (second row of data)
        Input:  None
        Output: String object that is data line from csv file
        '''    
        return self.sample_row2

    
    def getNumberRows(self):
        '''
        Purpose: Get number of rows in CSV File
        Input:  None
        Output: CSV file row count
        '''      
        return self.row_count


    def getColumnTypes(self):
        '''
        Purpose: Get CSV File column types
        Input:  None
        Output: List object containing csv file column formats e.g. Date, String..
        '''     
        return self.column_type
    
    def getNumberColumns(self):
        '''
        Purpose: Get number of columns in CSV File
        Input:  None
        Output: CSV file column count
        '''     
        return len( self.getHeader() )


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
            
    def printSummaryReport(self):
        '''
        Purpose: Summary Report on CSV file format and structure
        Input:  None
        Output: Summary Report on CSV file format and structure
        '''  
        print('Total number of rows in csv file ' + self.getCsvFileName()  + ' is ' + '{:,}'.format( self.getNumberRows() )  )
        print('Total number of fields in csv file ' + self.getCsvFileName()  + ' is ' +  str( self.getNumberColumns() )  )
        print(' ')
        print('Here is the header record:')
        print( self.getHeader() )
        print('\n\n')
        print('Here are two sample data records from this csv file:')
        print( self.getSampleRow1() )
        print( self.getSampleRow1() )
        print('Here are the column types for this csv file:')
        print( self.getColumnTypes() )
        print( "")
        self.printColumnReport()


    def openCSV (self):
        '''
        Purpose: Open CSV File
              Read file
              Store first data row in self.csv_header_row1
              Store second data row in self.csv_header_row2
              Store row count in self.row_count
        Input: none
        Output: populates self.csv_header_row1, self.csv_header_row2, self.row_count
        '''    
        with open(self.csv_filename , 'rt') as self.f:
            self.row_num = 0
            
            for self.row in self.f:
                self.row_num += 1

                if self.row_num == 1:
                    self.csv_header_row = self.row

                if self.row_num == 2:
                    self.sample_row1 = self.row

                if self.row_num == 3:
                    self.sample_row2 = self.row
                    
            self.row_count = self.row_num

                    
    def createHeaderList(self):
        '''
        Purpose: Get the column headers from CSV file in order to create JSON keys
        Input:  Header row from CSV file
        Output: List of strings, each string is a column header
        '''

        self.first_row_seq = self.csv_header_row.strip().split(',')
        self.header_list = [self.i[1:-1] for self.i in self.first_row_seq]


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
            return self.header_list

        if self.key_field_name in self.header_list:
            self.field_index = self.header_list.index( self.key_field_name )
            self.header_list[ self.field_index ] = '_id'
            self.procedure_status = True
        else:
            self.procedure_status = False


    def mapCsvColumns(self, sample_in):
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
        self.column_type = []
        self.next_row_seq = sample_in.strip().split(",")

        for self.num,self.field in enumerate(self.header_list):

            if self.header_list[ self.num ][-5:] == "_LIST":
                self.column_type.append('List')
                
            elif self.next_row_seq[ self.num ].isnumeric():
                self.column_type.append('Number')

            elif len( self.next_row_seq[ self.num] ) == 0:
                self.column_type.append('Blank')

            elif len( self.next_row_seq[self.num] ) == 9 and self.next_row_seq[self.num][2] == '-' and self.next_row_seq[self.num][6] == '-':
                self.column_type.append('Date')

            else:
                self.column_type.append('String')

 
