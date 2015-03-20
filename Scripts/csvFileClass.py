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
    Purpose:  Creates class to represent CSV file structure
    '''

    def __init__(self, file_in):
        
        self.csv_filename = file_in
        self.key_field_name = 'MSISDN'  # Mongo DB field name
        self.header_list = ""  # Python object holding header list
        self.csv_header_row = ""
        self.sample_row1 = ""
        self.sample_row2 = ""
        self.row_count = 0
        self.column_type = {}
        self.status = False

        #Method that opens file, does checks based on file content and structure
        self.openCSV()

        #create python list object to hold csv header row
        self.header_list = self.createHeaderList(self.csv_header_row)

        #look for field named in self.key_field_name and update to _id
        #this is required by Mongo to identify primary key
        self.setMongoId()

        #automatically determine type for each CSV file columm
        #The available types are: List, Number, String, Date, Blank
        self.column_type = self.mapCsvColumns(self.sample_row1)

        self.status = True




            
    def getStatus(self):
        return self.status

    
    def getCsvFileName(self):
        return self.csv_filename

    
    def getHeader(self):
        return self.header_list

    
    def getSampleRow1(self):
        return self.sample_row1

    
    def getSampleRow2(self):
        return self.sample_row2

    
    def getNumberRows(self):
        return self.row_count


    def getColumnTypes(self):
        return self.column_type
    
    def getNumberColumns(self):
        return len( self.getHeader() )


    def printColumnReport(self):
        
        self.next_row_seq = self.getSampleRow1().strip().split(",")
        self.header_row = self.getHeader()
        self.column_type = self.getColumnTypes()
        for self.num,self.field in enumerate(self.header_row):
            print('Field number ' + str(self.num) + ' is ' + self.header_row[self.num]
                                    + ' ' +  self.column_type[ self.num ] 
                      + ' csv ' +  repr(self.next_row_seq[ self.num ] )  + '  ' )
            
    def printSummaryReport(self):
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
        Name: openCSV
        Purpose: Open CSV File
              Read header record 
              Store first data row as sample row1
              Store second data row as sample row2
              Get file line count
        '''    
        with open(self.csv_filename , 'rt') as self.f:
            self.row_num = 0
            
            for self.row in self.f:
                self.row_num += 1

                if self.row_num == 1:
                    self.csv_header_row = self.row
                    #self.header_list = self.createHeaderList(self.row)

                if self.row_num == 2:
                    self.sample_row1 = self.row

                if self.row_num == 3:
                    self.sample_row2 = self.row
                    
            self.row_count = self.row_num

                    
    def createHeaderList(self, row_in):
        '''
        Purpose: Get the column headers from CSV file in order to create JSON keys
        Input:  Header row from CVV file
        Output: List of strings, each string is a column header
        '''

        self.first_row_seq = row_in.strip().split(',')
        self.header_list = [self.i[1:-1] for self.i in self.first_row_seq]
        return self.header_list

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

        return self.column_type
    

        
