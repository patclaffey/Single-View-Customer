#!/usr/local/bin/python3.4
import unittest
from oracleClass import *
class TestBT(unittest.TestCase):

    
    
    def setUp(self):
        self.table_name = 'DW_SVC_ID'
        self.myTable = OracleTable( self.table_name )
        pass


    def setTearDown(self):
        pass


    def test_class_initialization(self):

        result = self.myTable.getStatus()
        self.assertTrue(result, msg='Class OracleTable initialization fails  ')

    def test_getTnsEntry(self):

        result = self.myTable.getTnsEntry()
        expected = 'DWHDF07'
        self.assertEqual(result, expected, msg='Class OracleTable method getTnsEntry() fails  ')                      


    def test_getColumnNames(self):
        # this is a column name we expect in table
        member =  'PROFILE_ID'


        result = ' '.join(self.myTable.getColumnNames() )
        self.assertRegex( result, member, msg='Class OracleTable method getColumnNames() \
                    - Column Name PROFILE_ID not found in table column list  ')


    def test_setMongoId(self):
        # test a know good header record
        member = '_id' 

        result = ' '.join(self.myTable.getColumnNames() )
        self.assertRegex( result, member, msg='Class OracleTable method setMongoId() error \
                    - Column Name _id not found in table column list  ')
        


    @unittest.skip(" skip test 1")
    def test_mapCsvColumns(self):
        expected = ['String', 'Number', 'String', 'Number', 'Number', 'Number', 'Number', 'Number',
                    'Number', 'Number', 'List', 'String', 'Date', 'Date', 'Number', 'List', 'Blank',
                    'Blank', 'Blank',
                    'String', 'Blank', 'List', 'Number', 'Date', 'Date', 'String', 'Date',
                    'String', 'Date']
        csvFile = '/bt/import/test_indonesia.csv'
        myCsv1 = CsvFileStructure( csvFile )
        sample1 = myCsv1.getSampleRow1()
        result = myCsv1.mapCsvColumns(sample1)
        self.assertEqual(result, expected, msg='Method mapCsvColumns fails  ')


   

if __name__ == '__main__':

    csvFileHi = '/bt/import/test_indonesia.csv'
    suite = unittest.TestSuite()


    suite.addTest( TestBT('test_class_initialization' ) )
    suite.addTest( TestBT('test_getTnsEntry' ) )
    suite.addTest( TestBT('test_getColumnNames' ) )
    suite.addTest( TestBT('test_mapCsvColumns' ) )
    suite.addTest( TestBT('test_setMongoId' ) )
    runner = unittest.TextTestRunner()
    runner.run(suite)
