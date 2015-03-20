#!/usr/local/bin/python3.4
import unittest
from csvFileClass import *
class TestBT(unittest.TestCase):

    
    
    def setUp(self):
        pass


    def setTearDown(self):
        pass


    def test_class_initialization(self):

        csvFile = '/bt/import/test_indonesia.csv'
        myCsv1 = CsvFileStructure( csvFileHi )
        result = myCsv1.getStatus()
        self.assertTrue(result, msg='Class csvFileClass initialization fails  ')


    def test_getCsvFileName(self):

        csvFile = '/bt/import/test_indonesia.csv'
        myCsv1 = CsvFileStructure( csvFile )
        expected = csvFile
        result = myCsv1.getCsvFileName()
        self.assertEqual(result, expected, msg='Class csvFileClass initialization fails  ')                      


    def test_getHeader(self):
        # test a know good header record
        expected = ['_id', 'PROFILE_ID', 'F_TYPE', 'CAMPAIGNS_JOINED', 'ENTRIES', 'INVALIDS', 
         'FIRST_DATE_SEEN', 'LAST_DATE_SEEN', 'REVENUE', 'COST', 'CAMPAIGN_LIST', 
         'LAST_REGION', 'FIRST_REGION_DATE', 'LAST_REGION_DATE', 'NO_REGION_RESPONSES', 
         'REGION_LIST', 'NO_GENDER_RESPONSES', 'FIRST_GENDER_DATE', 'LAST_GENDER_DATE', 'MOST_RECENT_GENDER', 
         'GENDER_EXPIRY_DATE', 'GENDER_LIST', 'NO_PERMISSION_RESPONSES', 'FIRST_PERMISSION_DATE', 'LAST_PERMISSION_DATE', 
         'MOST_RECENT_PERMISSION', 'OPT_OUT_DATE', 'OPTED_OUT_FLAG', 'PERMISSION_EXPIRY_DATE']

        csvFile = '/bt/import/test_indonesia.csv'
        myCsv1 = CsvFileStructure( csvFile )
        result = myCsv1.getHeader()
        self.assertEqual(result,expected, msg='Class csvFileClass Header Record procedure fails  ')
        

    def test_getSampleRow1(self):

        csvFile = '/bt/import/test_indonesia.csv'
        myCsv1 = CsvFileStructure( csvFile )
        output = myCsv1.getSampleRow1()
        result= len(output)
        self.assertNotEqual(result, 0, msg='Class csvGetSampleRow1 fails  ')   


    def test_getNumberRows(self):

        csvFile = '/bt/import/test_indonesia.csv'
        myCsv1 = CsvFileStructure( csvFile )
        output = myCsv1.getNumberRows()
        result = output
        self.assertNotEqual(result, 0, msg='Class csvGetNumberRows fails  ')

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


    def test_setMongoId(self):
        # test a know good header record
        expected = ['_id', 'PROFILE_ID', 'F_TYPE', 'CAMPAIGNS_JOINED', 'ENTRIES', 'INVALIDS', 
         'FIRST_DATE_SEEN', 'LAST_DATE_SEEN', 'REVENUE', 'COST', 'CAMPAIGN_LIST', 
         'LAST_REGION', 'FIRST_REGION_DATE', 'LAST_REGION_DATE', 'NO_REGION_RESPONSES', 
         'REGION_LIST', 'NO_GENDER_RESPONSES', 'FIRST_GENDER_DATE', 'LAST_GENDER_DATE', 'MOST_RECENT_GENDER', 
         'GENDER_EXPIRY_DATE', 'GENDER_LIST', 'NO_PERMISSION_RESPONSES', 'FIRST_PERMISSION_DATE', 'LAST_PERMISSION_DATE', 
         'MOST_RECENT_PERMISSION', 'OPT_OUT_DATE', 'OPTED_OUT_FLAG', 'PERMISSION_EXPIRY_DATE']

        csvFile = '/bt/import/test_indonesia.csv'
        myCsv1 = CsvFileStructure( csvFile )
        result = myCsv1.setMongoId()
        self.assertEqual(result,expected, msg='Method setMongoId fails  ')

if __name__ == '__main__':

    csvFileHi = '/bt/import/test_indonesia.csv'
    suite = unittest.TestSuite()


    suite.addTest( TestBT('test_class_initialization' ) )
    suite.addTest( TestBT('test_getCsvFileName' ) )
    suite.addTest( TestBT('test_getHeader' ) )
    suite.addTest( TestBT('test_getSampleRow1' ) )
    suite.addTest( TestBT('test_getNumberRows' ) )
    suite.addTest( TestBT('test_mapCsvColumns' ) )
    suite.addTest( TestBT('test_setMongoId' ) )
    runner = unittest.TextTestRunner()
    runner.run(suite)
