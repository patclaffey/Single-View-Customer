#! /usr/local/bin/python3
import unittest
import csv_mongo
from csv_mongo import *
class TestBT(unittest.TestCase):

    def setUp(self):
        pass


    def setTearDown(self):
        pass
 

    def test_main_verify(self):
        db_name = 'any_db'
        collection_name = 'any_connection'
        csvFile = '/bt/import/test_indonesia.csv'
        program_mode = 'Verify'
        row_limit = 10
    
        myCsv1 = CsvFileStructure( csvFile )
        run_mongo_etl(db_name, collection_name, myCsv1, program_mode, row_limit)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest( TestBT('test_main_verify' ) )
    runner = unittest.TextTestRunner()
    runner.run(suite)
