#! /usr/local/bin/python3
import unittest
import csv_mongo
from csv_mongo import *
class TestBT(unittest.TestCase):

    def setUp(self):
        self.source_name = 'Csv_File'
        self.db_name = 'any_db'
        self.collection_name = 'any_connection'
        self.csvFile = 'test_indonesia.csv'
        self.program_mode = 'Verify'
        self.row_limit = 10
        pass


    def setTearDown(self):
        pass
 

    def test_mongo_connection(self):
        result = get_mongo_collection( self.db_name, self.collection_name)
        self.assertIsNotNone( result, 'Connection fails to Mongo Database')

    def test_main_verify(self):
        myCsv1 = CsvFileStructure( self.csvFile )
        run_etl_request(self.source_name,\
                        self.program_mode,\
                        myCsv1,\
                        self.row_limit,\
                        self.db_name,\
                        self.collection_name
                       )


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest( TestBT('test_mongo_connection' ) )
    suite.addTest( TestBT('test_main_verify' ) )
    runner = unittest.TextTestRunner()
    runner.run(suite)
