#! /usr/local/bin/python3
import unittest
#import csv_mongo
from request_etl import *
class TestBT(unittest.TestCase):

    def setUp(self):
        self.source_name = 'Csv_File'
        self.db_name = 'any_db2'
        self.collection_name = 'any_connection'
        self.program_mode = ''
        self.schema_name = ''
        self.csvFile = 'test_indonesia.csv'
        self.row_limit = 10
        pass


    def setTearDown(self):
        pass
 

    def test_mongo_connection(self):
        result = mongo_etl2.get_mongo_collection( self.db_name, self.collection_name)
        self.assertIsNotNone( result, 'Connection fails to Mongo Database')


    def test_main_verify(self):
        self.program_mode = 'Verify'
        run_etl_request(self.source_name,\
                        self.program_mode,\
                        self.schema_name,\
                        self.csvFile,\
                        self.row_limit,\
                        self.db_name,\
                        self.collection_name
                       )


    unittest.skip('Skipping csv file update test')
    def test_main_update(self):
        self.program_mode = 'Update'
        run_etl_request(self.source_name,\
                        self.program_mode,\
                        self.schema_name,\
                        self.csvFile,\
                        self.row_limit,\
                        self.db_name,\
                        self.collection_name
                       )

    unittest.skip('Skipping csv file insert test')        
    def test_main_insert(self):
        self.program_mode = 'Insert'
        run_etl_request(self.source_name,\
                        self.program_mode,\
                        self.schema_name,\
                        self.csvFile,\
                        self.row_limit,\
                        self.db_name,\
                        self.collection_name
                       )


        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest( TestBT('test_mongo_connection' ) )
    suite.addTest( TestBT('test_main_verify' ) )
    suite.addTest( TestBT('test_main_update' ) )
    suite.addTest( TestBT('test_main_insert' ) )
    runner = unittest.TextTestRunner()
    runner.run(suite)
