#! /usr/local/bin/python3
import unittest
import csv_mongo
from csv_mongo import *
class TestBT(unittest.TestCase):

    def setUp(self):
        self.source_name = 'Oracle'
        self.db_name = 'any_db3'
        self.collection_name = 'any_connection'
        self.program_mode = ''
        self.schema_name = 'BT_DW_SVC'
        self.table_name = 'DW_SVC_ID'
        self.row_limit = 10000010
        pass


    def setTearDown(self):
        pass
 

    def test_oracle_connection(self):
        result = get_oracle_connection()
        self.assertIsNotNone( result, 'Connection fails to Oracle Database')

    def test_mongo_connection(self):
        result = get_mongo_collection( self.db_name, self.collection_name)
        self.assertIsNotNone( result, 'Connection fails to Mongo Database')


    def test_main_verify(self):
        self.program_mode = 'Verify'
        self.row_limit = 10
        run_etl_request(self.source_name,\
                        self.program_mode,\
                        self.schema_name,\
                        self.table_name,\
                        self.row_limit,\
                        self.db_name,\
                        self.collection_name
                       )

    @unittest.skip('Skipping csv file insert test')        
    def test_main_insert(self):
        self.program_mode = 'Insert'
        run_etl_request(self.source_name,\
                        self.program_mode,\
                        self.schema_name,\
                        self.table_name,\
                        self.row_limit,\
                        self.db_name,\
                        self.collection_name
                       )

    @unittest.skip('Skipping csv file update test')
    def test_main_update(self):
        self.program_mode = 'Update'
        run_etl_request(self.source_name,\
                        self.program_mode,\
                        self.schema_name,\
                        self.table_name,\
                        self.row_limit,\
                        self.db_name,\
                        self.collection_name
                       )





        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest( TestBT('test_oracle_connection' ) )
    suite.addTest( TestBT('test_mongo_connection' ) )
    suite.addTest( TestBT('test_main_verify' ) )
    suite.addTest( TestBT('test_main_insert' ) )
    suite.addTest( TestBT('test_main_update' ) )


    runner = unittest.TextTestRunner()
    runner.run(suite)
