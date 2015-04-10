#! /usr/local/bin/python3
import unittest
from request_etl import *
from pymongo import MongoClient, Connection

class TestBT(unittest.TestCase):

    # to skip a test use decorator @unittest.skip('text')

    def setUp(self):
        self.source_name = 'Oracle'
        self.db_name = 'test_oracle_mongo'
        self.collection_name = 'test_oracle_mongo'
        self.program_mode = ''
        self.schema_name = 'BT_DW_SVC'
        self.table_name = 'DW_SVC_ID'
        self.row_limit = 10

        conn = Connection() # this is really the server object that we comm with
        if self.db_name in conn.database_names():
            conn.drop_database(self.db_name)


    def setTearDown(self):
        conn = Connection() # this is really the server object that we comm with
        if self.db_name in conn.database_names():
            conn.drop_database(self.db_name)

 

    def test_oracle_connection(self):
        result = oracle_etl.get_oracle_connection()
        self.assertIsNotNone( result, 'Connection fails to Oracle Database')

    def test_mongo_connection(self):
        result = mongo_etl2.get_mongo_collection( self.db_name, self.collection_name)
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
