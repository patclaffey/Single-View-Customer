1.0	Linux Command Line Interface for Brandtone Mongo Database

The Brandtone CLI (Command Line Interface) supports commands for the following operations
-  commands with prefix "test" are unit tests for the ETL software
-  commands with prefix "verify" check the data source and conversion to JSON without impacting the Mongo database
-  commands with prefix "insert" loads data from source into mongo in insert mode
-  commands with prefix "update" loads data from source into mongo in update mode

The following two sources are supported
-  "Oracle" - the data source is an Oracle data base
-  "Csv_File" - the data source is a CSV File


Some general notes about these commands:
-  Mongo databases are created lazily.  That means no need to pre-create mongo database on the mongo server
-  Mongo collections are created lazily.  That means no need to pre-create mongo collection on the mongo server
-  For csv commands the CSV file must first be loaded into directory /bt/import
-  The row limit is mainly user to verify a new source.  To process all rows ensure this number is greater than source row count.


2.0    Command for Oracle Data Source

2.1  test_oracle_mongo

Command:  test_oracle_mongo
Purpose:  Functional test suite for the Oracle to Mongo ETL softwar
Example:  test_oracle_mongo
Parameters;  None


2.2  verify_oracle_mongo

Command:  verify_oracle_mongo <Table_Schema_Name>  <Table_Name>  <Row_Limit>
Purpose:  check the data source and conversion to JSON without impacting the Mongo database
Example:  verify_oracle_mongo BT_DW_SVC DW_SVC_ID 10
Parameters;  
	Table_Schema_Name  -  Oracle schema that owns table
	Table_Name
	Row_Limit  -  limits number of rows processed


2.3  insert_oracle_mongo

Command:  insert_oracle_mongo <Table_Schema_Name>  <Table_Name>  <Row_Limit> <Mongo_Database> <Mongo_Collection>
Purpose:  insert all data from an Oracle table into a Mongo collection
Example:  insert_oracle_mongo BT_DW_SVC DW_SVC_ID 1000000000 my_mongo_db my_collection
Parameters;  
	Table_Schema_Name  -  Oracle schema that owns table
	Table_Name
	Row_Limit  -  limits number of rows processed, set to a very high value to ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection name on Mongo


2.4  update_oracle_mongo

Command:  update_oracle_mongo <Table_Schema_Name>  <Table_Name>  <Row_Limit> <Mongo_Database> <Mongo_Collection>
Purpose:  update all data from a CSV File into a Mongo collection
Example:  update_oracle_mongo BT_DW_SVC DW_SVC_ID 10000000 my_mongo_db my_collection
Parameters;  
	Table_Schema_Name  -  Oracle schema that owns table
	Table_Name
	Row_Limit  -  limits number of rows processed, set to a very high value to ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection name on Mongo



3.0    Command for CSV File as a Data Source

3.1  test_csv_mongo

Command:  test_csv_mongo
Purpose:  Functional test suite for the CSV File to Mongo ETL software
Example:  test_csv_mongo
Parameters;  None


3.2  verify_csv_mongo

Command:  verify_csv_mongo <Csv_File_Name>  <Row_Limit>
Purpose:  check the data source and conversion to JSON without impacting the Mongo database
Example:  verify_csv_mongo “Indonesia_1.csv” 10
Parameters;  
	Csv_File_Name  -  file name which must exist in folder /bt/import
	Row_Limit  -  limits number of rows processed


3.3  insert_csv_mongo

Command:  insert_csv_mongo <Csv_File_Name>  <Row_Limit> <Mongo_Database> <Mongo_Collection>
Purpose:  insert all data from a CSV File into a Mongo collection
Example:  insert_csv_mongo “Indonesia_1.csv” 10000000 my_mongo_db my_collection
Parameters;  
	Csv_File_Name  -  file name which must exist in folder /bt/import
	Row_Limit  -  limits number of rows processed, set to a very high value to ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection name on Mongo


3.4  update_csv_mongo

Command:  update_csv_mongo <Csv_File_Name>  <Row_Limit> <Mongo_Database> <Mongo_Collection>
Purpose:  update all data from a CSV File into a Mongo collection
Example:  update_csv_mongo “Indonesia_1.csv” 10000000 my_mongo_db my_collection
Parameters;  
	Csv_File_Name  -  file name which must exist in folder /bt/import
	Row_Limit  -  limits number of rows processed, set to a very high value to ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection name on Mongo






4.0  Bill of Software

All code is in git - repository name is SVC (Single View of the Customer)

Linux directory structure is as follows:
/usr/bin  -  the executable shell scripts are loaded here
/usr/lib/svc  -  all python code is loaded here
/bt/import - CSV files must be installed in this directory 

Linux directories are used as per the following Linux standard:
http://www.tldp.org/HOWTO/HighQuality-Apps-HOWTO/fhs.html




 