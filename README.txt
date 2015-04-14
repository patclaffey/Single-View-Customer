
1.0 Overview of Web Services front end for Brandtone MongoDb
Access to data in the Brandtone Mongo database is by means of a web service API. The  web service API is developed in the Python programming language using Python Django.   Django is the Python MVC (Model View Controller) web framework that is analagous to  the Java Spring framework used to develop New Konnect.
The web services are deployed using an Apache Web Server on the server 10.177.177.60 -  this is the same development server that hosts the Mongo database. All web service  responses are in JSON format.   The three web services are as follows:
-  query number of SVC records for a given country.
-  query any msisdn for a given country
-  query any profile_id for a given country

1.1 Query number of records for a given country
url:  http://10.177.177.60/svc/v1/<Country_Code>/count/
Purpose:   query number of records (documents) for a given country
Example:  http://10.177.177.60/svc/v1/za/count/
Example:  http://10.177.177.60/svc/v1/id/count/
Parameters;  
	Country_Code  -  allowed values are za or id 

1.2 Query any msisdn for a given country
msisdn is the mongo database primary key.  This is an indexed search.

url:  http://10.177.177.60/svc/v1/<Country_Code>/msisdn/<Msisdn_Value>/
Purpose:  Query any msisdn for a given country
Example:  http://10.177.177.60/svc/v1/za/msisdn/10000005523/
Example:  http://10.177.177.60/svc/v1/id/msisdn/001j4xQV7o/
Parameters;  
	Country_Code  -  allowed values are za or id 
	Msisdn_Value  -  any msisdn value for that country

1.3 Query any profile_id for a given country
There is currently no Mongo index on profile_id. The purpose of this search is to test
an non-indexed Mongo search. 

url:  http://10.177.177.60/svc/v1/<Country_Code>/profile/<Profile_Id>/
Purpose:  Query any msisdn for a given country
Example:  http://10.177.177.60/svc/v1/za/profile/23234960/
Example:  http://10.177.177.60/svc/v1/id/profile/55340323/
Parameters;  
	Country_Code  -  allowed values are za or id 
	Msisdn_Value  -  any msisdn value for that country

2.0	Overview of ETL process for Brandtone MongoDb
A linux CLI (Command Line Interface) has been developed using the Python programming  language to load data into Mongo from two data sources.  These two data sources are  any Oracle database or any Spreadsheet. This ETL software is hosted with the Brandtone  Mongo database on the server 10.177.177.60.  

The Brandtone CLI (Command Line Interface) supports the following functions:
-  commands with prefix "test" are unit tests for the ETL software
-  commands with prefix "verify" check the data source and conversion to JSON without  impacting the Mongo database
-  commands with prefix "insert" loads data from source into mongo in insert mode
-  commands with prefix "update" loads data from source into mongo in update mode

The following two sources are supported
-  "Oracle" - the data source is an Oracle data base
-  "Csv_File" - the data source is a CSV File

Some general notes about these commands:
-  Mongo databases are created lazily.  That means no need to pre-create mongo  database on the mongo server
-  Mongo collections are created lazily.  That means no need to pre-create mongo  collection on the mongo server
-  For csv commands the CSV file must first be loaded into directory /bt/import
-  The row limit is mainly user to verify a new source.  To process all rows ensure  this number is greater than source row count.


3.0    Command Line Interface for Mongo ETL with Oracle as the Data Source

3.1  test_oracle_mongo
Command:  test_oracle_mongo
Purpose:  Functional test suite for the Oracle to Mongo ETL softwar
Example:  test_oracle_mongo
Parameters;  None

3.2  verify_oracle_mongo
Command:  verify_oracle_mongo <Table_Schema_Name>  <Table_Name>  <Row_Limit>
Purpose:  check the data source and conversion to JSON without impacting the Mongo  database
Example:  verify_oracle_mongo BT_DW_SVC DW_SVC_ID 10
Parameters;  
	Table_Schema_Name  -  Oracle schema that owns table
	Table_Name
	Row_Limit  -  limits number of rows processed

3.3  insert_oracle_mongo
Command:  insert_oracle_mongo <Table_Schema_Name>  <Table_Name>  <Row_Limit>  <Mongo_Database> <Mongo_Collection>
Purpose:  insert all data from an Oracle table into a Mongo collection
Example:  insert_oracle_mongo BT_DW_SVC DW_SVC_ID 1000000000 my_mongo_db my_collection
Parameters;  
	Table_Schema_Name  -  Oracle schema that owns table
	Table_Name
	Row_Limit  -  limits number of rows processed, set to a very high value to  ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name  on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection  name on Mongo

3.4  update_oracle_mongo
Command:  update_oracle_mongo <Table_Schema_Name>  <Table_Name>  <Row_Limit>  <Mongo_Database> <Mongo_Collection>
Purpose:  update all data from a CSV File into a Mongo collection
Example:  update_oracle_mongo BT_DW_SVC DW_SVC_ID 10000000 my_mongo_db my_collection
Parameters;  
	Table_Schema_Name  -  Oracle schema that owns table
	Table_Name
	Row_Limit  -  limits number of rows processed, set to a very high value to  ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name  on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection  name on Mongo

4.0    Command Line Interface for Mongo ETL using a CSV File

4.1  test_csv_mongo
Command:  test_csv_mongo
Purpose:  Functional test suite for the CSV File to Mongo ETL software
Example:  test_csv_mongo
Parameters;  None

4.2  verify_csv_mongo
Command:  verify_csv_mongo <Csv_File_Name>  <Row_Limit>
Purpose:  check the data source and conversion to JSON without impacting the Mongo  database
Example:  verify_csv_mongo “Indonesia_1.csv” 10
Parameters;  
	Csv_File_Name  -  file name which must exist in folder /bt/import
	Row_Limit  -  limits number of rows processed

4.3  insert_csv_mongo
Command:  insert_csv_mongo <Csv_File_Name>  <Row_Limit> <Mongo_Database>  <Mongo_Collection>
Purpose:  insert all data from a CSV File into a Mongo collection
Example:  insert_csv_mongo “Indonesia_1.csv” 10000000 my_mongo_db my_collection
Parameters;  
	Csv_File_Name  -  file name which must exist in folder /bt/import
	Row_Limit  -  limits number of rows processed, set to a very high value to  ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name  on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection  name on Mongo

4.4  update_csv_mongo
Command:  update_csv_mongo <Csv_File_Name>  <Row_Limit> <Mongo_Database>  <Mongo_Collection>
Purpose:  update all data from a CSV File into a Mongo collection
Example:  update_csv_mongo “Indonesia_1.csv” 10000000 my_mongo_db my_collection
Parameters;  
	Csv_File_Name  -  file name which must exist in folder /bt/import
	Row_Limit  -  limits number of rows processed, set to a very high value to  ensure all data is loaded
	Mongo_Database - Name of Mongo Database.  No need to pre-define database name  on Mongo
	Mongo_Collection - Name of Mongo Collection.  No need to pre-define collection  name on Mongo


5.0  Bill of Software

All code is in git - repository name is SVC (Single View of the Customer)

Linux directory structure is as follows:
/usr/bin  -  the executable shell scripts are loaded here
/usr/lib/svc  -  all python code is loaded here
/bt/import - CSV files must be installed in this directory 

Linux directories are used as per the following Linux standard:
http://www.tldp.org/HOWTO/HighQuality-Apps-HOWTO/fhs.html




 