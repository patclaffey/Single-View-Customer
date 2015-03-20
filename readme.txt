


1.0	Loading Brandtone CSV Files into Mongo

The following three commands are available on the VM to load Brandtone SVC CVS data into the Mongo database:

- verify_csv_mongo :  the purpose of this command is to check the CSV file and verify that it reads the CSV data correctly.  
This command does not affect the mongo database.  The CSV file must pass this check in order to insert or update mongo.


- insert_csv_mongo : use this command to write the data from the CVS file to mongo in insert mode.

- update_csv_mongo : use this command to write the data from the CVS file to mongo in update mode.


The above command use the same parameters

parameter1 : name of mongo database.  In mongo databases are created lazily - they do not need to be predefined
parameter2 : name of mongo collection.  In mongo collections are created lazily - they do not need to be predefined
parameter3 : CSV file name.  The CSV file must be loaded into directory /bt/import
parameter4 : number representing row limit.  Limit rows processed to this number.  To process all rows ensure this number is greater than file row count.



1.1	verify CSV file command 

The command verify_csv_mongo verifies the CSV file.  The CSV file must be pre-loaded in directory /bt/import
Here is an example of usage:
	verify_csv_mongo any_db any_collection “Indonesia_1.csv”    100
The above command checks the first 100 rows of file “Indonesia_1.csv”.
The first two parameters (any_db, any_database) are required but not used.


1.2	insert data into mongo from CSV file

The command insert_csv_mongo reads the CSV file and inserts the contents into mongo.  The CSV file must be pre-loaded in directory /bt/import
Here is an example of usage:
	insert_csv_mongo any_db any_collection “Indonesia_1.csv”    100000000

parameter1 : any_db is the name of mongo database.
parameter2 : any_collection is name of mongo collection.
parameter3 : Indonesia_1.csv is CSV file name.  The CSV file has been loaded into directory /bt/import
parameter4 : 100000000 is the row limit.  This is set to a number larger than file row count in order to load all data from csv file.



1.3	update data in mongo from CSV file

The command update_csv_mongo reads the CSV file and updates the mongo database.  The CSV file must be pre-loaded in directory /bt/import
Here is an example of usage:
	update_csv_mongo any_db any_collection “Indonesia_v1.csv”    100000000

parameter1 : any_db is the name of mongo database.
parameter2 : any_collection is name of mongo collection.
parameter3 : Indonesia_v1.csv is CSV file name.  The CSV file has been loaded into directory /bt/import
parameter4 : 100000000 is the row limit.  This is set to a number larger than file row count in order to load all data from csv file.


2.0 Unit test Scripts

For python development a test driven approach is used.

2.1 Test script for the Csv Loader Class

run the following command:
	test_csv_file_class


2.2 Test script for the Verify input file

run the following command:
	test_csv_mongo
Running above command does not write any data to mongo



3.0  Bill of Software

All code is in git - repository name is SVC (Single View of the Customer)

Directory structure is as follows:
Scripts - copy all files to /usr/local/bin/Scripts on VM machine
bin - copy all files to /usr/local/bin on VM machine
import - copy all files to /bt/import on VM machine
 