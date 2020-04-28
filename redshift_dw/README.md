# DEND Project 3
## ETL from S3 to Redshift Data Warehouse for Analytics Tables

This project stages data in object store S3 which contains JSON files of user activity log data and song data. The purpose of this project is to perform ETL on the data residing in S3 to be loaded onto Redshift.

The redshift clusters were launched with boto3 programatically with AWS access keys. The Infrastructure as Code can be found on the redshift_iac.ipynb in this directory. (Need to plug in the right access key and secret in the config file for this to work).

The SQL queries invovled in importing over the JSON data, copying the data to a staging file, tables with a star schema data structure is all located in sql_queries.py

The create_tables (as well as drop_tables in case there are duplicates) are invoked in create_tables.py with a database connection with psycopg2.

Loading the files to staging tables, as well as inserting rows in the star schema structure is all done in etl.py