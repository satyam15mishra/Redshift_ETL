# Project Description
This project has the data residing in Amazon S3 as JSON logs. Moreover, it also has metadata in form of JSON in S3 bucket. The project is aimed to build an ETL pipeline with python such that data from S3 is staged on Amazon Redshift, which then has the potential scope to be used for analytics (can be integrated to Tableau and similar BI applications).

# Purpose of Files
create_tables.py can be seen as the driver code if you want to create the databse. It uses psycopg2 module and sql_queries.py to perform sql operations on the data.

etl.py has basic logic which is used as the base file for the ETL. It has the functions to create, insert, and connection setup for the database.

sql_queries.py contains queries for dropping, creating, and staging the database tables.

dwh.cfg is the configuration file which contains the credentials and details of AWS IAM roles and Redshift cluster.

# How to Run the pipeline?
First create the database tables by running create_tables.py.
After that, you can execute etl.py to execute datapipeline

# Why to use this pipeline?
Since Redshift provides us with query optimization features such as distkey and sortkey along with parallel processing; it significantly reduces the time taken to run the queries in a non-distributed database pipeline. Moreover, the flexibility and efficiency of cloud gives this pipeline an extra edge.