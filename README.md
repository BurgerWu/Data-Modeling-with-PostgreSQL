# Data Modeling with PostgresSQL
![image](image/postgreSQL.png)

## Motivation
This project is the hands-on project provided by udacity data engineering nanodegree. We have log file storing user activity and song data available for all the songs in Sparkify system. It is our job to extract, transform and load these data into PostgreSQL database.

## Introduction
There are two main source of data in this project. The first one is song_data that containing all the information of available songs within Sparkify system. The other is log data that preserves all user activities. However, we are only curious about song-playing activities. 

We firstly try out our data processing procedure using Jupyter Notebook and then implement the code into .py file. Finally, we run command line operations on those .py files to extract, transform and then load into PostgreSQL. Detailed processing steps will be introduced in detail in next section.

## Data Processing 
Below lists the detailed data processing procedure in this project

1. Writing **DROP**, **CREATE TABLE**, **INSERT** and some other SQL command in sql_queries.py file
2. Run create_tables.py file to initiate the database with desired table and schema
3. Test the processing and postgreSQL integration code using etl.ipynb notebook
4. Further test the database content using test.ipynb notebook using sql extension
5. Implement the code into etl.py file
6. Open terminal and run .py file within to see if data modeling works

## Libraries Used
- pandas (dataframe and data processing)
- psycopg2 (to interact with postgreSQL)

## Files and Folders
- create_tables.py (Initiate database with desired tables and schema)
- sql_queries (Collections of DROP, CREATE TABLE, INSERT and some other queries)
- etl.ipynb (Test code with psycopg2 library)
- etl.py (Implement final code)
- test.ipynb (Test database content with sql extension)

## Summary
We are able to do a batch processing of all the log and song data with etl.py along with the help of other python scripts. Also, the results seem to be within our expectation after testing with sql extension. The data fits well within the schema without errors.

## Acknowledgement
Special thanks to udacity for providing required training, data source and resource to complete the project.
