# Data Modeling with Postgres

The project creates a song and user activity data model for a music streaming app. The data files are in json format in the data folder. This project creates a database schema and ETL pipeline for data analysis.

## Getting Started

Unzipping the deliverable or cloning the repository will get you a copy of the project up and running on your local machine for development and testing purposes. 


### Prerequisites

* python 3.7
* PostgreSQL

### Contents

* `sql_queries.py` : All the sql queries are stored in this file and referenced by other files.
* `create_tables.py` : Drops and creates your tables. Can be used to reset tables.
* `test.ipynb` : For testing the database setup. You can copy snippets from other python files to execute and see the test results.
* `etl.ipynb` : For trial data loading of single files
* `etl.py` : Processes all data files and loads data in database
* `bulk_insert_log.ipynb` : Processes data file using the PostgreSQL COPY command for bulk loading data without using SQL INSERT statements

### Database Design
![alt text](https://github.com/ypatankar/PostgreETL/blob/master/Database%20Structure.png)

### Deployment Steps
1. Execute `create_tables.py` to create database and tables
2. Run `test.ipynb` to confirm the creation of your tables and to confirm your records were successfully inserted into each table.
3. Run `etl.py` to process the entire data set


## Running the tests

Terminate database sessions if it is preventing execution of the program

## Executing SQL for analytics
1. Finding popular songs:
```
SELECT title AS song_title, 
       COUNT(songplay_id) AS count
FROM songplays sp INNER JOIN songs s
       ON sp.song_id = s.song_id
GROUP BY title
ORDER BY 2 DESC;
```


2. User activity by location
```
SELECT location, 
         COUNT(songplay_id) AS user_activity
FROM songplays
GROUP BY location
ORDER BY COUNT(songplay_id) desc;
```







