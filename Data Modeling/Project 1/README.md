# Project 1: Data Modeling with Postgres
[![Project passed](https://img.shields.io/badge/project-passed-success.svg)](https://img.shields.io/badge/project-passed-success.svg)
## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. To create a database schema and ETL pipeline for this analysis. Test the database and ETL pipeline by running queries given by the analytics team from Sparkify and compare the results with their expected results.

## Project Description
In this project, apply what I've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Schema for Song Play Analysis
Using the song and log datasets, create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, latitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

## Files in repository
_test.ipynb_ displays the first few rows of each table to let you check thedatabase.
_create_tables.py_ drops and creates the tables. Run this file to reset the tables before each time you run the ETL scripts.
_etl.ipynb_ reads and processes a single file from song_data and log_data and loads the data into the tables. This notebook contains detailed instructions on the ETL process for each of the tables.
_etl.py_ reads and processes files from song_data and log_data and loads them into the tables. Fill this out based on the work in the ETL notebook.
_sql_queries.py_ contains all sql queries, and is imported into the last three files above.
_README.md_ provides discussion on this project.

## Project Steps
Below are steps you can follow to complete the project:

### Create Tables
Write CREATE statements in sql_queries.py to create each table.
Write DROP statements in sql_queries.py to drop each table if it exists.
Run create_tables.py to create your database and tables.
Run test.ipynb to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.
### Build ETL Processes
Follow instructions in the etl.ipynb notebook to develop ETL processes for each table. At the end of each table section, or at the end of the notebook, run test.ipynb to confirm that records were successfully inserted into each table. Remember to rerun create_tables.py to reset your tables before each time you run this notebook.

### Build ETL Pipeline
Use what you've completed in etl.ipynb to complete etl.py, where you'll process the entire datasets. Remember to run create_tables.py before running etl.py to reset your tables. Run test.ipynb to confirm your records were successfully inserted into each table.

### Document Process
Do the following steps in your README.md file.

NOTE: You will not be able to run test.ipynb, etl.ipynb, or etl.py until you have run create_tables.py at least once to create the sparkifydb database, which these other files connect to.
