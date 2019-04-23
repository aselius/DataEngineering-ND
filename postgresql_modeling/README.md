# Relational Data Modeling Project
This project extracts, transports and loads 2 types of data found in the data directory into fact and dimensional tables in a PostgresQL database.

This is a project part of the DataEngineering ND.

## Installation

### Setting up postgresql locally with Docker

    docker-compose up -d
    docker exec -it my_postgres psql -U postgres -c "create database my_database"

This should bring up a docker container which exposes its postgresql port to 54320 locally. And create a database "my_database" that we will be using for an initial connection to psql (Just making the postsql equivalent to the workspace environment.)

### Installing required Python packages
Install the required packages with pipenv

    pipenv install
    
## Data
The data included has two datasets: log data and song data. And is structured and modeled as follows.

### Data types
* Song Data
    * artist_id - Str
    * artist_latitude - Str
    * artist_location - Str
    * artist_longitude - Str
    * artist_name - Str
    * duration - Float
    * num_songs - Int
    * song_id - Str
    * title - Str
    * year - Int
* Log Data
    * artist - Str
    * auth - Str
    * firstName - Str
    * gender - Str
    * itemInSession - Int
    * lastName - Str
    * length - Float
    * location - Str
    * method - Str
    * page - Str
    * registration - Str
    * sessionId - Str
    * song - Str
    * status - Int
    * ts - Int
    * userAgent - Str
    * userId - Str

### Table Names
Tables names are as follows:

    |- songplay
    |- songs
    |- artists
    |- users
    |- time
    
Refer to the SQL statements on sql_queries.py for reference.

## Usage
Most of the heavy lifting is done by the ETL script. Create_tables.py initializes the tables for postgres on Docker.
With all the SQL quereis located on sql_queries.py
Requires Python 3

    python create_tables.py
    python etl.py

## Improvements from template
* Removed Pandas dependency to reduce overhead
* Memory allocation reduced
* Auto increment keys used when possible
* References tied in SQL statements when PK exists
* Local build with Docker
* And others I can't really think of rn

## Future work
* Dev deployment with Docker compose
* Data Validation with Schemas and Models
* Refactor extraction for better performance
* And others I can't really think of rn