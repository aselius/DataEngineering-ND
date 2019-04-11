# Relational Data Modeling Project
WIP: This project aims to model songs and log data into postgresql. Based on the data mart schema created. 

## Installation

### Setting up postgresql locally with Docker

    docker-compose up -d
    docker exec -it my_postgres psql -U postgres -c "create database my_database"

This should bring up a docker container which exposes its postgresql port to 54320 locally. And create a database "my_database" that we will be using for an initial connection to psql (Just making the postsql equivalent to the workspace environment.)

### Installing required Python packages
Install the required packages with pipenv

    pipenv install
    
## Data