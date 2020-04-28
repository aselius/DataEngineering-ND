# Datalake Project with Pyspark

### Context
Basically the context for this project is that we are trying to fetch raw unstructured data from an S3 bucket (log_data, song_data) and trying to map the log data (app usage data) to the song data (JSON metadata) and store them back into the datalake (S3). It will be stored as parquet files in structured fact/dimension table format.

### Using Pyspark
The ETL job first opens up the JSON meta data (song_data) and creates song, and artist tables. Then processes the log data, extracts just the songplay data of the users since that is what we are interested in (seeing the songplay activity of the users), creates a time, user dimension tables with the loaded data, and finally constructs a songplay fact table by joining the two dataframes created in Spark.

> Refer to the debug iPython for the thought process behind deciding on schemas, udfs to transform certain columns such as columns in the time table etc.

### How to start
Run the main function for etl.py `python etl.py` to start and finish the entire pipeline.

